"""
Task 4: Calculation of Delta-t and Histogram Generation
=========================================================
Reads from vesselDB.ais_filtered (produced by Task 3).

For every vessel (MMSI):
  1. Fetch all records sorted by timestamp (ascending).
  2. Compute delta-t in milliseconds between every pair of consecutive points.
  3. Store the per-vessel results back into vesselDB.delta_t.

After all workers finish:
  4. Pull every delta-t value from vesselDB.delta_t.
  5. Generate a histogram with configurable bins.
  6. Print a statistical summary and analysis of vessel behaviour.
  7. Save the histogram as delta_t_histogram.png.

Design mirrors Tasks 2 & 3:
  - One MongoClient per worker thread (required by assignment).
  - MMSI list partitioned into batches; batches processed in parallel.
  - Argparse CLI with sensible defaults.

Usage:
    python delta_t_histogram.py --workers 4 --batch-size 20
    python delta_t_histogram.py --workers 8 --batch-size 50 --bins 200 --max-delta-ms 600000
"""

import argparse
import threading
import time
from datetime import datetime
from typing import List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

# ── Configuration ──────────────────────────────────────────────────────────────
MONGO_URI             = "mongodb://localhost:27017"
DB_NAME               = "vesselDB"
FILTERED_COLLECTION   = "ais_filtered"
DELTA_T_COLLECTION    = "delta_t"

DEFAULT_BINS          = 100
DEFAULT_MAX_DELTA_MS  = 600_000
OUTPUT_FILE           = "delta_t_histogram.png"


# ── Index setup ────────────────────────────────────────────────────────────────

def create_delta_t_indexes():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    dst    = client[DB_NAME][DELTA_T_COLLECTION]
    dst.create_index([("MMSI",     ASCENDING)], name="idx_mmsi")
    dst.create_index([("delta_ms", ASCENDING)], name="idx_delta_ms")
    client.close()
    print("  Indexes created on delta_t collection.")


# ── Worker ─────────────────────────────────────────────────────────────────────

def compute_delta_t_for_batch(
    worker_id: int,
    mmsi_batch: List,
    results: dict,
    lock: threading.Lock,
):
    """
    One worker thread — opens its own MongoClient.

    For each MMSI in the batch:
      a) Fetch all records from ais_filtered sorted by timestamp ASC.
      b) Compute delta-t (ms) between consecutive timestamps.
      c) Insert one document per pair into the delta_t collection.
    """
    t_start = time.perf_counter()
    client  = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    src     = client[DB_NAME][FILTERED_COLLECTION]
    dst     = client[DB_NAME][DELTA_T_COLLECTION]

    total_pairs    = 0
    vessels_done   = 0
    vessels_single = 0

    for mmsi in mmsi_batch:
        # Fetch timestamps only (projection), sorted ascending
        cursor = src.find(
            {"MMSI": mmsi},
            {"timestamp": 1, "_id": 0}
        ).sort("timestamp", ASCENDING)

        timestamps = [doc["timestamp"] for doc in cursor]

        if len(timestamps) < 2:
            vessels_single += 1
            continue

        delta_docs = []
        for i in range(1, len(timestamps)):
            t_prev = timestamps[i - 1]
            t_curr = timestamps[i]

            if isinstance(t_curr, datetime) and isinstance(t_prev, datetime):
                delta_ms = (t_curr - t_prev).total_seconds() * 1000.0
            else:
                delta_ms = float(t_curr) - float(t_prev)

            if delta_ms < 0:
                continue

            delta_docs.append({
                "MMSI":     mmsi,
                "t_prev":   t_prev,
                "t_curr":   t_curr,
                "delta_ms": delta_ms,
            })

        if delta_docs:
            try:
                dst.insert_many(delta_docs, ordered=False)
                total_pairs  += len(delta_docs)
                vessels_done += 1
            except BulkWriteError as bwe:
                total_pairs  += bwe.details.get("nInserted", 0)
                vessels_done += 1

    client.close()
    elapsed = time.perf_counter() - t_start

    with lock:
        results[worker_id] = {
            "total_pairs":    total_pairs,
            "vessels_done":   vessels_done,
            "vessels_single": vessels_single,
            "elapsed":        round(elapsed, 2),
        }

    print(
        f"[Worker {worker_id:>2}] "
        f"vessels={vessels_done:>4}  "
        f"pairs={total_pairs:>7,}  "
        f"single_pt={vessels_single:>3}  "
        f"time={elapsed:.2f}s"
    )


# ── Histogram ──────────────────────────────────────────────────────────────────

def generate_histogram(bins: int, max_delta_ms: float):
    print("\nFetching delta-t values for histogram...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
    cursor = client[DB_NAME][DELTA_T_COLLECTION].find({}, {"delta_ms": 1, "_id": 0})
    deltas = np.array([doc["delta_ms"] for doc in cursor], dtype=np.float64)
    client.close()

    if len(deltas) == 0:
        print("  No delta-t data found.")
        return

    total        = len(deltas)
    mean_ms      = np.mean(deltas)
    median_ms    = np.median(deltas)
    std_ms       = np.std(deltas)
    min_ms       = np.min(deltas)
    max_ms       = np.max(deltas)
    p25          = np.percentile(deltas, 25)
    p75          = np.percentile(deltas, 75)
    p95          = np.percentile(deltas, 95)
    p99          = np.percentile(deltas, 99)
    within_1min  = np.sum(deltas <=  60_000) / total * 100
    within_5min  = np.sum(deltas <= 300_000) / total * 100
    within_10min = np.sum(deltas <= 600_000) / total * 100

    print("\n" + "=" * 65)
    print("DELTA-T STATISTICS")
    print(f"  Total delta-t pairs   : {total:,}")
    print(f"  Min                   : {min_ms:>12.1f} ms  ({min_ms/1000:.2f}s)")
    print(f"  Max                   : {max_ms:>12.1f} ms  ({max_ms/1000:.1f}s  /  {max_ms/60000:.1f}min)")
    print(f"  Mean                  : {mean_ms:>12.1f} ms  ({mean_ms/1000:.2f}s)")
    print(f"  Median                : {median_ms:>12.1f} ms  ({median_ms/1000:.2f}s)")
    print(f"  Std dev               : {std_ms:>12.1f} ms")
    print(f"  25th percentile       : {p25:>12.1f} ms  ({p25/1000:.2f}s)")
    print(f"  75th percentile       : {p75:>12.1f} ms  ({p75/1000:.2f}s)")
    print(f"  95th percentile       : {p95:>12.1f} ms  ({p95/1000:.1f}s)")
    print(f"  99th percentile       : {p99:>12.1f} ms  ({p99/1000:.1f}s)")
    print(f"  Within  1 min (<=60s) : {within_1min:>6.2f}%")
    print(f"  Within  5 min (<=300s): {within_5min:>6.2f}%")
    print(f"  Within 10 min (<=600s): {within_10min:>6.2f}%")
    print("=" * 65)

    print("\nVESSEL BEHAVIOUR ANALYSIS")
    if median_ms < 10_000:
        print("  * Very high-frequency reporting (median < 10s).")
        print("    Typical of vessels in busy ports or under traffic monitoring.")
    elif median_ms < 60_000:
        print("  * Standard AIS reporting interval (median 10s - 60s).")
        print("    Consistent with Class A transponders underway.")
    elif median_ms < 300_000:
        print("  * Reduced reporting rate (median 1 - 5 min).")
        print("    May indicate anchored vessels, Class B transponders, or low speed.")
    else:
        print("  * Sparse reporting (median > 5 min).")
        print("    Could indicate intermittent signal or long-range vessels.")
    if p99 > 3_600_000:
        print("  * Long tail detected (p99 > 1 hour).")
        print("    Some vessels go off-grid for extended periods.")
    if within_1min > 80:
        print("  * 80%+ of intervals are under 1 minute - dense, trackable dataset.")

    clipped  = deltas[deltas <= max_delta_ms]
    clip_pct = (1 - len(clipped) / total) * 100

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("AIS Vessel Delta-t Distribution", fontsize=15, fontweight="bold")

    ax1 = axes[0]
    ax1.hist(clipped / 1000, bins=bins, color="#2196F3", edgecolor="white", linewidth=0.3, alpha=0.85)
    ax1.set_xlabel("Delta-t (seconds)", fontsize=11)
    ax1.set_ylabel("Number of intervals", fontsize=11)
    ax1.set_title(f"Histogram (clipped at {max_delta_ms/1000:.0f}s,\n{clip_pct:.1f}% of data excluded)", fontsize=10)
    ax1.axvline(mean_ms   / 1000, color="red",    linestyle="--", linewidth=1.5, label=f"Mean   {mean_ms/1000:.1f}s")
    ax1.axvline(median_ms / 1000, color="orange", linestyle="--", linewidth=1.5, label=f"Median {median_ms/1000:.1f}s")
    ax1.legend(fontsize=9)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax1.grid(axis="y", alpha=0.3)

    ax2 = axes[1]
    log_bins = np.logspace(np.log10(max(deltas.min(), 1)), np.log10(deltas.max()), bins)
    ax2.hist(deltas / 1000, bins=log_bins, color="#4CAF50", edgecolor="white", linewidth=0.3, alpha=0.85)
    ax2.set_xscale("log")
    ax2.set_xlabel("Delta-t (seconds, log scale)", fontsize=11)
    ax2.set_ylabel("Number of intervals", fontsize=11)
    ax2.set_title("Full Range (log scale)", fontsize=10)
    ax2.axvline(mean_ms   / 1000, color="red",    linestyle="--", linewidth=1.5, label=f"Mean   {mean_ms/1000:.1f}s")
    ax2.axvline(median_ms / 1000, color="orange", linestyle="--", linewidth=1.5, label=f"Median {median_ms/1000:.1f}s")
    ax2.legend(fontsize=9)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
    ax2.grid(axis="y", alpha=0.3)

    stats_text = (
        f"n = {total:,}\n"
        f"mean  = {mean_ms/1000:.1f}s\n"
        f"median= {median_ms/1000:.1f}s\n"
        f"p95   = {p95/1000:.1f}s\n"
        f"p99   = {p99/1000:.1f}s"
    )
    ax2.text(0.97, 0.97, stats_text, transform=ax2.transAxes, fontsize=8,
             verticalalignment="top", horizontalalignment="right",
             bbox=dict(boxstyle="round,pad=0.4", facecolor="lightyellow", alpha=0.8))

    plt.tight_layout()
    plt.savefig(OUTPUT_FILE, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  Histogram saved -> {OUTPUT_FILE}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Task 4: Delta-t calculation and histogram")
    parser.add_argument("--workers",      type=int,   default=4)
    parser.add_argument("--batch-size",   type=int,   default=20)
    parser.add_argument("--bins",         type=int,   default=DEFAULT_BINS)
    parser.add_argument("--max-delta-ms", type=float, default=DEFAULT_MAX_DELTA_MS)
    parser.add_argument("--skip-compute", action="store_true",
                        help="Skip delta-t computation, go straight to histogram")
    args = parser.parse_args()

    print("=" * 65)
    print("Task 4: Delta-t Calculation + Histogram")
    print(f"  Source      : {DB_NAME}.{FILTERED_COLLECTION}")
    print(f"  Destination : {DB_NAME}.{DELTA_T_COLLECTION}")
    print(f"  Workers     : {args.workers}")
    print(f"  Batch size  : {args.batch_size} MMSIs/worker")
    print(f"  Bins        : {args.bins}")
    print(f"  Max delta   : {args.max_delta_ms:,.0f} ms  ({args.max_delta_ms/60000:.1f} min)")
    print("=" * 65)

    if not args.skip_compute:
        # ── Drop old delta_t collection ───────────────────────────────────────
        print("\nDropping old delta_t collection...")
        drop_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
        drop_client[DB_NAME][DELTA_T_COLLECTION].drop()
        drop_client.close()

        # ── Create indexes ────────────────────────────────────────────────────
        print("Creating indexes on delta_t collection...")
        create_delta_t_indexes()

        # ── Discover all distinct MMSIs ───────────────────────────────────────
        print("\nFetching distinct MMSIs from filtered collection...")
        setup_client   = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
        all_mmsi       = setup_client[DB_NAME][FILTERED_COLLECTION].distinct("MMSI")
        total_filtered = setup_client[DB_NAME][FILTERED_COLLECTION].count_documents({})
        setup_client.close()

        print(f"  Found {len(all_mmsi):,} vessels  |  {total_filtered:,} filtered records")

        if not all_mmsi:
            print("\nNo data in ais_filtered. Run Task 3 first.")
            return

        # ── Partition into batches ────────────────────────────────────────────
        batches = [all_mmsi[i: i + args.batch_size] for i in range(0, len(all_mmsi), args.batch_size)]
        print(f"  Split into {len(batches)} batches of up to {args.batch_size} MMSIs each\n")

        # ── Run workers in parallel ───────────────────────────────────────────
        results:        dict                   = {}
        lock                                   = threading.Lock()
        t_start                                = time.perf_counter()
        active_threads: List[threading.Thread] = []
        batch_idx                              = 0

        while batch_idx < len(batches):
            while len(active_threads) < args.workers and batch_idx < len(batches):
                t = threading.Thread(
                    target=compute_delta_t_for_batch,
                    args=(batch_idx, batches[batch_idx], results, lock),
                    daemon=True,
                )
                active_threads.append(t)
                t.start()
                batch_idx += 1

            finished = [t for t in active_threads if not t.is_alive()]
            for t in finished:
                active_threads.remove(t)

            if len(active_threads) >= args.workers:
                active_threads[0].join()
                active_threads.pop(0)

        for t in active_threads:
            t.join()

        total_elapsed = time.perf_counter() - t_start

        total_pairs    = sum(r["total_pairs"]    for r in results.values())
        vessels_done   = sum(r["vessels_done"]   for r in results.values())
        vessels_single = sum(r["vessels_single"] for r in results.values())

        verify_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15_000)
        db_count      = verify_client[DB_NAME][DELTA_T_COLLECTION].count_documents({})
        verify_client.close()

        print("\n" + "=" * 65)
        print("COMPUTATION COMPLETE")
        print(f"  Vessels processed     : {vessels_done:,}")
        print(f"  Vessels (1 pt only)   : {vessels_single:,}  (no delta possible)")
        print(f"  Delta-t pairs written : {total_pairs:,}")
        print(f"  DB verify (delta_t)   : {db_count:,} documents")
        print(f"  Total time            : {total_elapsed:.2f}s")
        print("=" * 65)
    else:
        print("\n--skip-compute set: reusing existing delta_t collection.")

    generate_histogram(bins=args.bins, max_delta_ms=args.max_delta_ms)


if __name__ == "__main__":
    main()
