# MongoDB Sharded Cluster — AIS Vessel Data Pipeline

This project sets up a **MongoDB sharded cluster** using Docker Compose for storing and processing AIS (Automatic Identification System) vessel data. The cluster consists of 2 shards (each with 3 replicas), a 3-node config server replica set, and a `mongos` query router — enabling parallel data insertion, horizontal scaling, and **fault tolerance** (if one node fails, another replica takes over automatically).


---

## Project Files Reference

```
big_data_assignment3/
├── parallel_insert.py            # Task 2 main: parallel CSV insertion (csv module + generators)
├── noise_filtering.py            # Task 3: AIS noise filtering and data cleaning pipeline
├── pyproject.toml                # Python project config
├── README.md                     # This file
├── aisdk-2026-04-18.csv          # AIS dataset (download separately)
└── mongo-cluster/
    ├── docker-compose.yml        # 10-container sharded cluster (with memory limits)
    ├── setup_and_insert.py       # Test-data inserter (1000 synthetic docs) — for pipeline verification
    ├── reset_and_insert.py       # Earlier single-threaded reference inserter
    └── clear_db.py               # Helper: deletes all docs from ais_data

```
---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Cluster Architecture](#cluster-architecture)
3. [Step 1 — Start the Cluster](#step-1--start-the-cluster)
4. [Step 2 — Initialize Replica Sets](#step-2--initialize-replica-sets)
5. [Step 3 — Add Shards to the Router](#step-3--add-shards-to-the-router)
6. [Step 4 — Verify the Cluster](#step-4--verify-the-cluster)
7. [Step 5 — Enable Sharding and Create the Database](#step-5--enable-sharding-and-create-the-database)
8. [Testing Pipeline](#testing-pipeline)
9. [Task 2 — Parallel Data Insertion](#task-2--parallel-data-insertion)
10. [Useful Commands](#useful-commands)
11. [Stopping and Cleaning Up](#stopping-and-cleaning-up)
12. [Troubleshooting](#troubleshooting)
13. [Task 3 — Parallel Data Noise Filtering](#task-3--parallel-data-noise-filtering)
14. [Task 4 — Delta-t Calculation and Histogram Generation](#task-4--delta-t-calculation-and-histogram-generation)

---

## Prerequisites

Make sure the following are installed on your machine before starting:

| Tool               | Version  | Check command            |
|--------------------|----------|--------------------------|
| **Docker**         | 20.10+   | `docker --version`       |
| **Docker Compose** | 2.0+     | `docker compose version` |
| **Python**         | 3.9+     | `python3 --version`      |
| **pymongo**        | 4.0+     | `pip show pymongo`       |

Install Python dependencies:

```bash
pip install pymongo
```

> No `pandas` required for the parallel insertion. The CSV is parsed with Python's native `csv` module and generators, so the entire dataset never sits in RAM.

### ⚠️ Docker Memory Requirement

Docker Desktop must be allocated **at least 12 GB of RAM** for the 10-container cluster to run stably under heavy insertion load.

Open Docker Desktop → Settings → Resources → drag Memory to **12 GB or higher** → Apply & Restart.

The `docker-compose.yml` also includes per-container `mem_limit` and `--wiredTigerCacheSizeGB` settings to prevent containers from competing for memory and crashing each other. Without these, MongoDB defaults to grabbing 50% of host RAM per container, which causes cascading failures during multi-million row inserts.

---

## Cluster Architecture

Each shard and the config server is a **3-node replica set**, providing fault tolerance — if any single node goes down, the remaining 2 nodes elect a new primary and the cluster continues to operate without data loss.

```
                        ┌──────────────┐
                        │   mongos     │  ← Application connects here (port 27017)
                        │  (router)    │
                        └──────┬───────┘
                               │
                  ┌────────────┼────────────┐
                  │                         │
      ┌───────────┴──────────┐  ┌───────────┴──────────┐
      │   Shard 1 Replica Set │  │   Shard 2 Replica Set │
      │  shard1a (primary)    │  │  shard2a (primary)    │
      │  shard1b (secondary)  │  │  shard2b (secondary)  │
      │  shard1c (secondary)  │  │  shard2c (secondary)  │
      │     (port 27018)      │  │     (port 27020)      │
      └───────────────────────┘  └───────────────────────┘

              ┌───────────────────────────┐
              │  Config Server Replica Set │
              │  configsvr1 (primary)      │
              │  configsvr2 (secondary)    │
              │  configsvr3 (secondary)    │
              │       (port 27019)         │
              └───────────────────────────┘
```

| Component       | Containers                          | Exposed Port | Role                                      |
|----------------|-------------------------------------|-------------|-------------------------------------------|
| Config Servers | `configsvr1`, `configsvr2`, `configsvr3` | 27019  | Store sharding metadata and chunk mapping  |
| Shard 1        | `shard1a`, `shard1b`, `shard1c`     | 27018       | Stores a portion of the sharded data       |
| Shard 2        | `shard2a`, `shard2b`, `shard2c`     | 27020       | Stores a portion of the sharded data       |
| Router         | `mongos`                            | 27017       | Entry point for all client connections     |

**Total containers:** 10 (3 config + 3 shard1 + 3 shard2 + 1 router)

**Shard key:** `{"MMSI": "hashed"}` — distributes data evenly across shards based on vessel MMSI identifier.

**Fault tolerance:** Each replica set has 3 members. If 1 node fails, the remaining 2 form a majority and elect a new primary automatically. Your application continues to work without interruption.

---

## Step 1 — Start the Cluster

Navigate to the `mongo-cluster` directory and start all containers:

```bash
cd mongo-cluster
docker compose up -d
```

Wait for all 10 containers to become healthy (~20-30 seconds):

```bash
docker compose ps
```

You should see all services with status **Up** or **healthy**. The `mongos` container will only start after the config servers and shard primaries pass their health checks.

---

## Step 2 — Initialize Replica Sets

Each replica set must be initialized with all its members. You must do this **once** — on the very first startup. On subsequent restarts, the replica sets persist and you can skip this step.

Run the following commands one by one, **waiting ~5 seconds between each**:

```bash
# Initialize the config server replica set (3 members)
docker exec configsvr1 mongosh --port 27019 --eval "rs.initiate({_id: 'configReplSet', configsvr: true, members: [{_id: 0, host: 'configsvr1:27019'}, {_id: 1, host: 'configsvr2:27019'}, {_id: 2, host: 'configsvr3:27019'}]})"

# Initialize shard1 replica set (3 members)
docker exec shard1a mongosh --port 27018 --eval "rs.initiate({_id: 'shard1ReplSet', members: [{_id: 0, host: 'shard1a:27018'}, {_id: 1, host: 'shard1b:27018'}, {_id: 2, host: 'shard1c:27018'}]})"

# Initialize shard2 replica set (3 members)
docker exec shard2a mongosh --port 27018 --eval "rs.initiate({_id: 'shard2ReplSet', members: [{_id: 0, host: 'shard2a:27018'}, {_id: 1, host: 'shard2b:27018'}, {_id: 2, host: 'shard2c:27018'}]})"
```

**Expected output** for each: `{ ok: 1 }`. If you get `MongoServerError: already initialized`, that's safe to ignore — it means the cluster was previously configured.

Wait **~10 seconds** for each replica set to elect a primary before proceeding.

---

## Step 3 — Add Shards to the Router

Register both shards with the `mongos` router:

```bash
docker exec mongos mongosh --port 27017 --eval "sh.addShard('shard1ReplSet/shard1a:27018,shard1b:27018,shard1c:27018')"
docker exec mongos mongosh --port 27017 --eval "sh.addShard('shard2ReplSet/shard2a:27018,shard2b:27018,shard2c:27018')"
```

**Expected output** for each: `{ shardAdded: 'shard1ReplSet', ok: 1 }` (and similar for shard2).

---

## Step 4 — Verify the Cluster

Check that everything is connected and both shards are recognized:

```bash
docker exec mongos mongosh --port 27017 --eval "sh.status()"
```

You should see output listing:
- **shards:** `shard1ReplSet` and `shard2ReplSet` (each with 3 hosts)
- **active mongoses:** 1
- **databases:** (may be empty at this point)

If both shards appear, the cluster is **ready**.

---

## Step 5 — Enable Sharding and Create the Database

This step is handled automatically by the test insertion script (see next section). It:
1. Creates the `vesselDB` database
2. Enables sharding on `vesselDB`
3. Creates the `ais_data` collection with shard key `{"MMSI": "hashed"}`

If you need to do it manually via `mongosh`:

```bash
docker exec mongos mongosh --port 27017
```

```javascript
sh.enableSharding("vesselDB")
sh.shardCollection("vesselDB.ais_data", { "MMSI": "hashed" })
```

---

## Testing Pipeline

Follow this pipeline to verify the cluster works end-to-end before inserting real data.

### 1. Insert Test Dataset (1000 documents)

From the project root directory, run:

```bash
python mongo-cluster/setup_and_insert.py
```

**What this script does:**
- Connects to `mongos` at `mongodb://127.0.0.1:27017`
- Enables sharding on `vesselDB`
- Creates and shards the `ais_data` collection (key: `MMSI` hashed)
- Generates and inserts **1000 synthetic AIS documents** with fields: `Timestamp`, `Type_of_mobile`, `MMSI`, `Latitude`, `Longitude`, `Navigational_status`, `ROT`, `SOG`, `COG`, `Heading`, `Ship_type`, `Destination`
- Prints the insertion count and per-shard distribution

**Expected output:**
```
Inserted 1000 documents
Total documents in collection: 1000
Distribution check:
  shard1ReplSet: ~500 docs
  shard2ReplSet: ~500 docs
```

### 2. Verify Data in the Database

**Option A — via mongosh (interactive):**

```bash
docker exec -it mongos mongosh --port 27017
```

```javascript
use vesselDB
db.ais_data.countDocuments({})          // Should return 1000
db.ais_data.find().limit(3)             // View sample documents
db.ais_data.getShardDistribution()      // Check data spread across shards
```

Type `exit` to leave.

**Option B — via mongosh (one-liners from terminal):**

```bash
docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.countDocuments({})'
docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.find().limit(2).toArray()'
docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.getShardDistribution()'
```

### 3. Confirm Sharding is Working

```bash
docker exec mongos mongosh --port 27017 --eval "sh.status()"
```

Under the `databases` section, you should see `vesselDB` listed with `ais_data` partitioned and chunks distributed across both shards.

### 4. Clear Test Data Before Real Insertion

After verifying the test pipeline works, clear the synthetic data so you can insert the real AIS dataset:

```bash
python mongo-cluster/clear_db.py
```

**Expected output:**
```
Deleted 1000 documents
Remaining: 0
```

> **Note:** This only removes the documents — the collection and its sharding configuration are preserved. You do **not** need to re-run the sharding setup.

---

## Task 2 — Parallel Data Insertion

This is the main deliverable for Task 2. The `parallel_insert.py` script (in the project root) implements production-style parallel CSV insertion that satisfies the assignment requirements:

- **Reads from a CSV file** using Python's native `csv.reader`
- **Streams via generators** — never loads the full file into memory
- **One MongoClient per worker thread** — each parallel task creates and closes its own connection
- **Bulk inserts** with `insert_many(ordered=False)` for high throughput
- **Validates and cleans rows on the fly** — drops malformed lines, parses timestamps as BSON Date, MMSI as int, etc.

### How it Works

1. `stream_valid_records(filepath)` is a generator that opens the CSV, skips the header, parses + validates each row on the fly, and yields one cleaned dict at a time. Invalid rows (bad MMSI, unparseable timestamp, missing coordinates) are dropped during streaming.
2. `chunked(iterator, size)` is a second generator that batches the streamed dicts into chunks of N records.
3. The main loop pulls chunks from the generator, dispatches them to N worker threads in parallel. Each thread creates its own `MongoClient`, performs `insert_many()`, then closes its client.
4. Memory stays flat: at any moment, only `chunk_size × workers` dicts exist in RAM.

### Get the Real Dataset

```bash
curl -O http://aisdata.ais.dk/aisdk-2026-04-18.zip
unzip aisdk-2026-04-18.zip
```

The full file is **~20.7 million rows / 4 GB**.

### Running the Insert

Activate the Python environment and run:

```bash
source venv/bin/activate    # or your project's venv path
python3 parallel_insert.py --csv aisdk-2026-04-18.csv --workers 4 --chunk-size 50000 --limit 8000000
```

**Arguments:**

| Flag            | Default                  | Description                                    |
|-----------------|--------------------------|------------------------------------------------|
| `--csv`         | `aisdk-2026-04-18.csv`   | Path to the AIS CSV file                       |
| `--workers`     | `4`                      | Number of parallel worker threads              |
| `--chunk-size`  | `50000`                  | Rows per chunk (each chunk → one worker)       |
| `--limit`       | none (full file)         | Max rows to read; useful to cap RAM usage      |

> **Why `--limit 8000000`?** The full file is ~20 M rows / 4 GB. On a 16 GB laptop with 12 GB allocated to Docker, ~7–8 M rows is a stable upper bound that completes cleanly in a few minutes. The assignment explicitly says to insert *"such an amount of data that is sufficient to your PC or visual machine memory"*. Inserting all 20M reliably requires more RAM than a typical consumer laptop can provide for a 6-node sharded cluster.

### Expected Output

```
============================================================
AIS Parallel Inserter (csv.reader + generators)
  CSV:        aisdk-2026-04-18.csv
  Workers:    4
  Chunk size: 50,000
  Row limit:  8000000
  MongoDB:    mongodb://localhost:27017  ->  vesselDB.ais_data
============================================================

Creating indexes...
  Indexes ready.

Streaming CSV and inserting (4 chunks at a time)...

[Worker   0] inserted= 50000  errors=   0  time=5.47s
[Worker   1] inserted= 50000  errors=   0  time=6.06s
...
============================================================
INSERTION COMPLETE
  Total inserted   : ~7,800,000
  Total errors     : 0
  Total time       : ~5 min
============================================================
```

### Indexes Created

`parallel_insert.py` creates these indexes before inserting (improves query performance for downstream Task 3 and Task 4):

- `MMSI`
- `timestamp`
- `(MMSI, timestamp)` — compound, ideal for per-vessel time-sorted queries

### Verify Distribution Across Shards

```bash
docker exec mongos mongosh --port 27017 --eval '
db = db.getSiblingDB("vesselDB");
db.ais_data.getShardDistribution();
'
```

Both shards should hold roughly 50% of the data (the hashed MMSI key distributes evenly).

---

## Useful Commands

| Task                          | Command                                                                  |
|-------------------------------|--------------------------------------------------------------------------|
| Start the cluster             | `docker compose up -d`                                                   |
| Stop the cluster              | `docker compose down`                                                    |
| View container status         | `docker compose ps`                                                      |
| View container logs           | `docker logs <container>` (e.g., `docker logs mongos --tail 50`)         |
| Open mongosh shell            | `docker exec -it mongos mongosh --port 27017`                            |
| Check shard status            | `docker exec mongos mongosh --port 27017 --eval "sh.status()"`           |
| Check replica set status      | `docker exec shard1a mongosh --port 27018 --eval "rs.status()"`          |
| Count documents               | `docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.countDocuments({})'` |
| Check shard distribution      | `docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.getShardDistribution()'` |

---

## Stopping and Cleaning Up

**Stop containers** (data persists in Docker volumes):

```bash
docker compose down
```

**Stop and remove all data** (full reset — you'll need to redo Steps 2-5):

```bash
docker compose down -v
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `mongos` container won't start | Check that config servers and shards are healthy: `docker compose ps`. Restart with `docker compose restart`. |
| `rs.initiate()` returns `already initialized` | The replica set is already configured (happens on restart). Run `rs.status()` to check — if it shows a primary, you're fine. |
| `sh.addShard()` returns error | The shard is already added. Run `sh.status()` to verify. |
| Python script connection refused | Make sure all containers are running and `mongos` is up on port 27017: `docker compose ps`. |
| Port conflict on 27017 | Another MongoDB instance may be running locally. Stop it or change the `mongos` port mapping in `docker-compose.yml`. |
| `enableSharding` error: "already enabled" | Safe to ignore — sharding was already configured from a previous run. |
| A node is down but cluster works | Expected — replica sets tolerate 1 node failure. Check `rs.status()` to see which node is down and restart it. |
| Workers showing `errors=N` mid-insert | A shard ran out of RAM and crashed. Verify Docker Desktop has 12+ GB allocated, and that `docker-compose.yml` includes `mem_limit` and `--wiredTigerCacheSizeGB` flags. |
| `Could not find host matching read preference { mode: "primary" }` | A shard has no primary. Check `rs.status()` on each shard; if a node crashed, restart it with `docker compose up -d`. |
| `ModuleNotFoundError: No module named 'pymongo'` | Activate your venv: `source venv/bin/activate`, then `pip install pymongo`. |

---

## Task 3 — Parallel Data Noise Filtering

The `noise_filtering.py` script reads from the `ais_data` collection populated by Task 2, applies parallel noise filtering across all vessels, and writes only clean records to a separate `ais_filtered` collection.

- **One MongoClient per worker thread** — each parallel task creates and closes its own connection, matching the Task 2 pattern
- **Per-vessel parallelism** — distinct MMSIs are partitioned into batches, each batch processed by one worker
- **Six filter categories** applied at both vessel and record level
- **Indexes created automatically** before filtering for efficient per-vessel queries
- **Output stored in a separate collection** (`ais_filtered`) as required by the assignment

---

### How it Works

1. All indexes on `ais_data` and `ais_filtered` are dropped and recreated fresh to avoid conflicts on re-runs.
2. All distinct MMSI values are fetched from `ais_data` and partitioned into batches.
3. Each worker thread receives a batch of MMSIs and opens its own `MongoClient`.
4. For each MMSI the worker first checks the MMSI itself (Categories 1 & 4), then counts its records (Category 5), then fetches and validates each record individually (Categories 2, 3, 6).
5. Records that pass all filters are bulk-inserted into `ais_filtered`.

---

### Noise Filter Categories

| Category | What is filtered | Why |
|----------|-----------------|-----|
| **Cat 1** | Invalid MMSI patterns — wrong length, non-numeric, known bad values (`000000000`, `123456789`, all-same-digit, etc.) | Unconfigured transponders produce a single massive MMSI bucket that crashes worker memory |
| **Cat 2** | Coordinates outside valid ranges or exactly `(0.0, 0.0)` — "Null Island" | AIS devices report `0°N 0°E` when GPS is not locked; including these creates false teleportation events |
| **Cat 3** | Missing or unparseable timestamps | Records without a valid timestamp cannot be placed in a vessel timeline |
| **Cat 4** | MMSIs with prefix `992` (base stations), `970` (SART), `111` (SAR aircraft) | Shore infrastructure transmits on AIS but has no vessel movement data |
| **Cat 5** | Vessels with fewer than 100 total records | Too few data points to form a meaningful vessel track |
| **Cat 6** | Records missing required fields: `MMSI`, `Latitude`, `Longitude`, `ROT`, `SOG`, `COG`, `Heading` | Incomplete records cannot be used for analysis |

---

### Indexes Created

The script creates the following indexes before filtering begins:

**On `ais_data` (source):**
- `MMSI` — speeds up per-vessel `count_documents()` and `find()` queries
- `(MMSI, Timestamp)` compound — efficient time-sorted per-vessel access

**On `ais_filtered` (output):**
- `MMSI` — fast vessel lookup in filtered results
- `(MMSI, Timestamp)` compound — for downstream time-series queries
- `nav_status` — for status-based filtering in later tasks
- `(Latitude, Longitude)` — for geospatial queries

---

### Running the Filter

Make sure Task 2 has been run first and `ais_data` contains documents. Then:

```bash
python noise_filtering.py --workers 4 --batch-size 20
```


**Arguments:**

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `4` | Number of parallel worker threads |
| `--batch-size` | `20` | Number of MMSIs processed per worker batch |

> **Note:** If you re-run `noise_filtering.py` without clearing `ais_filtered` first, the script will drop and recreate indexes automatically but will insert on top of existing filtered data. Drop the collection first in mongosh for a clean run:
> ```bash
>docker exec -it mongos mongosh --port 27017
>```

>```javascript
> use vesselDB
> db.ais_filtered.drop()
> ```
---

### Verify Filtered Results

```bash
docker exec -it mongos mongosh --port 27017
```

```javascript
use vesselDB
db.ais_filtered.countDocuments({})           // Total clean records
db.ais_filtered.distinct("MMSI").length      // Number of clean vessels
db.ais_filtered.findOne()                    // Inspect a sample record
db.ais_filtered.getShardDistribution()       // Check spread across shards
```

---

### Troubleshooting

| Problem | Solution |
|---------|----------|
| `IndexKeySpecsConflict` on startup | The script now drops indexes automatically — this should not occur. If it does, run `db.ais_filtered.dropIndexes()` manually in mongosh. |
| `Records kept: 0` but DB shows records | A previous run left records in `ais_filtered`. Drop the collection and rerun. |
| `No data found in source collection` | Task 2 has not been run yet, or the collection name differs. Verify with `db.ais_data.countDocuments({})` in mongosh. |

---

## Task 4 — Delta-t Calculation and Histogram Generation

The `delta_t_histogram.py` script reads from the `ais_filtered` collection populated by Task 3, computes the time difference between consecutive data points for each vessel in parallel, stores the results in a new `delta_t` collection, and generates a histogram for analysis.

- **One MongoClient per worker thread** — matches the pattern established in Tasks 2 & 3
- **Per-vessel parallelism** — distinct MMSIs are partitioned into batches, each batch processed by one worker
- **Delta-t stored in MongoDB** — each pair of consecutive timestamps produces one document in `vesselDB.delta_t`
- **Two-chart histogram output** — linear scale (clipped for readability) and log scale (full range showing the tail)

---

### How it Works

1. The old `delta_t` collection is dropped and indexes are recreated fresh.
2. All distinct MMSI values are fetched from `ais_filtered` and partitioned into worker batches.
3. Each worker opens its own `MongoClient`, fetches timestamps for each MMSI sorted ascending, and computes delta-t in milliseconds between every consecutive pair.
4. Negative deltas (out-of-order records) are discarded.
5. Delta-t documents are bulk-inserted into `vesselDB.delta_t`.
6. After all workers finish, the histogram is generated from the full `delta_t` collection and saved as `delta_t_histogram.png`.

---

### Indexes Created

**On `delta_t` (output):**
- `MMSI` — fast per-vessel lookup
- `delta_ms` — efficient range queries and percentile calculations

---

### Running the Script

Make sure Task 3 has been run first and `ais_filtered` contains documents. Then:

```bash
python delta_t_histogram.py --workers 4 --batch-size 20
```

**Arguments:**

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `4` | Number of parallel worker threads |
| `--batch-size` | `20` | Number of MMSIs processed per worker batch |
| `--bins` | `100` | Number of histogram bins |
| `--max-delta-ms` | `600000` | X-axis clip threshold for the linear histogram (ms) |
| `--skip-compute` | off | Skip delta-t computation and regenerate histogram only |

---

### Results

Running on the full `aisdk-2026-04-18.csv` dataset after Task 3 filtering:

| Metric | Value |
|--------|-------|
| Vessels processed | 1,858 |
| Delta-t pairs computed | 13,956,386 |
| Min delta-t | 0.0 ms |
| Max delta-t | 74,985,000 ms (~1,250 min) |
| Mean | 9,264 ms (9.3s) |
| Median | 4,000 ms (4.0s) |
| p95 | 20,000 ms (20s) |
| p99 | 141,000 ms (141s) |
| Within 1 minute | 98.42% |
| Within 5 minutes | 99.85% |
| Within 10 minutes | 99.96% |

---

### Histogram Analysis

The histogram reveals two key insights:

**Left chart (linear scale):** The overwhelming majority of intervals cluster under 20 seconds, with the distribution dropping off sharply. This confirms standard Class A AIS transponder behaviour — vessels underway report every 2–10 seconds.

**Right chart (log scale):** Shows the full range including the long tail. The small bars at 10⁵–10⁸ seconds represent vessels that went off-grid for extended periods — consistent with ocean crossings or port stays where transponders are turned off.

**Conclusion:** The dataset is dense and highly trackable. With 98.4% of all intervals under 60 seconds and a median of just 4 seconds, the filtered data provides a reliable basis for vessel trajectory reconstruction and behavioural analysis.

---

### Verify Results

```bash
docker exec -it mongos mongosh --port 27017
```

```javascript
use vesselDB
db.delta_t.countDocuments({})              // Total delta-t pairs
db.delta_t.distinct("MMSI").length         // Number of vessels with deltas
db.delta_t.findOne()                       // Inspect a sample document
db.delta_t.aggregate([{ $group: { _id: null, avg: { $avg: "$delta_ms" }, median: { $avg: "$delta_ms" } } }])
```

---

### Troubleshooting

| Problem | Solution |
|---------|----------|
| `No data in ais_filtered` | Task 3 has not been run yet. Run `noise_filtering.py` first. |
| `delta_t` collection is empty after run | Check worker output for errors. Verify `ais_filtered` has records with valid timestamps. |
| Histogram not saved | Ensure `matplotlib` and `numpy` are installed: `pip install matplotlib numpy`. |
