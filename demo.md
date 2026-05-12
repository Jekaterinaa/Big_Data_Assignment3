# Demo

### Prerequisites:
1. Cluster is running
2. Test data is inserted


### Demo steps

1. Cluster is healthy:
`docker exec mongos mongosh --port 27017 --eval "sh.status()"`
2. Verify data is accessible:
`docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.countDocuments({})'`
3. Check replica set status before failure:
`docker exec shard1a mongosh --port 27018 --eval "rs.status()" 2>&1 | findstr -E "(primary|secondary|host)"`
4. Stop one secondary node:
`docker stop shard1c`
5. Verify the cluster still works:
`docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.countDocuments({})'`
6. Check what changed in replica set:
`docker exec shard1a mongosh --port 27018 --eval "rs.status()" 2>&1 | findstr -E "(primary|secondary|host|_id)"`
7. Verify that shard distribution still works:
`docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.getShardDistribution()'`
8. Insert new data while node is down:
`docker exec mongos mongosh --port 27017 --eval 'db.getSiblingDB("vesselDB").ais_data.insertOne({test_record: true, timestamp: new Date()}); db.getSiblingDB("vesselDB").ais_data.countDocuments({})'`
9. Restart the failed node:
`docker start shard1c`
10. Verify full recovery:
`docker exec shard1a mongosh --port 27018 --eval "rs.status()" 2>&1 | findstr -E "(primary|secondary|host|_id)"`
