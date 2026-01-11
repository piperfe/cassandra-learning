# Execution Log: Node Failure Experiment

This document contains a complete execution log from a successful run of the node failure experiment.

## Execution Details

- **Date**: 2026-01-11
- **Time**: 16:42:37 - 16:43:10
- **Duration**: ~33 seconds
- **Result**: ✅ SUCCESS (Expected behavior observed)

## Complete Log Output

```
WARNING: Running pip as the 'root' user can result in broken permissions and conflicting behaviour with the system package manager. It is recommended to use a virtual environment instead: https://pip.pypa.io/warnings/venv

[notice] A new release of pip is available: 24.0 -> 25.3
[notice] To update, run: pip install --upgrade pip

2026-01-11 16:42:37,330 INFO ================================================================================
2026-01-11 16:42:37,330 INFO Node Failure Experiment: RF=1 Data Availability Test
2026-01-11 16:42:37,330 INFO ================================================================================
2026-01-11 16:42:37,330 INFO 
[Step 1] Connecting to Cassandra cluster...
2026-01-11 16:42:37,330 INFO Connecting to Cassandra cluster...
2026-01-11 16:42:37,333 WARNING Cluster.__init__ called with contact_points specified, but no load_balancing_policy. In the next major version, this will raise an error; please specify a load-balancing policy. (contact_points = ['cassandra-node1', 'cassandra-node2', 'cassandra-node3'], lbp = None)
2026-01-11 16:42:37,391 WARNING Downgrading core protocol version from 66 to 65 for 172.18.0.4:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2026-01-11 16:42:37,395 WARNING Downgrading core protocol version from 65 to 5 for 172.18.0.4:9042. To avoid this, it is best practice to explicitly set Cluster(protocol_version) to the version supported by your cluster. https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster.html#cassandra.cluster.Cluster.protocol_version
2026-01-11 16:42:37,503 INFO Using datacenter 'datacenter1' for DCAwareRoundRobinPolicy (via host '172.18.0.4:9042'); if incorrect, please specify a local_dc to the constructor, or limit contact points to local cluster nodes
2026-01-11 16:42:37,622 INFO ✓ Connected to cluster
2026-01-11 16:42:37,622 INFO Waiting for cluster to have 3 nodes...
2026-01-11 16:42:37,623 INFO Cluster status: 3/3 nodes up
2026-01-11 16:42:37,623 INFO ✓ Cluster ready with 3 nodes
2026-01-11 16:42:37,623 INFO   - 172.18.0.2:172.18.0.2 (rack: rack1, dc: datacenter1)
2026-01-11 16:42:37,623 INFO   - 172.18.0.3:172.18.0.3 (rack: rack1, dc: datacenter1)
2026-01-11 16:42:37,623 INFO   - 172.18.0.4:172.18.0.4 (rack: rack1, dc: datacenter1)
2026-01-11 16:42:37,623 INFO 
[Step 2] Creating keyspace with replication_factor=1...
2026-01-11 16:42:37,623 INFO Creating keyspace 'experiment_rf1' with replication_factor=1...
2026-01-11 16:42:37,623 INFO CQL Query: DROP KEYSPACE IF EXISTS experiment_rf1
2026-01-11 16:42:38,647 INFO CQL Query: CREATE KEYSPACE experiment_rf1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
2026-01-11 16:42:41,573 INFO ✓ Created keyspace 'experiment_rf1' with RF=1
2026-01-11 16:42:41,574 INFO 
[Step 3] Creating table...
2026-01-11 16:42:41,574 INFO Creating table 'test_data'...
2026-01-11 16:42:41,574 INFO CQL Query: CREATE TABLE IF NOT EXISTS experiment_rf1.test_data (id text, value text, timestamp timestamp, PRIMARY KEY (id))
2026-01-11 16:42:42,595 INFO ✓ Created table 'test_data'
2026-01-11 16:42:42,597 INFO 
[Step 4] Inserting test data...
2026-01-11 16:42:42,598 INFO Inserting data: id='experiment-key-001', value='This is test data for the RF=1 experiment'...
2026-01-11 16:42:42,598 INFO CQL Query: INSERT INTO experiment_rf1.test_data (id, value, timestamp) VALUES ('experiment-key-001', 'This is test data for the RF=1 experiment', '2026-01-11T16:42:42.597999')
2026-01-11 16:42:42,935 INFO ✓ Inserted data: id='experiment-key-001', value='This is test data for the RF=1 experiment'
2026-01-11 16:42:42,938 INFO 
[Step 5] Verifying data is accessible before node removal...
2026-01-11 16:42:42,942 INFO CQL Query: SELECT * FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:42:43,049 INFO ✓ Data retrieved successfully:
2026-01-11 16:42:43,051 INFO   id: experiment-key-001
2026-01-11 16:42:43,051 INFO   value: This is test data for the RF=1 experiment
2026-01-11 16:42:43,051 INFO   timestamp: 2026-01-11 16:42:42.597000
2026-01-11 16:42:43,051 INFO 
[Step 6] Identifying which node holds the data...
2026-01-11 16:42:43,051 INFO CQL Query: SELECT token(id) as token_value FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:42:43,781 INFO CQL Result (SELECT TOKEN): 1 row(s) returned
2026-01-11 16:42:43,782 INFO   Row 1: {'token_value': -504986041828077035}
2026-01-11 16:42:43,782 INFO Token value from query for key 'experiment-key-001': -504986041828077035
2026-01-11 16:42:43,800 INFO Token value from mmh3 algorithm for key 'experiment-key-001': 3900860844
2026-01-11 16:42:43,801 INFO ============================================================
2026-01-11 16:42:43,801 INFO Token Calculation Comparison:
2026-01-11 16:42:43,801 INFO   Query method:    -504986041828077035
2026-01-11 16:42:43,801 INFO   mmh3 algorithm:   3900860844
2026-01-11 16:42:43,801 WARNING   ⚠ Methods differ by: 504986045728937879
2026-01-11 16:42:43,802 WARNING      Query: -504986041828077035, mmh3: 3900860844
2026-01-11 16:42:43,802 INFO ============================================================
2026-01-11 16:42:43,802 INFO Using token from query method: -504986041828077035
2026-01-11 16:42:43,803 INFO ✓ Data is stored on node: 172.18.0.3
2026-01-11 16:42:43,803 INFO   Mapping replica node to container...
2026-01-11 16:42:43,803 INFO   Replica node address: 172.18.0.3
2026-01-11 16:42:43,803 INFO Docker Command: docker inspect -f {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} cassandra-node1
2026-01-11 16:42:44,056 INFO   Container cassandra-node1 has IP: 172.18.0.2
2026-01-11 16:42:44,056 INFO Docker Command: docker inspect -f {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}} cassandra-node2
2026-01-11 16:42:44,118 INFO   Container cassandra-node2 has IP: 172.18.0.3
2026-01-11 16:42:44,118 INFO   ✓ Matched! Container cassandra-node2 (IP: 172.18.0.3) holds the data
2026-01-11 16:42:44,118 INFO ✓ Will stop container: cassandra-node2
2026-01-11 16:42:44,118 INFO 
[Step 7] Stopping the node that holds the data...
2026-01-11 16:42:44,118 INFO Stopping container: cassandra-node2
2026-01-11 16:42:44,118 INFO Docker Command: docker stop cassandra-node2
2026-01-11 16:42:44,445 WARNING Host 172.18.0.3:9042 has been marked down
2026-01-11 16:42:45,488 WARNING Error attempting to reconnect to 172.18.0.3:9042, scheduling retry in 1.82 seconds: [Errno 111] Tried connecting to [('172.18.0.3', 9042)]. Last error: Connection refused
2026-01-11 16:42:45,700 INFO ✓ Successfully stopped cassandra-node2
2026-01-11 16:42:45,701 INFO Waiting 10 seconds for cluster to detect node failure...
2026-01-11 16:42:52,338 WARNING Error attempting to reconnect to 172.18.0.3:9042, scheduling retry in 4.52 seconds: [Errno None] Tried connecting to [('172.18.0.3', 9042)]. Last error: timed out
2026-01-11 16:42:55,702 INFO 
[Step 8] Attempting to query data after node removal...
2026-01-11 16:42:55,838 INFO ✓ Refreshed cluster metadata
2026-01-11 16:42:55,838 INFO   Attempt 1/3...
2026-01-11 16:42:55,838 INFO CQL Query: SELECT * FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:42:55,901 WARNING   Error on attempt 1: Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ONE" info={'consistency': 'ONE', 'required_replicas': 1, 'alive_replicas': 0}
2026-01-11 16:42:58,902 INFO   Attempt 2/3...
2026-01-11 16:42:58,903 INFO CQL Query: SELECT * FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:42:58,916 WARNING   Error on attempt 2: Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ONE" info={'consistency': 'ONE', 'required_replicas': 1, 'alive_replicas': 0}
2026-01-11 16:43:01,914 WARNING Error attempting to reconnect to 172.18.0.3:9042, scheduling retry in 8.88 seconds: [Errno None] Tried connecting to [('172.18.0.3', 9042)]. Last error: timed out
2026-01-11 16:43:01,918 INFO   Attempt 3/3...
2026-01-11 16:43:01,919 INFO CQL Query: SELECT * FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:43:01,968 WARNING   Error on attempt 3: Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ONE" info={'consistency': 'ONE', 'required_replicas': 1, 'alive_replicas': 0}
2026-01-11 16:43:01,968 INFO 
[Step 9] Restarting the node that holds the data...
2026-01-11 16:43:01,968 INFO Starting container: cassandra-node2
2026-01-11 16:43:01,969 INFO Docker Command: docker start cassandra-node2
2026-01-11 16:43:02,255 INFO ✓ Successfully started cassandra-node2
2026-01-11 16:43:02,256 INFO Waiting for container cassandra-node2 to become healthy...
2026-01-11 16:43:02,277 INFO   Container cassandra-node2 healthcheck is starting...
2026-01-11 16:43:04,455 INFO   Container cassandra-node2 healthcheck is starting...
2026-01-11 16:43:06,560 INFO   Container cassandra-node2 healthcheck is starting...
2026-01-11 16:43:08,673 INFO   Container cassandra-node2 healthcheck is starting...
2026-01-11 16:43:10,730 INFO ✓ Container cassandra-node2 is healthy
2026-01-11 16:43:10,730 INFO Refreshing cluster metadata...
2026-01-11 16:43:10,815 INFO ✓ Refreshed cluster metadata
2026-01-11 16:43:10,816 INFO Verifying node is recognized by cluster...
2026-01-11 16:43:10,816 WARNING ⚠ Node 172.18.0.3 not yet recognized by cluster (2/3 nodes up)
2026-01-11 16:43:10,816 INFO 
[Step 10] Attempting to query data after node restart...
2026-01-11 16:43:10,816 INFO   Attempt 1/5...
2026-01-11 16:43:10,816 INFO CQL Query: SELECT * FROM experiment_rf1.test_data WHERE id = 'experiment-key-001'
2026-01-11 16:43:10,821 WARNING Error attempting to reconnect to 172.18.0.3:9042, scheduling retry in 14.24 seconds: [Errno 111] Tried connecting to [('172.18.0.3', 9042)]. Last error: Connection refused
2026-01-11 16:43:10,864 INFO ✓ Data retrieved successfully!
2026-01-11 16:43:10,864 INFO   id: experiment-key-001
2026-01-11 16:43:10,864 INFO   value: This is test data for the RF=1 experiment
2026-01-11 16:43:10,865 INFO   timestamp: 2026-01-11 16:42:42.597000
2026-01-11 16:43:10,865 INFO 
2026-01-11 16:43:10,865 INFO ================================================================================
2026-01-11 16:43:10,865 INFO EXPERIMENT RESULTS
2026-01-11 16:43:10,865 INFO ================================================================================
2026-01-11 16:43:10,865 INFO Keyspace: experiment_rf1 (RF=1)
2026-01-11 16:43:10,865 INFO Test data ID: experiment-key-001
2026-01-11 16:43:10,865 INFO Node that held data: 172.18.0.3 (container: cassandra-node2)
2026-01-11 16:43:10,865 INFO Node status: RESTARTED
2026-01-11 16:43:10,865 INFO Data accessible after node removal: NO ✗
2026-01-11 16:43:10,865 INFO Data accessible after node restart: YES ✓
2026-01-11 16:43:10,865 INFO 
2026-01-11 16:43:10,865 INFO ✓ EXPECTED: Data is not accessible after removing the only replica node.
2026-01-11 16:43:10,865 INFO    This confirms that with RF=1, data loss occurs when the owning node fails.
2026-01-11 16:43:10,865 INFO 
2026-01-11 16:43:10,865 INFO ✓ EXPECTED: Data is accessible again after restarting the node.
2026-01-11 16:43:10,865 INFO    This confirms that data persisted on disk and is available when the node comes back up.
2026-01-11 16:43:10,865 INFO    The data was not lost - it was just temporarily unavailable while the node was down.
2026-01-11 16:43:10,865 INFO ================================================================================
```

## Key Observations

### Successful Steps

1. **Cluster Connection**: Successfully connected to all 3 nodes
2. **Keyspace Creation**: Created keyspace with RF=1
3. **Data Insertion**: Successfully inserted test data
4. **Replica Identification**: Identified that `cassandra-node2` (172.18.0.3) holds the data
5. **Node Stopping**: Successfully stopped the container
6. **Data Unavailability**: Correctly observed that data is NOT accessible after node removal (expected behavior)
7. **Node Restart**: Successfully restarted the container
8. **Data Recovery**: Successfully retrieved data after node restart (expected behavior)

### Error Messages (Expected)

- **Unavailable Exception**: `Cannot achieve consistency level ONE` - This is expected when the only replica node is down
- **Connection Errors**: Various connection refused/timeout errors while the node is down - These are expected

### Results Summary

- ✅ **Data accessible after node removal**: NO (Expected)
- ✅ **Data accessible after node restart**: YES (Expected)
- ✅ **Experiment Outcome**: SUCCESS - All expected behaviors observed

This execution demonstrates the expected behavior of Cassandra with RF=1: data becomes unavailable when the only replica node fails, but data persists on disk and becomes available again when the node restarts.
