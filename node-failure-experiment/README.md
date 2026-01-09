# Node Failure Experiment: RF=1 Data Availability Test

This experiment tests what happens when you remove the only node that holds data in a Cassandra cluster with `replication_factor=1`.

## Experiment Overview

1. **Setup**: Creates a 3-node Cassandra cluster
2. **Data Insertion**: Inserts test data with `replication_factor=1` (data stored on only one node)
3. **Node Identification**: Identifies which node holds the data using token mapping
4. **Node Removal**: Stops the Docker container for that node
5. **Data Query Test**: Attempts to query the data after node removal
6. **Results**: Reports whether data is still accessible

## Expected Result

With `replication_factor=1`, when the node that holds the data is removed, the data should **NOT** be accessible anymore. This demonstrates the risk of data loss with low replication factors.

## Running the Experiment

### Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ with `cassandra-driver` package (or run in Docker)

### Steps

1. **Start the 3-node cluster:**
   ```bash
   cd node-failure-experiment
   docker-compose up -d
   ```

2. **Wait for cluster to be ready** (about 30-60 seconds):
   ```bash
   # Check cluster status
   docker-compose logs cassandra-node1 | grep "Starting listening for CQL clients"
   ```

3. **Run the experiment:**
   ```bash
   # Option 1: Run directly (if you have Python and cassandra-driver installed locally)
   python node_failure_experiment.py
   
   # Option 2: Run in a Docker container
   docker run --rm \
     --network cassandra-node-failure_default \
     -v $(pwd):/app \
     -w /app \
     python:3.11 \
     bash -c "pip install -q cassandra-driver && python node_failure_experiment.py"
   ```

4. **View results:**
   The script will output detailed logs showing:
   - Which node holds the data
   - Whether data is accessible after node removal
   - Final experiment results

### Environment Variables

You can customize the experiment with these environment variables:

- `CASSANDRA_CONTACT_POINTS`: Comma-separated list of contact points (default: `localhost`)
- `CASSANDRA_PORT`: CQL port (default: `9042`)
- `CASSANDRA_KEYSPACE`: Keyspace name (default: `experiment_rf1`)
- `LOG_LEVEL`: Logging level (default: `INFO`)

Example:
```bash
CASSANDRA_CONTACT_POINTS=localhost CASSANDRA_PORT=9042 python node_failure_experiment.py
```

## Understanding the Results

### If data is NOT accessible (Expected):
```
Data accessible after node removal: NO ✗
✓ EXPECTED: Data is not accessible after removing the only replica node.
   This confirms that with RF=1, data loss occurs when the owning node fails.
```

This is the **expected behavior** - with RF=1, there's only one copy of the data, so when that node fails, the data is lost.

### If data IS accessible (Unexpected):
```
Data accessible after node removal: YES ✓
⚠️  UNEXPECTED: Data is still accessible even though the only replica node is down!
```

This would be unexpected and could indicate:
- Data was somehow replicated (shouldn't happen with RF=1)
- Query coordinator caching (unlikely)
- Cluster topology issues

## Cleanup

After running the experiment:

```bash
# Stop all containers
docker-compose down

# Remove volumes (to start fresh)
docker-compose down -v
```

## Notes

- The experiment uses `ConsistencyLevel.ONE` for both reads and writes
- The script automatically identifies which node holds the data using Cassandra's token mapping
- The node is stopped using `docker stop`, simulating a node failure
- The script waits 10 seconds after stopping the node for the cluster to detect the failure

