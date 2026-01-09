# Load Test Experiment

This experiment tests Cassandra cluster performance under various load conditions with configurable parameters.

## Experiment Overview

1. **Setup**: Creates a 3-node Cassandra cluster
2. **Schema Creation**: Creates keyspace and table for sensor data
3. **Load Generation**: Runs multiple threads performing read/write operations
4. **Metrics Collection**: Tracks throughput, latency, and error rates
5. **Results**: Reports performance statistics

## Running the Load Test Experiment

### Prerequisites

- Docker and Docker Compose installed

### Steps

1. **Start the cluster and run the load test:**
   ```bash
   cd load-test-experiment
   docker-compose up
   ```

   The load generator will automatically start after all Cassandra nodes are healthy.

2. **View results:**
   The load generator will output a summary showing:
   - Total operations (reads and writes)
   - Throughput (operations per second)
   - Average latency for reads and writes
   - Error counts and sample error messages

## Configuration

You can customize the load test by modifying environment variables in `docker-compose.yml`:

- `NUM_THREADS`: Number of concurrent worker threads (default: `16`)
- `DURATION_SECONDS`: Test duration in seconds (default: `60`)
- `WRITE_RATIO`: Ratio of writes to total operations, 0.0-1.0 (default: `0.5`)
- `CONSISTENCY`: Consistency level - ONE, QUORUM, LOCAL_QUORUM, or ALL (default: `ONE`)
- `CASSANDRA_KEYSPACE`: Keyspace name (default: `test_scaling`)
- `CASSANDRA_PORT`: CQL port (default: `9042`)

Example: To run a longer test with more threads:
```bash
docker-compose run --rm \
  -e NUM_THREADS=32 \
  -e DURATION_SECONDS=120 \
  load-generator
```

## Understanding the Results

The load test output includes:

```
=== Load Test Summary ===
Total operations: 1234
  Writes: 617 (errors=0)
  Reads : 617 (errors=0)
Throughput: 20.6 ops/sec
Avg write latency: 45.23 ms
Avg read latency : 38.12 ms
```

- **Total operations**: Sum of all reads and writes
- **Throughput**: Operations per second
- **Latency**: Average time per operation in milliseconds
- **Errors**: Count of failed operations

## Running the Load Test Manually

If you want to run the load test script directly (outside Docker):

```bash
cd load-test-experiment
python load_test.py
```

You'll need to have Python 3.11+ and `cassandra-driver` installed, and ensure the cluster is running.

## Cleanup

After running the experiment:

```bash
# Stop all containers
docker-compose down

# Remove volumes (to start fresh)
docker-compose down -v
```

## Notes

- The load test uses a sensor data schema with device_id, timestamp, and value
- Operations are distributed across 100 random device IDs
- The test uses `ConsistencyLevel.ONE` by default for better performance
- Error samples are logged (limited to 5 by default)
- The cluster uses port `9043` on the host (mapped to `9042` in containers) to avoid conflicts with other experiments

