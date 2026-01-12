# Eventual Consistency Experiment: RF=1 Data Availability Test

This experiment tests eventual consistency behavior when you remove the only node that holds data in a Cassandra cluster with `replication_factor=1`.

## Experiment Overview

The experiment follows these steps:

1. **Connect to Cluster**: Connects to the 3-node Cassandra cluster
2. **Create Keyspace**: Creates a keyspace with `replication_factor=1` (data stored on only one node)
3. **Create Table**: Creates the test table structure
4. **Insert Test Data**: Inserts test data into the table
5. **Verify Initial Access**: Verifies that data is accessible before any node removal
6. **Identify Replica Node**: Identifies which node holds the data using token mapping
7. **Stop Node**: Stops the Docker container for the node that holds the data
8. **Query After Removal**: Attempts to query the data after node removal (should fail)
9. **Restart Node**: Restarts the Docker container for the node that holds the data
10. **Query After Restart**: Attempts to query the data after node restart (should succeed)
11. **Report Results**: Reports the complete experiment results

## Expected Results

1. **After Node Removal (Step 8)**: With `replication_factor=1`, when the node that holds the data is removed, the data should **NOT** be accessible anymore. This demonstrates the risk of data unavailability with low replication factors.

2. **After Node Restart (Step 10)**: When the node is restarted, the data should **be accessible again**. This demonstrates that data persisted on disk and is available when the node comes back up. The data was not lost - it was just temporarily unavailable while the node was down.

## Project Structure

The codebase is organized into layers following a clean architecture pattern:

**Production Code:**
- **`src/application/`** - Application layer containing the main experiment script and business logic
- **`src/repository/`** - Repository layer for Cassandra data access operations
- **`src/infrastructure/`** - Infrastructure layer for Docker utilities and external services

**Tests:**
- **`tests/application/`** - Tests for the application layer
- **`tests/repository/`** - Tests for the repository layer
  - **`test_cassandra_repository.py`** - Unit tests (fast, use mocks)
  - **`test_cassandra_repository_integration.py`** - Integration tests (real Cassandra via testcontainers)
- **`tests/infrastructure/`** - Tests for the infrastructure layer

**Configuration:**
- **`config/`** - Configuration files (logback, etc.)

The test structure mirrors the production code structure, making it easy to locate tests for specific modules. This separation allows for better maintainability, testability, and clear separation of concerns.

## Testing

This project includes both **unit tests** and **integration tests** to ensure comprehensive coverage.

### Unit Tests

Unit tests are fast, isolated tests that use mocks and don't require a database connection.

**Location:** `tests/repository/test_cassandra_repository.py`

**Coverage:**
- `log_cql_query` function (17 tests)
  - Query logging with various parameter types
  - Edge cases: empty strings, unicode, very long strings, SQL injection patterns
  - BugMagnet session with advanced edge cases

**Run unit tests:**
```bash
cd eventual-consistency-experiment
pytest tests/repository/test_cassandra_repository.py -v
```

### Integration Tests

Integration tests use [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up real Cassandra 5.0.6 instances in Docker containers, providing realistic testing against actual database behavior.

**Location:** `tests/repository/test_cassandra_repository_integration.py`

**Prerequisites:**
- Docker must be installed and running
- Dependencies: `pip install -r ../requirements.txt` (includes `pytest` and `testcontainers`)

**Coverage (68+ tests):**
- **Connection Management**: Real cluster connections, authentication, multiple contact points
- **Keyspace Operations**: Creation, deletion, replication factors, edge cases (48-char limit)
- **Table Operations**: Creation with various configurations
- **Data Operations**: Insert, query with different consistency levels, edge cases
- **Token Operations**: Real partition token retrieval and validation
- **Metadata Operations**: Schema refresh operations
- **End-to-End Workflows**: Complete multi-step operations
- **BugMagnet Edge Cases**: Comprehensive edge case coverage including:
  - Empty/whitespace values
  - Very long strings (10,000+ characters)
  - Unicode and special characters
  - SQL injection patterns (parameterized query safety)
  - Timestamp edge cases (epoch, future dates, leap years)
  - Consistency level variations
  - Multiple keyspaces and sequential operations

**Run integration tests:**
```bash
cd eventual-consistency-experiment
pytest tests/repository/test_cassandra_repository_integration.py -v
```

**Note:** Integration tests take longer to run (~2-3 minutes) as they start real Cassandra containers. The testcontainers library automatically manages container lifecycle (startup, health checks, cleanup).

### Running All Tests

Run both unit and integration tests:

```bash
cd eventual-consistency-experiment
# Run all tests
pytest tests/ -v

# Run only unit tests (fast)
pytest tests/repository/test_cassandra_repository.py -v

# Run only integration tests
pytest tests/repository/test_cassandra_repository_integration.py -v
```

### Test Organization Strategy

- **Unit tests** are used for pure functions that don't require database interaction (e.g., `log_cql_query`)
- **Integration tests** are used for functions that interact with Cassandra, ensuring real database behavior is tested
- This separation provides:
  - Fast feedback from unit tests during development
  - Confidence from integration tests that verify real database interactions
  - No duplicate test coverage between unit and integration tests

## Running the Experiment

### Prerequisites

- Docker and Docker Compose installed
- Python 3.11+ with `cassandra-driver` package (or run in Docker)

### Steps

1. **Start the 3-node cluster:**
   ```bash
   cd eventual-consistency-experiment
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
   python src/application/eventual_consistency_experiment.py
   
   # Option 2: Run in a Docker container
   docker run --rm \
     --network cassandra-eventual-consistency_default \
     -v $(pwd):/app \
     -w /app \
     python:3.11 \
     bash -c "pip install -q cassandra-driver && python src/application/eventual_consistency_experiment.py"
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
CASSANDRA_CONTACT_POINTS=localhost CASSANDRA_PORT=9042 python src/application/eventual_consistency_experiment.py
```

## Understanding the Results

The experiment reports two key results:

### Result 1: Data Access After Node Removal (Step 8)

**Expected: Data is NOT accessible**
```
Data accessible after node removal: NO ✗
✓ EXPECTED: Data is not accessible after removing the only replica node.
   This confirms that with RF=1, data loss occurs when the owning node fails.
```

This is the **expected behavior** - with RF=1, there's only one copy of the data, so when that node fails, the data becomes unavailable.

**Unexpected: Data IS accessible**
```
Data accessible after node removal: YES ✓
⚠️  UNEXPECTED: Data is still accessible even though the only replica node is down!
```

This would be unexpected and could indicate:
- Data was somehow replicated (shouldn't happen with RF=1)
- Query coordinator caching (unlikely)
- Cluster topology issues

### Result 2: Data Access After Node Restart (Step 10)

**Expected: Data IS accessible again**
```
Data accessible after node restart: YES ✓
✓ EXPECTED: Data is accessible again after restarting the node.
   This confirms that data persisted on disk and is available when the node comes back up.
   The data was not lost - it was just temporarily unavailable while the node was down.
```

This is the **expected behavior** - Cassandra persists data to disk, so when the node restarts, the data should be available again.

**Unexpected: Data is still NOT accessible**
```
Data accessible after node restart: NO ✗
⚠️  UNEXPECTED: Data is still not accessible after restarting the node.
```

This would be unexpected and could indicate:
- Node has not fully rejoined the cluster
- Data was lost from disk (unlikely with persistent volumes)
- Cluster topology changed significantly

### Complete Expected Outcome

The experiment is successful when:
- Data is **NOT** accessible after node removal (Step 8)
- Data **IS** accessible after node restart (Step 10)

This demonstrates that with RF=1, data becomes unavailable when the owning node fails, but the data persists on disk and becomes available again when the node restarts.

## Example Execution Log

A complete execution log from a successful run of the experiment is available in [`EXECUTION_LOG.md`](EXECUTION_LOG.md). This log shows:

- Complete step-by-step execution output
- All CQL queries executed during the experiment
- Token calculation and replica node identification
- Error messages encountered (expected during node unavailability)
- Final experiment results confirming expected behavior

The log demonstrates a successful execution where:
- ✅ Data was correctly identified on `cassandra-node2` (172.18.0.3)
- ✅ Data became unavailable after node removal (as expected with RF=1)
- ✅ Data became accessible again after node restart (confirming data persistence)

You can view the full log by opening [`EXECUTION_LOG.md`](EXECUTION_LOG.md) in this directory.

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
- The node is stopped using `docker stop`, simulating node unavailability
- The script waits 10 seconds after stopping the node for the cluster to detect the failure
- After restarting the node, the script waits for the container to become healthy (up to 180 seconds) and verifies the node is recognized by the cluster
- The experiment demonstrates both data unavailability (when node is down) and data persistence (when node restarts)

