# Cassandra Experiments

This repository contains two separate experiments for testing Cassandra cluster behavior. Each experiment is organized in its own directory with all necessary files.

## Directory Structure

```
.
├── eventual-consistency-experiment/    # Eventual consistency experiment
│   ├── eventual_consistency_experiment.py
│   ├── docker-compose.yml
│   ├── logback-debug.xml
│   └── README.md
├── load-test-experiment/       # Load test experiment
│   ├── load_test.py
│   ├── docker-compose.yml
│   ├── logback-debug.xml
│   └── README.md
└── EXPERIMENT_README.md         # This file
```

## Quick Start

### Eventual Consistency Experiment

Tests eventual consistency behavior with RF=1 when a node becomes unavailable.

```bash
cd eventual-consistency-experiment
docker-compose up -d
python src/application/eventual_consistency_experiment.py
```

See [eventual-consistency-experiment/README.md](eventual-consistency-experiment/README.md) for detailed instructions.

### Load Test Experiment

Tests cluster performance under load.

```bash
cd load-test-experiment
docker-compose up
```

See [load-test-experiment/README.md](load-test-experiment/README.md) for detailed instructions.

### Running Tests

**Unit Tests (fast, no database required):**
```bash
cd eventual-consistency-experiment
pytest tests/repository/test_cassandra_repository.py -v
```

**Integration Tests (requires Docker, uses real Cassandra):**
```bash
cd eventual-consistency-experiment
pytest tests/repository/test_cassandra_repository_integration.py -v
```

See the [Testing](#testing) section below for more details.

---

## Experiment 1: Eventual Consistency Experiment

**Location**: `eventual-consistency-experiment/`

This experiment tests eventual consistency behavior when you remove the only node that holds data in a Cassandra cluster with `replication_factor=1`.

### Key Features

- Creates a 3-node Cassandra cluster
- Inserts data with RF=1 (single replica)
- Identifies which node holds the data
- Stops that node and tests data availability
- Demonstrates the risk of data loss with low replication factors

### Expected Result

With `replication_factor=1`, when the node that holds the data is removed, the data should **NOT** be accessible anymore.

**Full documentation**: [eventual-consistency-experiment/README.md](eventual-consistency-experiment/README.md)

---

## Experiment 2: Load Test Experiment

**Location**: `load-test-experiment/`

This experiment tests Cassandra cluster performance under various load conditions with configurable parameters.

### Key Features

- Creates a 3-node Cassandra cluster
- Runs concurrent read/write operations
- Configurable thread count, duration, write ratio, and consistency levels
- Reports throughput, latency, and error rates

### Metrics Collected

- Total operations (reads and writes)
- Throughput (operations per second)
- Average latency for reads and writes
- Error counts and sample error messages

**Full documentation**: [load-test-experiment/README.md](load-test-experiment/README.md)

---

## Running Both Experiments

You can run both experiments simultaneously since they use different Docker Compose projects and different ports:

- **Eventual Consistency Experiment**: Uses port `9042` on the host
- **Load Test Experiment**: Uses port `9043` on the host

```bash
# Terminal 1: Run eventual consistency experiment
cd eventual-consistency-experiment
docker-compose up -d
python src/application/eventual_consistency_experiment.py

# Terminal 2: Run load test experiment
cd load-test-experiment
docker-compose up
```

---

## General Cleanup

To clean up all experiments:

```bash
# Clean up eventual consistency experiment
cd eventual-consistency-experiment
docker-compose down -v

# Clean up load test experiment
cd load-test-experiment
docker-compose down -v
```

---

## Testing

### Unit Tests

Fast unit tests that use mocks and don't require a database:

```bash
cd eventual-consistency-experiment
pytest tests/ -k "not integration" -v
```

### Integration Tests

Comprehensive integration tests using [testcontainers](https://testcontainers-python.readthedocs.io/) to spin up real Cassandra instances:

```bash
cd eventual-consistency-experiment
pytest tests/repository/test_cassandra_repository_integration.py -v
```

**Prerequisites for Integration Tests:**
- Docker must be running (testcontainers uses Docker to manage containers)
- Dependencies installed: `pip install -r ../requirements.txt`

**Integration Test Coverage:**
- All repository functions tested against real Cassandra 5.0.6 instances
- 68+ integration tests covering:
  - Connection management
  - Keyspace and table operations
  - Data insertion and querying
  - Token retrieval
  - Metadata operations
  - End-to-end workflows
  - Comprehensive edge cases (BugMagnet session)

**Test Organization:**
- **Unit tests** (`test_cassandra_repository.py`): Fast tests for logging functions (17 tests)
- **Integration tests** (`test_cassandra_repository_integration.py`): Real database tests (68 tests)

See [eventual-consistency-experiment/README.md](eventual-consistency-experiment/README.md) for detailed testing documentation.

---

## Notes

- Each experiment is self-contained in its own directory
- Both experiments can run simultaneously without conflicts
- Each experiment has its own Docker volumes for data persistence
- The experiments use different container names and networks to avoid conflicts
