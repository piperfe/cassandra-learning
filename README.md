# Cassandra Experiments

This repository contains two separate experiments for testing Cassandra cluster behavior. Each experiment is organized in its own directory with all necessary files.

## Directory Structure

```
.
├── node-failure-experiment/    # Node failure experiment
│   ├── node_failure_experiment.py
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

### Node Failure Experiment

Tests data availability with RF=1 when a node fails.

```bash
cd node-failure-experiment
docker-compose up -d
python node_failure_experiment.py
```

See [node-failure-experiment/README.md](node-failure-experiment/README.md) for detailed instructions.

### Load Test Experiment

Tests cluster performance under load.

```bash
cd load-test-experiment
docker-compose up
```

See [load-test-experiment/README.md](load-test-experiment/README.md) for detailed instructions.

---

## Experiment 1: Node Failure Experiment

**Location**: `node-failure-experiment/`

This experiment tests what happens when you remove the only node that holds data in a Cassandra cluster with `replication_factor=1`.

### Key Features

- Creates a 3-node Cassandra cluster
- Inserts data with RF=1 (single replica)
- Identifies which node holds the data
- Stops that node and tests data availability
- Demonstrates the risk of data loss with low replication factors

### Expected Result

With `replication_factor=1`, when the node that holds the data is removed, the data should **NOT** be accessible anymore.

**Full documentation**: [node-failure-experiment/README.md](node-failure-experiment/README.md)

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

- **Node Failure Experiment**: Uses port `9042` on the host
- **Load Test Experiment**: Uses port `9043` on the host

```bash
# Terminal 1: Run node failure experiment
cd node-failure-experiment
docker-compose up -d
python node_failure_experiment.py

# Terminal 2: Run load test experiment
cd load-test-experiment
docker-compose up
```

---

## General Cleanup

To clean up all experiments:

```bash
# Clean up node failure experiment
cd node-failure-experiment
docker-compose down -v

# Clean up load test experiment
cd load-test-experiment
docker-compose down -v
```

---

## Notes

- Each experiment is self-contained in its own directory
- Both experiments can run simultaneously without conflicts
- Each experiment has its own Docker volumes for data persistence
- The experiments use different container names and networks to avoid conflicts
