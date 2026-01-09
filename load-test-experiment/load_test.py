import logging
import os
import random
import string
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


def random_device_id():
    return "device-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


def get_env_int(name, default):
    return int(os.getenv(name, default))


def main():
    log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
    )

    # Default to simple container names for this experiment
    contact_points = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra-node1").split(",")
    port = get_env_int("CASSANDRA_PORT", 9042)
    username = os.getenv("CASSANDRA_USERNAME", "")
    password = os.getenv("CASSANDRA_PASSWORD", "")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "test_scaling")
    num_threads = get_env_int("NUM_THREADS", 8)
    duration_seconds = get_env_int("DURATION_SECONDS", 60)
    write_ratio = float(os.getenv("WRITE_RATIO", "0.5"))  # 0.0–1.0
    consistency_str = os.getenv("CONSISTENCY", "ONE").upper()

    consistency_map = {
        "ONE": ConsistencyLevel.ONE,
        "QUORUM": ConsistencyLevel.QUORUM,
        "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
        "ALL": ConsistencyLevel.ALL,
    }
    consistency = consistency_map.get(consistency_str, ConsistencyLevel.ONE)

    logging.info(
        "Connecting to Cassandra at %s:%s (keyspace=%s)",
        contact_points,
        port,
        keyspace,
    )

    auth_provider = None
    if username:
        auth_provider = PlainTextAuthProvider(username=username, password=password)

    try:
        cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)
        session = cluster.connect()
    except NoHostAvailable as exc:
        logging.error("Unable to connect to Cassandra: %s", exc)
        return

    # Ensure keyspace/table exist (idempotent)
    logging.info("Ensuring keyspace test_scaling exists with RF=1")
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test_scaling
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
        """
    )
    session.set_keyspace(keyspace)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_data (
            device_id text,
            ts timestamp,
            value double,
            PRIMARY KEY (device_id, ts)
        ) WITH CLUSTERING ORDER BY (ts DESC)
        """
    )

    insert_cql = SimpleStatement(
        "INSERT INTO sensor_data (device_id, ts, value) VALUES (%s, %s, %s)",
        consistency_level=consistency,
    )
    select_cql = SimpleStatement(
        "SELECT * FROM sensor_data WHERE device_id = %s LIMIT 50",
        consistency_level=consistency,
    )

    stop_time = time.time() + duration_seconds
    lock = threading.Lock()
    stats = {
        "writes": 0,
        "reads": 0,
        "write_latency_sum": 0.0,
        "read_latency_sum": 0.0,
        "write_errors": 0,
        "read_errors": 0,
    }

    device_ids = [random_device_id() for _ in range(100)]

    def worker():
        nonlocal stats
        while time.time() < stop_time:
            is_write = random.random() < write_ratio
            device_id = random.choice(device_ids)
            start = time.time()
            try:
                if is_write:
                    ts = datetime.utcnow()
                    value = random.random() * 100.0
                    session.execute(insert_cql, (device_id, ts, value))
                    elapsed = time.time() - start
                    with lock:
                        stats["writes"] += 1
                        stats["write_latency_sum"] += elapsed
                else:
                    session.execute(select_cql, (device_id,))
                    elapsed = time.time() - start
                    with lock:
                        stats["reads"] += 1
                        stats["read_latency_sum"] += elapsed
            except Exception as e:
                with lock:
                    if is_write:
                        stats["write_errors"] += 1
                    else:
                        stats["read_errors"] += 1
                    if len(error_samples) < max_error_samples:
                        error_samples.append(("WRITE" if is_write else "READ", str(e)))

    logging.info(
        "Starting load threads=%s duration=%ss write_ratio=%.2f consistency=%s",
        num_threads,
        duration_seconds,
        write_ratio,
        consistency_str,
    )

    max_error_samples = get_env_int("ERROR_LOG_LIMIT", 5)
    error_samples = deque(maxlen=max_error_samples)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker) for _ in range(num_threads)]
        for f in as_completed(futures):
            pass

    cluster.shutdown()

    total_writes = stats["writes"]
    total_reads = stats["reads"]
    total_ops = total_writes + total_reads
    total_time = float(duration_seconds)

    print("\n=== Load Test Summary ===")
    print(f"Total operations: {total_ops}")
    print(f"  Writes: {total_writes} (errors={stats['write_errors']})")
    print(f"  Reads : {total_reads} (errors={stats['read_errors']})")
    if total_time > 0:
        print(f"Throughput: {total_ops / total_time:.1f} ops/sec")
    if total_writes > 0:
        print(f"Avg write latency: {stats['write_latency_sum'] / total_writes * 1000:.2f} ms")
    if total_reads > 0:
        print(f"Avg read latency : {stats['read_latency_sum'] / total_reads * 1000:.2f} ms")

    if error_samples:
        print("\nSample errors:")
        for error_type, message in error_samples:
            print(f"  [{error_type}] {message}")


if __name__ == "__main__":
    main()

