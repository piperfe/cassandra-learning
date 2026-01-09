#!/usr/bin/env python3
"""
Experiment: Test data availability after removing and restarting the node that holds the only replica
(RF=1 scenario)

This script:
1. Creates a 3-node Cassandra cluster
2. Inserts data with replication_factor=1
3. Identifies which node stores the data
4. Removes that node from the cluster
5. Tests if the data is still accessible (should fail with RF=1)
6. Restarts the node
7. Tests if the data is accessible again (should succeed - data persisted on disk)
"""

import logging
import os
import subprocess
import time
from datetime import datetime

from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement


def log_cql_query(query, params=None):
    """Log CQL query in native format (with actual values)"""
    if params:
        # Replace %s placeholders with actual values
        native_query = query
        if isinstance(params, (tuple, list)):
            for param in params:
                # Format the parameter appropriately for CQL
                if isinstance(param, str):
                    # Escape single quotes in strings
                    escaped_param = param.replace("'", "''")
                    native_query = native_query.replace('%s', f"'{escaped_param}'", 1)
                elif isinstance(param, datetime):
                    native_query = native_query.replace('%s', f"'{param.isoformat()}'", 1)
                elif param is None:
                    native_query = native_query.replace('%s', 'NULL', 1)
                else:
                    native_query = native_query.replace('%s', str(param), 1)
        logging.info(f"CQL Query: {native_query}")
    else:
        logging.info(f"CQL Query: {query}")


def log_docker_command(cmd, args=None):
    """Log Docker command being executed"""
    if isinstance(cmd, list):
        full_cmd = ['docker'] + cmd
    else:
        full_cmd = ['docker', cmd]
    if args:
        if isinstance(args, list):
            full_cmd.extend(args)
        else:
            full_cmd.append(args)
    logging.info(f"Docker Command: {' '.join(full_cmd)}")


def setup_logging():
    """Configure logging"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def wait_for_cluster(cluster, expected_nodes=3, max_wait=120):
    """Wait for cluster to have expected number of nodes"""
    logging.info(f"Waiting for cluster to have {expected_nodes} nodes...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            metadata = cluster.metadata
            hosts = list(metadata.all_hosts())
            up_hosts = [h for h in hosts if h.is_up]
            
            logging.info(f"Cluster status: {len(up_hosts)}/{len(hosts)} nodes up")
            
            if len(up_hosts) >= expected_nodes:
                logging.info(f"✓ Cluster ready with {len(up_hosts)} nodes")
                for host in up_hosts:
                    logging.info(f"  - {host.address}:{host.broadcast_address} (rack: {host.rack}, dc: {host.datacenter})")
                return True
        except Exception as e:
            logging.info(f"Error checking cluster status: {e}")
        
        time.sleep(2)
    
    logging.error(f"Cluster did not reach {expected_nodes} nodes within {max_wait} seconds")
    return False


def get_replica_nodes(cluster, session, keyspace, partition_key, table_name="test_data"):
    """Get the nodes that hold replicas for a given partition key"""
    try:
        # Check if keyspace exists in cluster metadata
        if keyspace not in cluster.metadata.keyspaces:
            logging.error(f"Keyspace '{keyspace}' not found in cluster metadata")
            return []
        
        # Calculate token using both methods for comparison
        token_value_query = None
        token_value_mmh3 = None
        
        # Method 1: Get token via query (preferred method)
        try:
            # Query the token from the table using the partition key
            # CQL requires a FROM clause, so we query from the actual table
            token_query = f"SELECT token(id) as token_value FROM {keyspace}.{table_name} WHERE id = %s"
            log_cql_query(token_query, (partition_key,))
            result = session.execute(token_query, (partition_key,))
            rows = list(result)
            if rows:
                logging.info(f"CQL Result (SELECT TOKEN): {len(rows)} row(s) returned")
                for idx, row in enumerate(rows):
                    if hasattr(row, '_fields'):
                        row_dict = {field: getattr(row, field) for field in row._fields}
                        logging.info(f"  Row {idx + 1}: {row_dict}")
                    else:
                        logging.info(f"  Row {idx + 1}: {row}")
                token_value_query = rows[0].token_value
                logging.info(f"Token value from query for key '{partition_key}': {token_value_query}")
        except Exception as e:
            logging.warning(f"Could not get token via query: {e}")
        
        # Method 2: Calculate token using mmh3 algorithm (for comparison)
        partitioner = cluster.metadata.partitioner
        if 'Murmur3' in partitioner:
            try:
                import mmh3
                token_value_mmh3 = mmh3.hash(partition_key.encode('utf-8'), signed=False)
                logging.info(f"Token value from mmh3 algorithm for key '{partition_key}': {token_value_mmh3}")
            except ImportError:
                logging.warning("mmh3 library not available for token calculation")
            except Exception as e:
                logging.warning(f"Error calculating token with mmh3: {e}")
        else:
            logging.warning(f"Partitioner '{partitioner}' is not Murmur3, skipping mmh3 calculation")
        
        # Compare the two methods
        logging.info("=" * 60)
        logging.info("Token Calculation Comparison:")
        logging.info(f"  Query method:    {token_value_query if token_value_query is not None else 'FAILED'}")
        logging.info(f"  mmh3 algorithm:   {token_value_mmh3 if token_value_mmh3 is not None else 'FAILED'}")
        if token_value_query is not None and token_value_mmh3 is not None:
            if token_value_query == token_value_mmh3:
                logging.info(f"  ✓ Both methods match: {token_value_query}")
            else:
                diff = abs(token_value_query - token_value_mmh3)
                logging.warning(f"  ⚠ Methods differ by: {diff}")
                logging.warning(f"     Query: {token_value_query}, mmh3: {token_value_mmh3}")
        logging.info("=" * 60)
        
        # Use query result if available, otherwise use mmh3
        if token_value_query is not None:
            token_value = token_value_query
            logging.info(f"Using token from query method: {token_value}")
        elif token_value_mmh3 is not None:
            token_value = token_value_mmh3
            logging.info(f"Using token from mmh3 algorithm (fallback): {token_value}")
        else:
            logging.error("Both token calculation methods failed")
            return []
        
        token_map = cluster.metadata.token_map
        if not token_map:
            logging.error("Token map not available")
            return []
        
        # Create token object
        from cassandra.metadata import Murmur3Token
        token = Murmur3Token(token_value)
        
        # Get replicas for this token
        replicas = list(token_map.get_replicas(keyspace, token))
        
        return [r.address for r in replicas]
    except Exception as e:
        logging.error(f"Error getting replica nodes: {e}")
        import traceback
        logging.info(traceback.format_exc())
        return []


def stop_node(container_name):
    """Stop a Docker container"""
    logging.info(f"Stopping container: {container_name}")
    try:
        log_docker_command("stop", container_name)
        result = subprocess.run(
            ["docker", "stop", container_name],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            logging.info(f"✓ Successfully stopped {container_name}")
            return True
        else:
            logging.error(f"Failed to stop {container_name}: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Error stopping container {container_name}: {e}")
        return False


def start_node(container_name):
    """Start a Docker container"""
    logging.info(f"Starting container: {container_name}")
    try:
        log_docker_command("start", container_name)
        result = subprocess.run(
            ["docker", "start", container_name],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            logging.info(f"✓ Successfully started {container_name}")
            return True
        else:
            logging.error(f"Failed to start {container_name}: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Error starting container {container_name}: {e}")
        return False


def get_container_health_status(container_name):
    """Get the health status of a Docker container"""
    try:
        inspect_cmd = ["inspect", "--format", "{{.State.Health.Status}}", container_name]
        result = subprocess.run(
            ["docker"] + inspect_cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            status = result.stdout.strip()
            return status
        else:
            # Container might not have healthcheck configured
            return None
    except Exception as e:
        logging.debug(f"Could not get health status for {container_name}: {e}")
        return None


def wait_for_container_healthy(container_name, max_wait=180):
    """Wait for a container to become healthy based on its healthcheck"""
    logging.info(f"Waiting for container {container_name} to become healthy...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        health_status = get_container_health_status(container_name)
        
        if health_status == "healthy":
            logging.info(f"✓ Container {container_name} is healthy")
            return True
        elif health_status == "unhealthy":
            logging.warning(f"⚠ Container {container_name} is unhealthy")
            # Continue waiting in case it recovers
        elif health_status == "starting":
            logging.info(f"  Container {container_name} healthcheck is starting...")
        elif health_status is None:
            # No healthcheck configured, check if container is running instead
            try:
                inspect_cmd = ["inspect", "--format", "{{.State.Running}}", container_name]
                result = subprocess.run(
                    ["docker"] + inspect_cmd,
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0 and result.stdout.strip() == "true":
                    logging.info(f"  Container {container_name} is running (no healthcheck configured)")
                    # If no healthcheck, just wait a bit and return
                    time.sleep(5)
                    return True
            except Exception:
                pass
        
        time.sleep(2)
    
    logging.warning(f"⚠ Container {container_name} did not become healthy within {max_wait} seconds")
    return False


def main():
    setup_logging()
    
    # Configuration
    contact_points = os.getenv("CASSANDRA_CONTACT_POINTS", "localhost").split(",")
    port = int(os.getenv("CASSANDRA_PORT", 9042))
    username = os.getenv("CASSANDRA_USERNAME", "")
    password = os.getenv("CASSANDRA_PASSWORD", "")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "experiment_rf1")
    table_name = "test_data"
    
    logging.info("=" * 80)
    logging.info("Node Failure Experiment: RF=1 Data Availability Test")
    logging.info("=" * 80)
    
    # Step 1: Connect to cluster
    logging.info("\n[Step 1] Connecting to Cassandra cluster...")
    auth_provider = None
    if username:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    
    try:
        cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("✓ Connected to cluster")
    except NoHostAvailable as exc:
        logging.error(f"✗ Unable to connect to Cassandra: {exc}")
        return 1
    
    # Wait for cluster to be ready
    if not wait_for_cluster(cluster, expected_nodes=3):
        logging.error("✗ Cluster not ready")
        return 1
    
    # Step 2: Create keyspace with RF=1
    logging.info("\n[Step 2] Creating keyspace with replication_factor=1...")
    try:
        drop_keyspace_query = f"DROP KEYSPACE IF EXISTS {keyspace}"
        log_cql_query(drop_keyspace_query)
        session.execute(drop_keyspace_query)
        time.sleep(1)  # Give it a moment to propagate
        
        create_keyspace_query = f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}"
        log_cql_query(create_keyspace_query)
        session.execute(create_keyspace_query)
        session.set_keyspace(keyspace)
        logging.info(f"✓ Created keyspace '{keyspace}' with RF=1")
    except Exception as e:
        logging.error(f"✗ Error creating keyspace: {e}")
        return 1
    
    # Step 3: Create table
    logging.info("\n[Step 3] Creating table...")
    try:
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id text, value text, timestamp timestamp, PRIMARY KEY (id))"
        log_cql_query(create_table_query)
        session.execute(create_table_query)
        logging.info(f"✓ Created table '{table_name}'")
    except Exception as e:
        logging.error(f"✗ Error creating table: {e}")
        return 1
    
    # Step 4: Insert test data
    logging.info("\n[Step 4] Inserting test data...")
    test_id = "experiment-key-001"
    test_value = "This is test data for the RF=1 experiment"
    test_timestamp = datetime.utcnow()
    
    try:
        insert_query = f"INSERT INTO {table_name} (id, value, timestamp) VALUES (%s, %s, %s)"
        log_cql_query(insert_query, (test_id, test_value, test_timestamp))
        insert_stmt = SimpleStatement(
            insert_query,
            consistency_level=ConsistencyLevel.ONE
        )
        session.execute(insert_stmt, (test_id, test_value, test_timestamp))
        logging.info(f"✓ Inserted data: id='{test_id}', value='{test_value}'")
    except Exception as e:
        logging.error(f"✗ Error inserting data: {e}")
        return 1
    
    # Step 5: Verify data is accessible
    logging.info("\n[Step 5] Verifying data is accessible before node removal...")
    try:
        select_query = f"SELECT * FROM {table_name} WHERE id = %s"
        log_cql_query(select_query, (test_id,))
        select_stmt = SimpleStatement(
            select_query,
            consistency_level=ConsistencyLevel.ONE
        )
        result = session.execute(select_stmt, (test_id,))
        rows = list(result)
        
        if rows:
            row = rows[0]
            logging.info(f"✓ Data retrieved successfully:")
            logging.info(f"  id: {row.id}")
            logging.info(f"  value: {row.value}")
            logging.info(f"  timestamp: {row.timestamp}")
        else:
            logging.error("✗ Data not found!")
            return 1
    except Exception as e:
        logging.error(f"✗ Error reading data: {e}")
        return 1
    
    # Step 6: Identify which node holds the data
    logging.info("\n[Step 6] Identifying which node holds the data...")
    replica_nodes = get_replica_nodes(cluster, session, keyspace, test_id, table_name)
    
    if not replica_nodes:
        logging.error("✗ Could not determine which node holds the data")
        return 1
    
    replica_node = replica_nodes[0]
    logging.info(f"✓ Data is stored on node: {replica_node}")
    
    # Map replica node address to container name
    container_to_stop = None
    
    # Container names for this experiment
    container_names = ["cassandra-node1", "cassandra-node2", "cassandra-node3"]
    
    logging.info("  Mapping replica node to container...")
    logging.info(f"  Replica node address: {replica_node}")
    
    # First, try to match by checking each container's IP
    for container in container_names:
        try:
            # Get container IP address
            inspect_cmd = ["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container]
            log_docker_command(inspect_cmd)
            result = subprocess.run(
                ["docker"] + inspect_cmd,
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                container_ip = result.stdout.strip()
                if container_ip:
                    logging.info(f"  Container {container} has IP: {container_ip}")
                    # Check if this IP matches the replica node address
                    if str(container_ip) == str(replica_node) or str(replica_node) in str(container_ip) or str(container_ip) in str(replica_node):
                        container_to_stop = container
                        logging.info(f"  ✓ Matched! Container {container} (IP: {container_ip}) holds the data")
                        break
        except Exception as e:
            logging.info(f"  Could not inspect container {container}: {e}")
    
    # Fallback: try to match by host metadata
    if not container_to_stop:
        logging.info("  Trying alternative method: matching by host metadata...")
        for host in cluster.metadata.all_hosts():
            host_address = str(host.address)
            broadcast_address = str(host.broadcast_address) if host.broadcast_address else None
            
            logging.info(f"  Host: {host_address} (broadcast: {broadcast_address})")
            
            # Check if this host matches the replica node
            if host_address == str(replica_node) or (broadcast_address and broadcast_address == str(replica_node)):
                # Try to get container name from host's endpoint
                endpoint = str(host.endpoint) if hasattr(host, 'endpoint') else None
                logging.info(f"  Host endpoint: {endpoint}")
                
                # Try to match container by checking all containers
                for container in container_names:
                    try:
                        inspect_cmd = ["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container]
                        log_docker_command(inspect_cmd)
                        result = subprocess.run(
                            ["docker"] + inspect_cmd,
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                        if result.returncode == 0:
                            container_ip = result.stdout.strip()
                            if container_ip == host_address or container_ip == broadcast_address:
                                container_to_stop = container
                                logging.info(f"  ✓ Matched container {container}")
                                break
                    except Exception:
                        pass
    
    if not container_to_stop:
        logging.error("✗ Could not determine which container to stop")
        logging.info("Available hosts:")
        for host in cluster.metadata.all_hosts():
            logging.info(f"  - {host.address} (broadcast: {host.broadcast_address})")
        return 1
    
    logging.info(f"✓ Will stop container: {container_to_stop}")
    
    # Step 7: Stop the node
    logging.info("\n[Step 7] Stopping the node that holds the data...")
    if not stop_node(container_to_stop):
        logging.error("✗ Failed to stop node")
        return 1
    
    # Wait a bit for cluster to detect the node is down
    logging.info("Waiting 10 seconds for cluster to detect node failure...")
    time.sleep(10)
    
    # Step 8: Try to query the data
    logging.info("\n[Step 8] Attempting to query data after node removal...")
    
    # Refresh cluster metadata
    try:
        cluster.refresh_schema_metadata()
        cluster.refresh_keyspace_metadata(keyspace)
    except Exception as e:
        logging.warning(f"Could not refresh metadata: {e}")
    
    max_retries = 3
    data_found = False
    
    for attempt in range(max_retries):
        try:
            logging.info(f"  Attempt {attempt + 1}/{max_retries}...")
            select_query = f"SELECT * FROM {table_name} WHERE id = %s"
            log_cql_query(select_query, (test_id,))
            select_stmt = SimpleStatement(
                select_query,
                consistency_level=ConsistencyLevel.ONE
            )
            result = session.execute(select_stmt, (test_id,))
            rows = list(result)
            
            if rows:
                row = rows[0]
                logging.info(f"✓ Data still accessible!")
                logging.info(f"  id: {row.id}")
                logging.info(f"  value: {row.value}")
                logging.info(f"  timestamp: {row.timestamp}")
                data_found = True
                break
            else:
                logging.warning(f"  No data returned (attempt {attempt + 1})")
        except Exception as e:
            logging.warning(f"  Error on attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(3)
    
    # Step 9: Restart the node
    logging.info("\n[Step 9] Restarting the node that holds the data...")
    if not start_node(container_to_stop):
        logging.error("✗ Failed to start node")
        return 1
    
    # Wait for node to become healthy (using docker healthcheck)
    if not wait_for_container_healthy(container_to_stop, max_wait=180):
        logging.warning("⚠ Container did not become healthy, but continuing with query test...")
    
    # Refresh cluster metadata to detect the node is back
    logging.info("Refreshing cluster metadata...")
    try:
        cluster.refresh_schema_metadata()
        cluster.refresh_keyspace_metadata(keyspace)
        logging.info("✓ Refreshed cluster metadata")
    except Exception as e:
        logging.warning(f"Could not refresh metadata: {e}")
    
    # Verify the node is recognized by the cluster
    logging.info("Verifying node is recognized by cluster...")
    try:
        metadata = cluster.metadata
        hosts = list(metadata.all_hosts())
        up_hosts = [h for h in hosts if h.is_up]
        
        node_back_up = False
        for host in up_hosts:
            if str(host.address) == str(replica_node):
                node_back_up = True
                logging.info(f"✓ Node {replica_node} is back up and recognized by cluster")
                break
        
        if not node_back_up:
            logging.warning(f"⚠ Node {replica_node} not yet recognized by cluster ({len(up_hosts)}/{len(hosts)} nodes up)")
    except Exception as e:
        logging.warning(f"Error checking cluster status: {e}")
    
    # Step 10: Query the data again (should succeed now)
    logging.info("\n[Step 10] Attempting to query data after node restart...")
    
    max_retries = 5
    data_found_after_restart = False
    
    for attempt in range(max_retries):
        try:
            logging.info(f"  Attempt {attempt + 1}/{max_retries}...")
            select_query = f"SELECT * FROM {table_name} WHERE id = %s"
            log_cql_query(select_query, (test_id,))
            select_stmt = SimpleStatement(
                select_query,
                consistency_level=ConsistencyLevel.ONE
            )
            result = session.execute(select_stmt, (test_id,))
            rows = list(result)
            
            if rows:
                row = rows[0]
                logging.info(f"✓ Data is accessible again!")
                logging.info(f"  id: {row.id}")
                logging.info(f"  value: {row.value}")
                logging.info(f"  timestamp: {row.timestamp}")
                data_found_after_restart = True
                break
            else:
                logging.warning(f"  No data returned (attempt {attempt + 1})")
        except Exception as e:
            logging.warning(f"  Error on attempt {attempt + 1}: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(3)
    
    # Step 11: Report results
    logging.info("\n" + "=" * 80)
    logging.info("EXPERIMENT RESULTS")
    logging.info("=" * 80)
    logging.info(f"Keyspace: {keyspace} (RF=1)")
    logging.info(f"Test data ID: {test_id}")
    logging.info(f"Node that held data: {replica_node} (container: {container_to_stop})")
    logging.info(f"Node status: RESTARTED")
    logging.info(f"Data accessible after node removal: {'YES ✓' if data_found else 'NO ✗'}")
    logging.info(f"Data accessible after node restart: {'YES ✓' if data_found_after_restart else 'NO ✗'}")
    
    if data_found:
        logging.info("\n⚠️  UNEXPECTED: Data is still accessible even though the only replica node is down!")
        logging.info("   This could indicate:")
        logging.info("   - Data was replicated to another node (unlikely with RF=1)")
        logging.info("   - Query is being served from coordinator cache (unlikely)")
        logging.info("   - Cluster topology changed and data moved (unlikely)")
    else:
        logging.info("\n✓ EXPECTED: Data is not accessible after removing the only replica node.")
        logging.info("   This confirms that with RF=1, data loss occurs when the owning node fails.")
    
    if data_found_after_restart:
        logging.info("\n✓ EXPECTED: Data is accessible again after restarting the node.")
        logging.info("   This confirms that data persisted on disk and is available when the node comes back up.")
        logging.info("   The data was not lost - it was just temporarily unavailable while the node was down.")
    else:
        logging.info("\n⚠️  UNEXPECTED: Data is still not accessible after restarting the node.")
        logging.info("   This could indicate:")
        logging.info("   - Node has not fully rejoined the cluster")
        logging.info("   - Data was lost from disk (unlikely with persistent volumes)")
        logging.info("   - Cluster topology changed significantly")
    
    logging.info("=" * 80)
    
    cluster.shutdown()
    
    # Return 0 if experiment behaved as expected (data unavailable when down, available when back up)
    # Return 1 if unexpected behavior occurred
    if not data_found and data_found_after_restart:
        return 0  # Expected behavior
    else:
        return 1  # Unexpected behavior


if __name__ == "__main__":
    exit(main())

