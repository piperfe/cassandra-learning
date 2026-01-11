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
import time
from datetime import datetime

from src.infrastructure.docker_utils import (
    stop_node,
    start_node,
    wait_for_container_healthy,
    map_replica_node_to_container,
)

from src.repository.cassandra_repository import (
    connect_to_cluster,
    create_keyspace,
    create_table,
    insert_data,
    query_data,
    refresh_metadata,
    log_cql_query,
)


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
    cluster, session = connect_to_cluster(contact_points, port, username, password)
    if not cluster or not session:
        return 1
    
    # Wait for cluster to be ready
    if not wait_for_cluster(cluster, expected_nodes=3):
        logging.error("✗ Cluster not ready")
        return 1
    
    # Step 2: Create keyspace with RF=1
    logging.info("\n[Step 2] Creating keyspace with replication_factor=1...")
    if not create_keyspace(session, keyspace, replication_factor=1):
        return 1
    
    # Step 3: Create table
    logging.info("\n[Step 3] Creating table...")
    if not create_table(session, table_name, keyspace=keyspace):
        return 1
    
    # Step 4: Insert test data
    logging.info("\n[Step 4] Inserting test data...")
    test_id = "experiment-key-001"
    test_value = "This is test data for the RF=1 experiment"
    test_timestamp = datetime.utcnow()
    
    if not insert_data(session, table_name, test_id, test_value, test_timestamp, keyspace=keyspace):
        return 1
    
    # Step 5: Verify data is accessible
    logging.info("\n[Step 5] Verifying data is accessible before node removal...")
    row = query_data(session, table_name, test_id, keyspace=keyspace, max_retries=1)
    if not row:
        return 1
    
    # Step 6: Identify which node holds the data
    logging.info("\n[Step 6] Identifying which node holds the data...")
    replica_nodes = get_replica_nodes(cluster, session, keyspace, test_id, table_name)
    
    if not replica_nodes:
        logging.error("✗ Could not determine which node holds the data")
        return 1
    
    replica_node = replica_nodes[0]
    logging.info(f"✓ Data is stored on node: {replica_node}")
    
    # Container names for this experiment
    container_names = ["cassandra-node1", "cassandra-node2", "cassandra-node3"]
    
    # Map replica node address to container name
    container_to_stop = map_replica_node_to_container(
        replica_node,
        container_names,
        cluster=cluster
    )
    
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
    refresh_metadata(cluster, keyspace=keyspace)
    
    # Try to query with retries
    row_after_removal = query_data(session, table_name, test_id, keyspace=keyspace, max_retries=3, retry_delay=3)
    data_found = row_after_removal is not None
    
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
    refresh_metadata(cluster, keyspace=keyspace)
    
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
    
    row_after_restart = query_data(session, table_name, test_id, keyspace=keyspace, max_retries=5, retry_delay=3)
    data_found_after_restart = row_after_restart is not None
    
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

