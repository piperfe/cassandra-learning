#!/usr/bin/env python3
"""
Replica Resolver: Resolves which Cassandra nodes hold replicas for a given partition key.

This module provides functionality to:
1. Calculate partition tokens using multiple methods (CQL query and mmh3 algorithm)
2. Resolve replica nodes for a given partition key
3. Validate keyspace existence and token map availability
"""

import logging

from src.repository.cassandra_repository import get_partition_token


def _validate_keyspace_exists(cluster, keyspace):
    """Validate that the keyspace exists in cluster metadata"""
    if keyspace not in cluster.metadata.keyspaces:
        logging.error(f"Keyspace '{keyspace}' not found in cluster metadata")
        return False
    return True


def _get_token_via_query(session, keyspace, partition_key, table_name):
    """Get token value via CQL query (preferred method)"""
    return get_partition_token(session, keyspace, table_name, partition_key)


def _calculate_token_via_mmh3(cluster, partition_key):
    """Calculate token value using mmh3 algorithm (for comparison/fallback)"""
    partitioner = cluster.metadata.partitioner
    if 'Murmur3' not in partitioner:
        logging.warning(f"Partitioner '{partitioner}' is not Murmur3, skipping mmh3 calculation")
        return None
    
    try:
        import mmh3
        token_value = mmh3.hash(partition_key.encode('utf-8'), signed=False)
        logging.info(f"Token value from mmh3 algorithm for key '{partition_key}': {token_value}")
        return token_value
    except ImportError:
        logging.warning("mmh3 library not available for token calculation")
        return None
    except Exception as e:
        logging.warning(f"Error calculating token with mmh3: {e}")
        return None


def _compare_and_log_token_methods(token_value_query, token_value_mmh3):
    """Compare and log the results from both token calculation methods"""
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


def _select_token_value(token_value_query, token_value_mmh3):
    """Select token value from available methods (query preferred, mmh3 fallback)"""
    if token_value_query is not None:
        logging.info(f"Using token from query method: {token_value_query}")
        return token_value_query
    elif token_value_mmh3 is not None:
        logging.info(f"Using token from mmh3 algorithm (fallback): {token_value_mmh3}")
        return token_value_mmh3
    else:
        logging.error("Both token calculation methods failed")
        return None


def _get_replicas_from_token(cluster, keyspace, token_value):
    """Get replica nodes from token value"""
    token_map = cluster.metadata.token_map
    if not token_map:
        logging.error("Token map not available")
        return []
    
    from cassandra.metadata import Murmur3Token
    token = Murmur3Token(token_value)
    replicas = list(token_map.get_replicas(keyspace, token))
    return [r.address for r in replicas]


def get_replica_nodes(cluster, session, keyspace, partition_key, table_name="test_data"):
    """
    Get the nodes that hold replicas for a given partition key.
    
    Args:
        cluster: Cassandra cluster connection
        session: Cassandra session
        keyspace: Name of the keyspace
        partition_key: The partition key to resolve replicas for
        table_name: Name of the table (default: "test_data")
    
    Returns:
        List of node addresses that hold replicas for the partition key
    """
    try:
        # Validate keyspace exists
        if not _validate_keyspace_exists(cluster, keyspace):
            return []
        
        # Calculate token using both methods for comparison
        token_value_query = _get_token_via_query(session, keyspace, partition_key, table_name)
        token_value_mmh3 = _calculate_token_via_mmh3(cluster, partition_key)
        
        # Compare and log both methods
        _compare_and_log_token_methods(token_value_query, token_value_mmh3)
        
        # Select which token to use
        token_value = _select_token_value(token_value_query, token_value_mmh3)
        if token_value is None:
            return []
        
        # Get replica nodes from token
        return _get_replicas_from_token(cluster, keyspace, token_value)
        
    except Exception as e:
        logging.error(f"Error getting replica nodes: {e}")
        import traceback
        logging.info(traceback.format_exc())
        return []
