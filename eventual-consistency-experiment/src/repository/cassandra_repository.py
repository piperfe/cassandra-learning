#!/usr/bin/env python3
"""
Cassandra Repository Module

This module provides repository functions for interacting with Apache Cassandra:
- Connection management
- Keyspace and table operations
- Data insertion and querying
- Metadata management

These functions abstract the low-level Cassandra driver operations and provide
a clean interface for common database operations.
"""

import logging
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


def connect_to_cluster(contact_points, port, username=None, password=None):
    """
    Connect to Cassandra cluster
    
    Args:
        contact_points: List of contact points or comma-separated string
        port: Port number
        username: Optional username for authentication
        password: Optional password for authentication
    
    Returns:
        Tuple of (cluster, session) or (None, None) on failure
    """
    logging.info("Connecting to Cassandra cluster...")
    
    if isinstance(contact_points, str):
        contact_points = contact_points.split(",")
    
    auth_provider = None
    if username:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    
    try:
        cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)
        session = cluster.connect()
        logging.info("✓ Connected to cluster")
        return cluster, session
    except NoHostAvailable as exc:
        logging.error(f"✗ Unable to connect to Cassandra: {exc}")
        return None, None


def create_keyspace(session, keyspace, replication_factor=1):
    """
    Create a keyspace with the specified replication factor
    
    Args:
        session: Cassandra session
        keyspace: Keyspace name
        replication_factor: Replication factor (default: 1)
    
    Returns:
        True on success, False on failure
    """
    logging.info(f"Creating keyspace '{keyspace}' with replication_factor={replication_factor}...")
    
    try:
        # Drop keyspace if it exists
        drop_keyspace_query = f"DROP KEYSPACE IF EXISTS {keyspace}"
        log_cql_query(drop_keyspace_query)
        session.execute(drop_keyspace_query)
        time.sleep(1)  # Give it a moment to propagate
        
        # Create keyspace
        create_keyspace_query = f"CREATE KEYSPACE {keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{replication_factor}'}}"
        log_cql_query(create_keyspace_query)
        session.execute(create_keyspace_query)
        session.set_keyspace(keyspace)
        logging.info(f"✓ Created keyspace '{keyspace}' with RF={replication_factor}")
        return True
    except Exception as e:
        logging.error(f"✗ Error creating keyspace: {e}")
        return False


def create_table(session, table_name, keyspace=None):
    """
    Create a test table with id, value, and timestamp columns
    
    Args:
        session: Cassandra session
        table_name: Name of the table to create
        keyspace: Optional keyspace name (if not already set on session)
    
    Returns:
        True on success, False on failure
    """
    logging.info(f"Creating table '{table_name}'...")
    
    try:
        if keyspace:
            full_table_name = f"{keyspace}.{table_name}"
        else:
            full_table_name = table_name
        
        create_table_query = f"CREATE TABLE IF NOT EXISTS {full_table_name} (id text, value text, timestamp timestamp, PRIMARY KEY (id))"
        log_cql_query(create_table_query)
        session.execute(create_table_query)
        logging.info(f"✓ Created table '{table_name}'")
        return True
    except Exception as e:
        logging.error(f"✗ Error creating table: {e}")
        return False


def insert_data(session, table_name, record_id, value, timestamp, keyspace=None, consistency_level=ConsistencyLevel.ONE):
    """
    Insert data into a table
    
    Args:
        session: Cassandra session
        table_name: Name of the table
        record_id: Partition key value
        value: Value to insert
        timestamp: Timestamp value
        keyspace: Optional keyspace name (if not already set on session)
        consistency_level: Consistency level for the insert (default: ONE)
    
    Returns:
        True on success, False on failure
    """
    logging.info(f"Inserting data: id='{record_id}', value='{value}'...")
    
    try:
        if keyspace:
            full_table_name = f"{keyspace}.{table_name}"
        else:
            full_table_name = table_name
        
        insert_query = f"INSERT INTO {full_table_name} (id, value, timestamp) VALUES (%s, %s, %s)"
        log_cql_query(insert_query, (record_id, value, timestamp))
        insert_stmt = SimpleStatement(
            insert_query,
            consistency_level=consistency_level
        )
        session.execute(insert_stmt, (record_id, value, timestamp))
        logging.info(f"✓ Inserted data: id='{record_id}', value='{value}'")
        return True
    except Exception as e:
        logging.error(f"✗ Error inserting data: {e}")
        return False


def query_data(session, table_name, record_id, keyspace=None, consistency_level=ConsistencyLevel.ONE, max_retries=1, retry_delay=3):
    """
    Query data from a table by partition key
    
    Args:
        session: Cassandra session
        table_name: Name of the table
        record_id: Partition key value to query
        keyspace: Optional keyspace name (if not already set on session)
        consistency_level: Consistency level for the query (default: ONE)
        max_retries: Maximum number of retry attempts (default: 1)
        retry_delay: Delay between retries in seconds (default: 3)
    
    Returns:
        Row object if found, None if not found or error occurred
    """
    if keyspace:
        full_table_name = f"{keyspace}.{table_name}"
    else:
        full_table_name = table_name
    
    select_query = f"SELECT * FROM {full_table_name} WHERE id = %s"
    
    for attempt in range(max_retries):
        try:
            if max_retries > 1:
                logging.info(f"  Attempt {attempt + 1}/{max_retries}...")
            
            log_cql_query(select_query, (record_id,))
            select_stmt = SimpleStatement(
                select_query,
                consistency_level=consistency_level
            )
            result = session.execute(select_stmt, (record_id,))
            rows = list(result)
            
            if rows:
                row = rows[0]
                if max_retries == 1:
                    # Only log detailed info on single attempt
                    logging.info(f"✓ Data retrieved successfully:")
                    logging.info(f"  id: {row.id}")
                    logging.info(f"  value: {row.value}")
                    logging.info(f"  timestamp: {row.timestamp}")
                else:
                    # For retries, log success concisely
                    logging.info(f"✓ Data retrieved successfully!")
                    logging.info(f"  id: {row.id}")
                    logging.info(f"  value: {row.value}")
                    logging.info(f"  timestamp: {row.timestamp}")
                return row
            else:
                if max_retries > 1:
                    logging.warning(f"  No data returned (attempt {attempt + 1})")
                else:
                    logging.error("✗ Data not found!")
        except Exception as e:
            if max_retries > 1:
                logging.warning(f"  Error on attempt {attempt + 1}: {e}")
            else:
                logging.error(f"✗ Error reading data: {e}")
        
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
    
    return None


def get_partition_token(session, keyspace, table_name, partition_key):
    """
    Get the partition token for a given partition key
    
    Args:
        session: Cassandra session
        keyspace: Keyspace name
        table_name: Name of the table
        partition_key: Partition key value to get token for
    
    Returns:
        Token value (int) if found, None if not found or error occurred
    """
    try:
        full_table_name = f"{keyspace}.{table_name}"
        token_query = f"SELECT token(id) as token_value FROM {full_table_name} WHERE id = %s"
        
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
            
            token_value = rows[0].token_value
            logging.info(f"Token value from query for key '{partition_key}': {token_value}")
            return token_value
        
        return None
    except Exception as e:
        logging.warning(f"Could not get token via query: {e}")
        return None


def refresh_metadata(cluster, keyspace=None):
    """
    Refresh cluster metadata
    
    Args:
        cluster: Cassandra cluster object
        keyspace: Optional keyspace name to refresh metadata for
    
    Returns:
        True on success, False on failure
    """
    try:
        cluster.refresh_schema_metadata()
        if keyspace:
            cluster.refresh_keyspace_metadata(keyspace)
        logging.info("✓ Refreshed cluster metadata")
        return True
    except Exception as e:
        logging.warning(f"Could not refresh metadata: {e}")
        return False

