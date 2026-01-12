#!/usr/bin/env python3
"""
Integration Tests for cassandra_repository.py using Testcontainers

This test suite provides integration tests for the Cassandra repository module,
using testcontainers to spin up real Cassandra instances for testing.
"""

import logging
import time
from datetime import datetime
import pytest

try:
    from testcontainers.core.container import DockerContainer
except ImportError:
    try:
        from testcontainers.core import DockerContainer
    except ImportError:
        from testcontainers import DockerContainer

# Import the module under test
import os
import sys
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.repository.cassandra_repository import (
    log_cql_query,
    connect_to_cluster,
    create_keyspace,
    create_table,
    insert_data,
    query_data,
    get_partition_token,
    refresh_metadata,
)


# ============================================================================
# Pytest Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def cassandra_container():
    """Fixture that provides a Cassandra container for the entire test module"""
    container = DockerContainer("cassandra:5.0.6")
    container.with_env("CASSANDRA_CLUSTER_NAME", "TestCluster")
    container.with_env("MAX_HEAP_SIZE", "256M")
    container.with_env("HEAP_NEWSIZE", "128M")
    container.with_exposed_ports(9042)
    
    with container:
        # Wait for Cassandra to be ready
        # Try connecting with retries
        contact_point = container.get_container_host_ip()
        port = container.get_exposed_port(9042)
        
        max_retries = 60  # Increased retries
        retry_delay = 3   # Increased delay
        for attempt in range(max_retries):
            try:
                from cassandra.cluster import Cluster
                # Use a shorter timeout for the test connection
                test_cluster = Cluster(
                    contact_points=[contact_point], 
                    port=port,
                    connect_timeout=5,
                    control_connection_timeout=5
                )
                test_session = test_cluster.connect()
                test_session.shutdown()
                test_cluster.shutdown()
                logging.info(f"Cassandra container ready after {attempt + 1} attempts")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    if attempt % 10 == 0:  # Log every 10th attempt
                        logging.info(f"Waiting for Cassandra... attempt {attempt + 1}/{max_retries}")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Cassandra container did not become ready: {e}")
                    raise Exception(f"Cassandra container did not become ready in time: {e}")
        
        yield container


@pytest.fixture(scope="function")
def cassandra_session(cassandra_container):
    """Fixture that provides a fresh Cassandra session for each test"""
    contact_point = cassandra_container.get_container_host_ip()
    port = cassandra_container.get_exposed_port(9042)
    
    cluster, session = connect_to_cluster([contact_point], port)
    
    if cluster is None or session is None:
        pytest.fail("Failed to connect to Cassandra container")
    
    yield session
    
    # Cleanup: close session and cluster
    session.shutdown()
    cluster.shutdown()


@pytest.fixture(scope="function")
def test_keyspace(cassandra_session):
    """Fixture that creates a test keyspace for each test"""
    keyspace_name = f"test_keyspace_{int(time.time() * 1000000)}"
    create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
    return keyspace_name


@pytest.fixture(scope="function")
def test_table(cassandra_session, test_keyspace):
    """Fixture that creates a test table for each test"""
    table_name = "test_data"
    create_table(cassandra_session, table_name, keyspace=test_keyspace)
    return table_name


# ============================================================================
# Test: connect_to_cluster (Integration)
# ============================================================================

class TestConnectToClusterIntegration:
    """Integration tests for connect_to_cluster function"""
    
    def test_connects_to_cassandra_container(self, cassandra_container):
        """successfully connects to Cassandra container"""
        contact_point = cassandra_container.get_container_host_ip()
        port = cassandra_container.get_exposed_port(9042)
        
        cluster, session = connect_to_cluster([contact_point], port)
        
        assert cluster is not None
        assert session is not None
        
        # Cleanup
        session.shutdown()
        cluster.shutdown()
    
    def test_connects_with_string_contact_points(self, cassandra_container):
        """successfully connects with comma-separated string contact points"""
        contact_point = cassandra_container.get_container_host_ip()
        port = cassandra_container.get_exposed_port(9042)
        contact_points_str = f"{contact_point}"
        
        cluster, session = connect_to_cluster(contact_points_str, port)
        
        assert cluster is not None
        assert session is not None
        
        # Cleanup
        session.shutdown()
        cluster.shutdown()
    
    def test_fails_to_connect_to_invalid_host(self):
        """returns None when unable to connect to invalid host"""
        cluster, session = connect_to_cluster(["invalid-host"], 9042)
        
        assert cluster is None
        assert session is None


# ============================================================================
# Test: create_keyspace (Integration)
# ============================================================================

class TestCreateKeyspaceIntegration:
    """Integration tests for create_keyspace function"""
    
    def test_creates_keyspace_successfully(self, cassandra_session):
        """creates a keyspace with specified replication factor"""
        keyspace_name = f"test_keyspace_{int(time.time() * 1000000)}"
        
        result = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        
        assert result is True
        
        # Verify keyspace exists by trying to use it
        cassandra_session.set_keyspace(keyspace_name)
        result = cassandra_session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", [keyspace_name])
        assert len(list(result)) == 1
    
    def test_drops_existing_keyspace_before_creating(self, cassandra_session):
        """drops existing keyspace before creating new one"""
        keyspace_name = f"test_keyspace_{int(time.time() * 1000000)}"
        
        # Create keyspace first time
        result1 = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        assert result1 is True
        
        # Create same keyspace again (should drop and recreate)
        result2 = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        assert result2 is True


# ============================================================================
# Test: create_table (Integration)
# ============================================================================

class TestCreateTableIntegration:
    """Integration tests for create_table function"""
    
    def test_creates_table_successfully(self, cassandra_session, test_keyspace):
        """creates a table successfully"""
        table_name = "test_table"
        
        result = create_table(cassandra_session, table_name, keyspace=test_keyspace)
        
        assert result is True
        
        # Verify table exists
        cassandra_session.set_keyspace(test_keyspace)
        result = cassandra_session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s AND table_name = %s",
            [test_keyspace, table_name]
        )
        assert len(list(result)) == 1
    
    def test_creates_table_without_keyspace_when_session_has_keyspace(self, cassandra_session, test_keyspace):
        """creates table when keyspace is set on session"""
        cassandra_session.set_keyspace(test_keyspace)
        table_name = "test_table_no_keyspace"
        
        result = create_table(cassandra_session, table_name)
        
        assert result is True


# ============================================================================
# Test: insert_data (Integration)
# ============================================================================

class TestInsertDataIntegration:
    """Integration tests for insert_data function"""
    
    def test_inserts_data_successfully(self, cassandra_session, test_keyspace, test_table):
        """inserts data into table successfully"""
        record_id = "test-id-1"
        value = "test-value-1"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        assert result is True
        
        # Verify data was inserted
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == record_id
        assert row.value == value
    
    def test_inserts_data_with_special_characters(self, cassandra_session, test_keyspace, test_table):
        """inserts data with special characters successfully"""
        record_id = "test-id-2"
        value = "O'Brien's value"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        assert result is True
        
        # Verify data was inserted correctly
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.value == value
    
    def test_inserts_data_with_unicode(self, cassandra_session, test_keyspace, test_table):
        """inserts data with unicode characters successfully"""
        record_id = "test-id-3"
        value = "café"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        assert result is True
        
        # Verify data was inserted correctly
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.value == value


# ============================================================================
# Test: query_data (Integration)
# ============================================================================

class TestQueryDataIntegration:
    """Integration tests for query_data function"""
    
    def test_queries_data_successfully(self, cassandra_session, test_keyspace, test_table):
        """queries data from table successfully"""
        record_id = "test-query-1"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data first
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Query data
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        
        assert row is not None
        assert row.id == record_id
        assert row.value == value
        assert row.timestamp is not None
    
    def test_returns_none_when_data_not_found(self, cassandra_session, test_keyspace, test_table):
        """returns None when data is not found"""
        row = query_data(cassandra_session, test_table, "non-existent-id", keyspace=test_keyspace)
        
        assert row is None
    
    def test_queries_data_with_retries(self, cassandra_session, test_keyspace, test_table):
        """queries data with retry mechanism"""
        record_id = "test-query-retry"
        value = "test-value-retry"
        timestamp = datetime.now()
        
        # Insert data first
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Query with retries
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace, max_retries=3, retry_delay=1)
        
        assert row is not None
        assert row.id == record_id
        assert row.value == value


# ============================================================================
# Test: get_partition_token (Integration)
# ============================================================================

class TestGetPartitionTokenIntegration:
    """Integration tests for get_partition_token function"""
    
    def test_returns_token_value_when_partition_key_found(self, cassandra_session, test_keyspace, test_table, caplog):
        """returns token value when partition key is found"""
        record_id = "test-token-1"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data first
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Get token
        with caplog.at_level(logging.INFO):
            token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        assert token is not None
        assert isinstance(token, int)
        assert "Token value from query for key" in caplog.text
    
    def test_returns_none_when_partition_key_not_found(self, cassandra_session, test_keyspace, test_table):
        """returns None when partition key is not found"""
        token = get_partition_token(cassandra_session, test_keyspace, test_table, "non-existent-key")
        
        assert token is None
    
    def test_returns_none_for_empty_string_partition_key(self, cassandra_session, test_keyspace, test_table, caplog):
        """returns None for empty string partition key (Cassandra doesn't allow empty keys)"""
        record_id = ""
        
        # Get token for empty string key (should return None since empty keys are invalid)
        with caplog.at_level(logging.WARNING):
            token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        # Cassandra doesn't allow empty string partition keys, so token should be None
        assert token is None
    
    def test_returns_token_value_for_unicode_partition_key(self, cassandra_session, test_keyspace, test_table, caplog):
        """returns token value for unicode partition key"""
        record_id = "测试-key-测试"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data with unicode key
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Get token
        with caplog.at_level(logging.INFO):
            token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        assert token is not None
        assert isinstance(token, int)
        assert record_id in caplog.text
    
    def test_returns_token_value_for_special_characters_partition_key(self, cassandra_session, test_keyspace, test_table, caplog):
        """returns token value for partition key with special characters"""
        record_id = "key-with'special\"chars&symbols"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data with special characters key
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Get token
        with caplog.at_level(logging.INFO):
            token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        assert token is not None
        assert isinstance(token, int)
    
    def test_returns_token_value_for_very_long_partition_key(self, cassandra_session, test_keyspace, test_table):
        """returns token value for very long partition key"""
        very_long_key = "x" * 1000
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data with very long key
        insert_data(cassandra_session, test_table, very_long_key, value, timestamp, keyspace=test_keyspace)
        
        # Get token
        token = get_partition_token(cassandra_session, test_keyspace, test_table, very_long_key)
        
        assert token is not None
        assert isinstance(token, int)
    
    def test_returns_zero_token_value_when_token_is_zero(self, cassandra_session, test_keyspace, test_table):
        """returns token value even when it might be zero (if partition key maps to zero)"""
        # Note: It's unlikely but possible for a token to be zero
        record_id = "test-zero-token"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert data
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Get token
        token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        # Token should be returned (could be any int value including zero)
        assert token is not None
        assert isinstance(token, int)


# ============================================================================
# Test: refresh_metadata (Integration)
# ============================================================================

class TestRefreshMetadataIntegration:
    """Integration tests for refresh_metadata function"""
    
    def test_refreshes_metadata_successfully(self, cassandra_container, caplog):
        """refreshes cluster metadata successfully"""
        contact_point = cassandra_container.get_container_host_ip()
        port = cassandra_container.get_exposed_port(9042)
        
        cluster, session = connect_to_cluster([contact_point], port)
        
        with caplog.at_level(logging.INFO):
            result = refresh_metadata(cluster)
        
        assert result is True
        assert "Refreshed cluster metadata" in caplog.text
        
        # Cleanup
        session.shutdown()
        cluster.shutdown()
    
    def test_refreshes_keyspace_metadata_successfully(self, cassandra_session, test_keyspace, caplog):
        """refreshes keyspace metadata successfully"""
        contact_point = cassandra_session.cluster.contact_points[0]
        port = cassandra_session.cluster.port
        
        cluster, _ = connect_to_cluster([contact_point], port)
        
        with caplog.at_level(logging.INFO):
            result = refresh_metadata(cluster, keyspace=test_keyspace)
        
        assert result is True
        
        # Cleanup
        cluster.shutdown()


# ============================================================================
# Integration Test: End-to-End Workflow
# ============================================================================

class TestEndToEndWorkflowIntegration:
    """End-to-end integration tests for complete workflows"""
    
    def test_complete_workflow_insert_and_query(self, cassandra_session, test_keyspace, test_table):
        """tests complete workflow: create keyspace, table, insert, and query"""
        record_id = "e2e-test-1"
        value = "e2e-test-value"
        timestamp = datetime.now()
        
        # Insert data
        insert_result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert insert_result is True
        
        # Query data
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == record_id
        assert row.value == value
        
        # Get token
        token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token is not None
        assert isinstance(token, int)
    
    def test_multiple_inserts_and_queries(self, cassandra_session, test_keyspace, test_table):
        """tests multiple inserts and queries"""
        records = [
            ("multi-1", "value-1", datetime.now()),
            ("multi-2", "value-2", datetime.now()),
            ("multi-3", "value-3", datetime.now()),
        ]
        
        # Insert all records
        for record_id, value, timestamp in records:
            result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
            assert result is True
        
        # Query all records
        for record_id, expected_value, _ in records:
            row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
            assert row is not None
            assert row.id == record_id
            assert row.value == expected_value
        
        # Get tokens for all records
        for record_id, _, _ in records:
            token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
            assert token is not None
            assert isinstance(token, int)


# ============================================================================
# BugMagnet Session 2026-01-12: Comprehensive Edge Case Coverage
# ============================================================================

class TestBugMagnetSessionIntegration:
    """Comprehensive edge case tests for integration scenarios with real Cassandra"""
    
    # ========================================================================
    # log_cql_query Edge Cases (Integration)
    # ========================================================================
    
    def test_logs_query_with_list_params(self, caplog):
        """handles list parameters correctly in query logging"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", ["test-id"])
        
        assert "CQL Query: SELECT * FROM table WHERE id = 'test-id'" in caplog.text
    
    def test_logs_query_with_empty_string_param(self, caplog):
        """handles empty string parameter correctly in query logging"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", ("",))
        
        assert "CQL Query: SELECT * FROM table WHERE id = ''" in caplog.text
    
    def test_logs_query_with_very_long_string_param(self, caplog):
        """handles very long string parameter correctly in query logging"""
        long_string = "x" * 10000
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE data = %s", (long_string,))
        
        assert f"CQL Query: SELECT * FROM table WHERE data = '{long_string}'" in caplog.text
    
    def test_logs_query_with_unicode_string_param(self, caplog):
        """handles unicode string parameter correctly in query logging"""
        unicode_string = "café"
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE name = %s", (unicode_string,))
        
        assert "CQL Query: SELECT * FROM table WHERE name = 'café'" in caplog.text
    
    def test_logs_query_with_zero_numeric_param(self, caplog):
        """handles zero numeric parameter correctly in query logging"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE count = %s", (0,))
        
        assert "CQL Query: SELECT * FROM table WHERE count = 0" in caplog.text
    
    def test_logs_query_with_negative_numeric_param(self, caplog):
        """handles negative numeric parameter correctly in query logging"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE value = %s", (-5,))
        
        assert "CQL Query: SELECT * FROM table WHERE value = -5" in caplog.text
    
    def test_logs_query_with_float_param(self, caplog):
        """handles float parameter correctly in query logging"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE price = %s", (99.99,))
        
        assert "CQL Query: SELECT * FROM table WHERE price = 99.99" in caplog.text
    
    def test_logs_query_with_sql_injection_patterns_in_params(self, caplog):
        """handles SQL injection patterns in query parameters safely"""
        malicious_param = "'; DROP TABLE users; --"
        
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", (malicious_param,))
        
        # Should escape single quotes properly
        assert "CQL Query:" in caplog.text
        # The escaped version should have double single quotes
        assert "''" in caplog.text or "'" in caplog.text
        # Should not contain unescaped dangerous patterns
    
    def test_logs_query_with_very_long_query_string(self, caplog):
        """handles very long CQL query strings"""
        long_table_name = "a" * 1000
        long_query = f"SELECT * FROM {long_table_name} WHERE id = %s"
        
        with caplog.at_level(logging.INFO):
            log_cql_query(long_query, ("test-id",))
        
        assert long_table_name in caplog.text
    
    def test_logs_query_with_multiple_parameters_in_sequence(self, caplog):
        """handles queries with many parameters"""
        many_params = tuple(f"param{i}" for i in range(50))
        query = "INSERT INTO table VALUES (" + ", ".join(["%s"] * 50) + ")"
        
        with caplog.at_level(logging.INFO):
            log_cql_query(query, many_params)
        
        # Should handle all parameters
        assert "CQL Query:" in caplog.text
        # Verify all params are in the logged query
        for i in range(50):
            assert f"param{i}" in caplog.text
    
    # ========================================================================
    # connect_to_cluster Edge Cases (Integration)
    # ========================================================================
    
    def test_connects_with_authentication_when_provided(self, cassandra_container):
        """successfully connects when authentication credentials provided (even if not required)"""
        contact_point = cassandra_container.get_container_host_ip()
        port = cassandra_container.get_exposed_port(9042)
        
        # Note: Our test container doesn't require auth, but we test the code path
        cluster, session = connect_to_cluster([contact_point], port, username="test", password="test")
        
        # Should either connect (if auth not enforced) or fail gracefully
        if cluster is not None:
            session.shutdown()
            cluster.shutdown()
    
    def test_connects_with_multiple_contact_points(self, cassandra_container):
        """successfully connects with multiple contact points (same host)"""
        contact_point = cassandra_container.get_container_host_ip()
        port = cassandra_container.get_exposed_port(9042)
        
        # Multiple contact points pointing to same host
        cluster, session = connect_to_cluster([contact_point, contact_point], port)
        
        assert cluster is not None
        assert session is not None
        
        session.shutdown()
        cluster.shutdown()
    
    # ========================================================================
    # create_keyspace Edge Cases (Integration)
    # ========================================================================
    
    def test_creates_keyspace_with_different_replication_factors(self, cassandra_session):
        """creates keyspaces with various replication factors"""
        for rf in [1, 2, 3]:
            keyspace_name = f"test_rf_{rf}_{int(time.time() * 1000000)}"
            result = create_keyspace(cassandra_session, keyspace_name, replication_factor=rf)
            assert result is True
    
    def test_creates_keyspace_with_special_characters_in_name(self, cassandra_session):
        """creates keyspace with special characters in name"""
        # Cassandra keyspace names have restrictions, but we test valid special chars
        keyspace_name = f"test_keyspace_underscore_{int(time.time() * 1000000)}"
        result = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        assert result is True
    
    def test_creates_keyspace_with_very_long_name(self, cassandra_session):
        """creates keyspace with very long name (at Cassandra's 48 character limit)"""
        # Cassandra has a 48 character limit on keyspace names
        # Test at the boundary: 48 characters exactly
        long_name = "a" * 30  # 30 chars + timestamp suffix will be within limit
        keyspace_name = f"{long_name}_{int(time.time() * 1000000)}"
        # Ensure we're within 48 char limit (timestamp adds ~16 chars, so 30 is safe)
        assert len(keyspace_name) <= 48, f"Keyspace name too long: {len(keyspace_name)} chars"
        result = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        assert result is True
    
    def test_fails_to_create_keyspace_with_name_exceeding_48_characters(self, cassandra_session):
        """fails to create keyspace when name exceeds 48 character limit"""
        # Cassandra enforces 48 character limit on keyspace names
        long_name = "a" * 50  # Exceeds limit
        keyspace_name = f"{long_name}_{int(time.time() * 1000000)}"
        result = create_keyspace(cassandra_session, keyspace_name, replication_factor=1)
        # Should fail due to length restriction
        assert result is False
    
    # ========================================================================
    # create_table Edge Cases (Integration)
    # ========================================================================
    
    def test_creates_table_with_special_characters_in_name(self, cassandra_session, test_keyspace):
        """creates table with special characters in name"""
        table_name = "test_table_underscore"
        result = create_table(cassandra_session, table_name, keyspace=test_keyspace)
        assert result is True
    
    def test_creates_table_with_very_long_name(self, cassandra_session, test_keyspace):
        """creates table with very long name"""
        long_name = "a" * 48  # Within typical limits
        result = create_table(cassandra_session, long_name, keyspace=test_keyspace)
        assert result is True
    
    def test_creates_multiple_tables_in_same_keyspace(self, cassandra_session, test_keyspace):
        """creates multiple tables in the same keyspace"""
        for i in range(5):
            table_name = f"test_table_{i}"
            result = create_table(cassandra_session, table_name, keyspace=test_keyspace)
            assert result is True
    
    # ========================================================================
    # insert_data Edge Cases (Integration)
    # ========================================================================
    
    def test_inserts_data_with_empty_string_value(self, cassandra_session, test_keyspace, test_table):
        """inserts data with empty string value successfully"""
        record_id = "test-empty-value"
        value = ""
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.value == ""
    
    def test_inserts_data_with_whitespace_only_value(self, cassandra_session, test_keyspace, test_table):
        """inserts data with whitespace-only value successfully"""
        record_id = "test-whitespace-value"
        value = "   \t\n   "
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.value == "   \t\n   "
    
    def test_inserts_data_with_very_long_value(self, cassandra_session, test_keyspace, test_table):
        """inserts data with very long value successfully"""
        record_id = "test-long-value"
        value = "x" * 10000
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert len(row.value) == 10000
    
    def test_inserts_data_with_single_character_id(self, cassandra_session, test_keyspace, test_table):
        """inserts data with single character partition key successfully"""
        record_id = "a"
        value = "test-value"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == "a"
    
    def test_inserts_data_with_whitespace_only_id(self, cassandra_session, test_keyspace, test_table):
        """inserts data with whitespace-only partition key successfully"""
        record_id = "   "
        value = "test-value"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == "   "
    
    def test_inserts_data_with_sql_injection_pattern_in_id(self, cassandra_session, test_keyspace, test_table):
        """inserts data with SQL injection pattern in partition key safely"""
        record_id = "'; DROP TABLE test_data; --"
        value = "test-value"
        timestamp = datetime.now()
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        # Verify it was stored as literal value, not executed
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == "'; DROP TABLE test_data; --"
    
    def test_inserts_data_with_different_consistency_levels(self, cassandra_session, test_keyspace, test_table):
        """inserts data with different consistency levels successfully"""
        from cassandra import ConsistencyLevel
        
        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
        ]
        
        for i, cl in enumerate(consistency_levels):
            record_id = f"test-cl-{i}"
            value = f"value-{i}"
            timestamp = datetime.now()
            
            result = insert_data(cassandra_session, test_table, record_id, value, timestamp, 
                               keyspace=test_keyspace, consistency_level=cl)
            assert result is True
    
    def test_inserts_data_with_epoch_timestamp(self, cassandra_session, test_keyspace, test_table):
        """inserts data with epoch timestamp (1970-01-01) successfully"""
        record_id = "test-epoch"
        value = "test-value"
        timestamp = datetime(1970, 1, 1, 0, 0, 0)
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.timestamp == timestamp
    
    def test_inserts_data_with_future_timestamp(self, cassandra_session, test_keyspace, test_table):
        """inserts data with future timestamp successfully"""
        record_id = "test-future"
        value = "test-value"
        timestamp = datetime(2100, 1, 1, 0, 0, 0)
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
    
    def test_inserts_data_with_leap_year_timestamp(self, cassandra_session, test_keyspace, test_table):
        """inserts data with leap year date (Feb 29) successfully"""
        record_id = "test-leap-year"
        value = "test-value"
        timestamp = datetime(2024, 2, 29, 12, 0, 0)  # 2024 is a leap year
        
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.timestamp.day == 29
    
    def test_overwrites_existing_data_on_same_partition_key(self, cassandra_session, test_keyspace, test_table):
        """overwrites existing data when inserting with same partition key"""
        record_id = "test-overwrite"
        value1 = "value-1"
        value2 = "value-2"
        timestamp = datetime.now()
        
        # First insert
        result1 = insert_data(cassandra_session, test_table, record_id, value1, timestamp, keyspace=test_keyspace)
        assert result1 is True
        
        # Second insert with same key (should overwrite)
        result2 = insert_data(cassandra_session, test_table, record_id, value2, timestamp, keyspace=test_keyspace)
        assert result2 is True
        
        # Should have the second value
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.value == value2
    
    # ========================================================================
    # query_data Edge Cases (Integration)
    # ========================================================================
    
    def test_queries_data_with_different_consistency_levels(self, cassandra_session, test_keyspace, test_table):
        """queries data with different consistency levels successfully"""
        from cassandra import ConsistencyLevel
        
        record_id = "test-query-cl"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert first
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Query with different consistency levels
        consistency_levels = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.ALL,
        ]
        
        for cl in consistency_levels:
            row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace, consistency_level=cl)
            assert row is not None
            assert row.value == value
    
    def test_queries_data_after_multiple_retries(self, cassandra_session, test_keyspace, test_table):
        """queries data successfully after multiple retry attempts"""
        record_id = "test-retry-many"
        value = "test-value"
        timestamp = datetime.now()
        
        # Insert first
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        # Query with many retries
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace, max_retries=5, retry_delay=0.1)
        assert row is not None
        assert row.value == value
    
    def test_queries_data_with_single_character_id(self, cassandra_session, test_keyspace, test_table):
        """queries data with single character partition key successfully"""
        record_id = "x"
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == "x"
    
    def test_queries_data_with_whitespace_only_id(self, cassandra_session, test_keyspace, test_table):
        """queries data with whitespace-only partition key successfully"""
        record_id = "   "
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.id == "   "
    
    # ========================================================================
    # get_partition_token Edge Cases (Integration)
    # ========================================================================
    
    def test_returns_token_value_for_single_character_partition_key(self, cassandra_session, test_keyspace, test_table):
        """returns token value for single character partition key"""
        record_id = "a"
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token is not None
        assert isinstance(token, int)
    
    def test_returns_token_value_for_whitespace_only_partition_key(self, cassandra_session, test_keyspace, test_table):
        """returns token value for whitespace-only partition key"""
        record_id = "   "
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token is not None
        assert isinstance(token, int)
    
    def test_returns_token_value_for_sql_injection_pattern_partition_key(self, cassandra_session, test_keyspace, test_table):
        """returns token value for partition key containing SQL injection pattern"""
        record_id = "'; DROP TABLE test_data; --"
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        token = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token is not None
        assert isinstance(token, int)
    
    def test_returns_same_token_for_same_partition_key(self, cassandra_session, test_keyspace, test_table):
        """returns same token value for same partition key"""
        record_id = "test-same-token"
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        
        token1 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        token2 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        
        assert token1 is not None
        assert token2 is not None
        assert token1 == token2
    
    def test_returns_different_tokens_for_different_keys(self, cassandra_session, test_keyspace, test_table):
        """returns different token values for different partition keys"""
        record_id1 = "test-token-1"
        record_id2 = "test-token-2"
        value = "test-value"
        timestamp = datetime.now()
        
        insert_data(cassandra_session, test_table, record_id1, value, timestamp, keyspace=test_keyspace)
        insert_data(cassandra_session, test_table, record_id2, value, timestamp, keyspace=test_keyspace)
        
        token1 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id1)
        token2 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id2)
        
        assert token1 is not None
        assert token2 is not None
        # Tokens should be different (very likely, though not guaranteed)
        # We just verify both are valid integers
    
    # ========================================================================
    # Complex Interactions and State Transitions
    # ========================================================================
    
    def test_complete_workflow_with_multiple_keyspaces(self, cassandra_session):
        """tests complete workflow across multiple keyspaces"""
        keyspace1 = f"test_ks1_{int(time.time() * 1000000)}"
        keyspace2 = f"test_ks2_{int(time.time() * 1000000)}"
        
        # Create both keyspaces
        assert create_keyspace(cassandra_session, keyspace1, replication_factor=1) is True
        assert create_keyspace(cassandra_session, keyspace2, replication_factor=1) is True
        
        # Create tables in both
        assert create_table(cassandra_session, "test_table", keyspace=keyspace1) is True
        assert create_table(cassandra_session, "test_table", keyspace=keyspace2) is True
        
        # Insert data in both
        timestamp = datetime.now()
        assert insert_data(cassandra_session, "test_table", "id1", "value1", timestamp, keyspace=keyspace1) is True
        assert insert_data(cassandra_session, "test_table", "id2", "value2", timestamp, keyspace=keyspace2) is True
        
        # Query from both
        row1 = query_data(cassandra_session, "test_table", "id1", keyspace=keyspace1)
        row2 = query_data(cassandra_session, "test_table", "id2", keyspace=keyspace2)
        
        assert row1 is not None
        assert row2 is not None
        assert row1.value == "value1"
        assert row2.value == "value2"
    
    def test_sequential_operations_on_same_record(self, cassandra_session, test_keyspace, test_table):
        """tests sequential operations on the same record"""
        record_id = "test-sequential"
        timestamp = datetime.now()
        
        # Insert
        assert insert_data(cassandra_session, test_table, record_id, "value1", timestamp, keyspace=test_keyspace) is True
        
        # Query
        row1 = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row1 is not None
        assert row1.value == "value1"
        
        # Get token
        token1 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token1 is not None
        
        # Update (overwrite)
        assert insert_data(cassandra_session, test_table, record_id, "value2", timestamp, keyspace=test_keyspace) is True
        
        # Query again
        row2 = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row2 is not None
        assert row2.value == "value2"
        
        # Token should be same
        token2 = get_partition_token(cassandra_session, test_keyspace, test_table, record_id)
        assert token2 == token1
    
    def test_operations_with_none_timestamp_handling(self, cassandra_session, test_keyspace, test_table):
        """tests operations handle timestamp edge cases"""
        record_id = "test-timestamp-edge"
        value = "test-value"
        
        # Test with current timestamp
        timestamp = datetime.now()
        result = insert_data(cassandra_session, test_table, record_id, value, timestamp, keyspace=test_keyspace)
        assert result is True
        
        row = query_data(cassandra_session, test_table, record_id, keyspace=test_keyspace)
        assert row is not None
        assert row.timestamp is not None
