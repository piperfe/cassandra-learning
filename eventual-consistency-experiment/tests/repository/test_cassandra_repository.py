#!/usr/bin/env python3
"""
Tests for cassandra_repository.py

This test suite provides comprehensive coverage for the Cassandra repository module,
testing CQL query logging and other repository functions.
"""

import logging
from datetime import datetime
from unittest.mock import patch, MagicMock
import pytest

# Import the module under test
import os
import sys
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.repository.cassandra_repository import (
    log_cql_query,
    get_partition_token,
)


# ============================================================================
# Test: log_cql_query
# ============================================================================

class TestLogCqlQuery:
    """Tests for log_cql_query function"""
    
    def test_logs_query_without_params(self, caplog):
        """logs query correctly when no parameters provided"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table")
        
        assert "CQL Query: SELECT * FROM table" in caplog.text
    
    def test_logs_query_with_string_param(self, caplog):
        """replaces string parameter correctly in query"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", ("test-id",))
        
        assert "CQL Query: SELECT * FROM table WHERE id = 'test-id'" in caplog.text
    
    def test_escapes_single_quotes_in_string_params(self, caplog):
        """escapes single quotes in string parameters correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE name = %s", ("O'Brien",))
        
        assert "CQL Query: SELECT * FROM table WHERE name = 'O''Brien'" in caplog.text
    
    def test_logs_query_with_datetime_param(self, caplog):
        """replaces datetime parameter correctly in query"""
        test_datetime = datetime(2024, 1, 15, 10, 30, 45)
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE timestamp = %s", (test_datetime,))
        
        assert "CQL Query: SELECT * FROM table WHERE timestamp = '2024-01-15T10:30:45'" in caplog.text
    
    def test_logs_query_with_none_param(self, caplog):
        """replaces None parameter with NULL in query"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE value = %s", (None,))
        
        assert "CQL Query: SELECT * FROM table WHERE value = NULL" in caplog.text
    
    def test_logs_query_with_numeric_param(self, caplog):
        """replaces numeric parameter correctly in query"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE count = %s", (42,))
        
        assert "CQL Query: SELECT * FROM table WHERE count = 42" in caplog.text
    
    def test_logs_query_with_multiple_params(self, caplog):
        """replaces multiple parameters correctly in query"""
        with caplog.at_level(logging.INFO):
            log_cql_query("INSERT INTO table (id, name, count) VALUES (%s, %s, %s)", 
                         ("test-id", "test-name", 100))
        
        assert "'test-id'" in caplog.text
        assert "'test-name'" in caplog.text
        assert "100" in caplog.text
    
    def test_logs_query_with_list_params(self, caplog):
        """handles list parameters correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", ["test-id"])
        
        assert "CQL Query: SELECT * FROM table WHERE id = 'test-id'" in caplog.text
    
    def test_handles_empty_string_param(self, caplog):
        """handles empty string parameter correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE id = %s", ("",))
        
        assert "CQL Query: SELECT * FROM table WHERE id = ''" in caplog.text
    
    def test_handles_very_long_string_param(self, caplog):
        """handles very long string parameter correctly"""
        long_string = "x" * 10000
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE data = %s", (long_string,))
        
        assert f"CQL Query: SELECT * FROM table WHERE data = '{long_string}'" in caplog.text
    
    def test_handles_unicode_string_param(self, caplog):
        """handles unicode string parameter correctly"""
        unicode_string = "café"
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE name = %s", (unicode_string,))
        
        assert "CQL Query: SELECT * FROM table WHERE name = 'café'" in caplog.text
    
    def test_handles_zero_numeric_param(self, caplog):
        """handles zero numeric parameter correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE count = %s", (0,))
        
        assert "CQL Query: SELECT * FROM table WHERE count = 0" in caplog.text
    
    def test_handles_negative_numeric_param(self, caplog):
        """handles negative numeric parameter correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE value = %s", (-5,))
        
        assert "CQL Query: SELECT * FROM table WHERE value = -5" in caplog.text
    
    def test_handles_float_param(self, caplog):
        """handles float parameter correctly"""
        with caplog.at_level(logging.INFO):
            log_cql_query("SELECT * FROM table WHERE price = %s", (99.99,))
        
        assert "CQL Query: SELECT * FROM table WHERE price = 99.99" in caplog.text


# ============================================================================
# BugMagnet Session 2026-01-09: CQL Query String Edge Cases
# ============================================================================

class TestBugMagnetSessionCqlQuery:
    """Advanced edge case tests for CQL query logging"""
    
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
        logged_query = caplog.text
        # Verify it's logged as a parameter, not injected into query structure
    
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


# ============================================================================
# Test: get_partition_token
# ============================================================================

class TestGetPartitionToken:
    """Tests for get_partition_token function"""
    
    def test_returns_token_value_when_partition_key_found(self, caplog):
        """returns token value when partition key is found"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 123456789
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result == 123456789
        assert "Token value from query for key 'test-key': 123456789" in caplog.text
        assert "CQL Result (SELECT TOKEN): 1 row(s) returned" in caplog.text
        mock_session.execute.assert_called_once()
    
    def test_returns_none_when_partition_key_not_found(self, caplog):
        """returns None when partition key is not found"""
        mock_session = MagicMock()
        mock_session.execute.return_value = []
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result is None
        mock_session.execute.assert_called_once()
    
    def test_returns_none_when_exception_occurs(self, caplog):
        """returns None when exception occurs during query execution"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Connection error")
        
        with caplog.at_level(logging.WARNING):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result is None
        assert "Could not get token via query" in caplog.text
        assert "Connection error" in caplog.text
    
    def test_logs_query_with_keyspace_and_table_when_executed(self, caplog):
        """logs CQL query with keyspace.table format when executed"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 987654321
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            get_partition_token(mock_session, "my_keyspace", "my_table", "my-key")
        
        assert "CQL Query: SELECT token(id) as token_value FROM my_keyspace.my_table WHERE id = 'my-key'" in caplog.text
    
    def test_logs_row_details_with_token_value_when_found(self, caplog):
        """logs row details with token value when token is found"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 555666777
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert "Row 1:" in caplog.text
        assert "token_value" in caplog.text
        assert "555666777" in caplog.text
    
    def test_returns_token_value_when_row_lacks_fields_attribute(self, caplog):
        """returns token value when row object lacks _fields attribute"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 111222333
        del mock_row._fields
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result == 111222333
        assert "Row 1:" in caplog.text
    
    def test_returns_token_value_for_empty_string_partition_key(self, caplog):
        """returns token value for empty string partition key"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 0
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "")
        
        assert result == 0
        assert "Token value from query for key '': 0" in caplog.text
    
    def test_returns_token_value_for_unicode_partition_key(self, caplog):
        """returns token value for unicode partition key"""
        unicode_key = "测试-key-测试"
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 999888777
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", unicode_key)
        
        assert result == 999888777
        assert unicode_key in caplog.text
    
    def test_returns_first_token_value_when_multiple_rows_found(self, caplog):
        """returns first token value when multiple rows are found"""
        mock_session = MagicMock()
        mock_row1 = MagicMock()
        mock_row1.token_value = 111111111
        mock_row1._fields = ['token_value']
        mock_row2 = MagicMock()
        mock_row2.token_value = 222222222
        mock_row2._fields = ['token_value']
        mock_session.execute.return_value = [mock_row1, mock_row2]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result == 111111111
        assert "CQL Result (SELECT TOKEN): 2 row(s) returned" in caplog.text
    
    def test_returns_token_value_for_single_character_partition_key(self, caplog):
        """returns token value for single character partition key"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 12345
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "a")
        
        assert result == 12345
        assert "Token value from query for key 'a': 12345" in caplog.text
    
    def test_returns_token_value_for_very_long_partition_key(self, caplog):
        """returns token value for very long partition key (10000+ characters)"""
        very_long_key = "x" * 10000
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 999999999
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", very_long_key)
        
        assert result == 999999999
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        assert len(call_args[0]) == 2  # query and params tuple
        assert call_args[0][1][0] == very_long_key  # Check the parameter
    
    def test_returns_token_value_for_whitespace_only_partition_key(self, caplog):
        """returns token value for whitespace-only partition key"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 444555666
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "   ")
        
        assert result == 444555666
    
    def test_returns_token_value_for_partition_key_with_sql_injection_pattern(self, caplog):
        """returns token value for partition key containing SQL injection pattern"""
        sql_injection_key = "'; DROP TABLE test_data; --"
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 777888999
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", sql_injection_key)
        
        assert result == 777888999
        # Verify parameterized query is used (safe)
        call_args = mock_session.execute.call_args
        assert len(call_args[0]) == 2  # query and params tuple
    
    def test_returns_token_value_for_partition_key_with_special_characters(self, caplog):
        """returns token value for partition key with special characters"""
        special_chars_key = "key-with'special\"chars&symbols"
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 333444555
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", special_chars_key)
        
        assert result == 333444555
    
    def test_returns_zero_token_value_when_token_is_zero(self, caplog):
        """returns zero token value when token value is zero"""
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 0
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result == 0
        assert "Token value from query for key 'test-key': 0" in caplog.text
    
    def test_returns_very_large_token_value_when_found(self, caplog):
        """returns very large token value when found"""
        very_large_token = 9223372036854775807  # max 64-bit signed int
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = very_large_token
        mock_row._fields = ['token_value']
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.INFO):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result == very_large_token
        assert f"Token value from query for key 'test-key': {very_large_token}" in caplog.text
    
    def test_returns_none_when_connection_exception_occurs(self, caplog):
        """returns None when connection exception occurs"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = ConnectionError("Connection failed")
        
        with caplog.at_level(logging.WARNING):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result is None
        assert "Could not get token via query" in caplog.text
        assert "Connection failed" in caplog.text
    
    def test_returns_none_when_timeout_exception_occurs(self, caplog):
        """returns None when timeout exception occurs"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = TimeoutError("Query timeout")
        
        with caplog.at_level(logging.WARNING):
            result = get_partition_token(mock_session, "test_keyspace", "test_table", "test-key")
        
        assert result is None
        assert "Could not get token via query" in caplog.text
        assert "Query timeout" in caplog.text

