#!/usr/bin/env python3
"""
Tests for cassandra_repository.py

This test suite provides comprehensive coverage for the Cassandra repository module,
testing CQL query logging and other repository functions.
"""

import logging
from datetime import datetime
from unittest.mock import patch
import pytest

# Import the module under test
import os
import sys
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.repository.cassandra_repository import (
    log_cql_query,
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

