#!/usr/bin/env python3
"""
Tests for src.application.replica_resolver.py

This test suite provides comprehensive coverage for the replica resolver module,
testing individual functions and edge cases related to resolving replica nodes.
"""

import logging
import os
from unittest.mock import Mock, patch, MagicMock
import pytest

from cassandra.metadata import Murmur3Token

# Import the module under test
import sys
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.application.replica_resolver import get_replica_nodes


# ============================================================================
# Test: get_replica_nodes
# ============================================================================

class TestGetReplicaNodes:
    """Tests for get_replica_nodes function"""
    
    def test_returns_empty_list_when_keyspace_not_found(self, caplog):
        """returns empty list when keyspace does not exist in cluster metadata"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {}  # Empty keyspaces
        
        mock_session = MagicMock()
        
        with caplog.at_level(logging.ERROR):
            result = get_replica_nodes(mock_cluster, mock_session, "nonexistent_keyspace", "test-key")
        
        assert result == []
        assert "Keyspace 'nonexistent_keyspace' not found" in caplog.text
    
    def test_returns_empty_list_when_token_map_not_available(self, caplog):
        """returns empty list when token map is not available"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.token_map = None  # No token map
        
        mock_session = MagicMock()
        mock_row = MagicMock()
        mock_row.token_value = 123456789
        mock_session.execute.return_value = [mock_row]
        
        with caplog.at_level(logging.ERROR):
            result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == []
        assert "Token map not available" in caplog.text
    
    def test_returns_replica_addresses_when_successful(self, caplog):
        """returns list of replica node addresses when successful"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        # Mock token query result
        mock_row = MagicMock()
        mock_row.token_value = 123456789
        mock_row._fields = ['token_value']
        mock_session = MagicMock()
        mock_session.execute.return_value = [mock_row]
        
        # Mock token map and replicas
        mock_replica1 = MagicMock()
        mock_replica1.address = "127.0.0.1"
        mock_replica2 = MagicMock()
        mock_replica2.address = "127.0.0.2"
        
        mock_token_map = MagicMock()
        mock_token = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica1, mock_replica2]
        
        mock_cluster.metadata.token_map = mock_token_map
        
        # Murmur3Token is imported inside the function, so we patch it at the cassandra.metadata level
        with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
            mock_token_instance = MagicMock()
            mock_token_class.return_value = mock_token_instance
            
            with caplog.at_level(logging.INFO):
                result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == ["127.0.0.1", "127.0.0.2"]
        mock_token_map.get_replicas.assert_called_once()
    
    def test_uses_mmh3_fallback_when_query_fails(self, caplog):
        """uses mmh3 algorithm as fallback when query method fails"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        # Mock query to fail
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        # Mock token map
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 987654321
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):  # Changed from WARNING to INFO to capture INFO messages
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == ["127.0.0.1"]
        assert "Could not get token via query" in caplog.text
        assert "Using token from mmh3 algorithm (fallback)" in caplog.text
    
    def test_returns_empty_list_when_both_methods_fail(self, caplog):
        """returns empty list when both query and mmh3 methods fail"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        # Mock query to fail
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        # Mock mmh3 to fail
        with patch('mmh3.hash', side_effect=ImportError("mmh3 not available")):
            with caplog.at_level(logging.ERROR):
                result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == []
        assert "Both token calculation methods failed" in caplog.text
    
    def test_handles_empty_query_result(self, caplog):
        """handles empty query result gracefully"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        # Mock empty query result
        mock_session = MagicMock()
        mock_session.execute.return_value = []  # No rows
        
        # Mock mmh3 as fallback
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 987654321
            
            mock_replica = MagicMock()
            mock_replica.address = "127.0.0.1"
            mock_token_map = MagicMock()
            mock_token_map.get_replicas.return_value = [mock_replica]
            mock_cluster.metadata.token_map = mock_token_map
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == ["127.0.0.1"]
        assert "Using token from mmh3 algorithm (fallback)" in caplog.text
    
    def test_logs_token_comparison_when_both_methods_succeed(self, caplog):
        """logs token comparison when both query and mmh3 methods succeed"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        # Mock query result
        mock_row = MagicMock()
        mock_row.token_value = 123456789
        mock_row._fields = ['token_value']
        mock_session = MagicMock()
        mock_session.execute.return_value = [mock_row]
        
        # Mock mmh3 to return same value
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 123456789
            
            mock_replica = MagicMock()
            mock_replica.address = "127.0.0.1"
            mock_token_map = MagicMock()
            mock_token_map.get_replicas.return_value = [mock_replica]
            mock_cluster.metadata.token_map = mock_token_map
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert "Both methods match" in caplog.text or "Methods differ" in caplog.text
    
    def test_handles_non_murmur3_partitioner(self, caplog):
        """handles non-Murmur3 partitioner gracefully"""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.RandomPartitioner"  # Not Murmur3
        
        # Mock query result
        mock_row = MagicMock()
        mock_row.token_value = 123456789
        mock_row._fields = ['token_value']
        mock_session = MagicMock()
        mock_session.execute.return_value = [mock_row]
        
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
            mock_token_instance = MagicMock()
            mock_token_class.return_value = mock_token_instance
            
            with caplog.at_level(logging.WARNING):
                result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", "test-key")
        
        assert result == ["127.0.0.1"]
        assert "not Murmur3" in caplog.text


# ============================================================================
# Edge Case Tests
# ============================================================================

class TestGetReplicaNodesEdgeCases:
    """Edge case tests for get_replica_nodes function"""
    
    def test_handles_very_long_partition_key(self, caplog):
        """handles partition keys at size boundaries (10000+ characters)"""
        very_long_key = "x" * 10000
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 123456789
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", very_long_key)
        
        assert result == ["127.0.0.1"]
        # Verify mmh3 was called with the long key
        mock_mmh3.assert_called_once()
        call_args = mock_mmh3.call_args[0][0]
        assert len(call_args) == 10000
    
    def test_handles_partition_key_with_unicode(self, caplog):
        """handles partition keys with unicode characters"""
        unicode_key = "测试-key-测试"
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 987654321
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", unicode_key)
        
        assert result == ["127.0.0.1"]
        # Verify mmh3 was called with utf-8 encoded unicode
        mock_mmh3.assert_called_once()
        call_args = mock_mmh3.call_args[0][0]
        assert isinstance(call_args, bytes)
        assert unicode_key.encode('utf-8') == call_args
    
    def test_handles_partition_key_with_sql_injection_patterns(self, caplog):
        """handles partition keys that look like SQL injection attempts"""
        sql_injection_key = "'; DROP TABLE test_data; --"
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 111222333
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", sql_injection_key)
        
        assert result == ["127.0.0.1"]
        # Should handle safely (parameterized queries prevent injection)
        mock_mmh3.assert_called_once()
    
    def test_handles_empty_partition_key(self, caplog):
        """handles empty string partition key"""
        empty_key = ""
        mock_cluster = MagicMock()
        mock_cluster.metadata.keyspaces = {"test_keyspace": MagicMock()}
        mock_cluster.metadata.partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
        
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")
        
        mock_replica = MagicMock()
        mock_replica.address = "127.0.0.1"
        mock_token_map = MagicMock()
        mock_token_map.get_replicas.return_value = [mock_replica]
        mock_cluster.metadata.token_map = mock_token_map
        
        with patch('mmh3.hash') as mock_mmh3:
            mock_mmh3.return_value = 0  # Empty string has a hash
            
            with patch('cassandra.metadata.Murmur3Token') as mock_token_class:
                mock_token_instance = MagicMock()
                mock_token_class.return_value = mock_token_instance
                
                with caplog.at_level(logging.INFO):
                    result = get_replica_nodes(mock_cluster, mock_session, "test_keyspace", empty_key)
        
        assert result == ["127.0.0.1"]
        mock_mmh3.assert_called_once_with(b'', signed=False)
