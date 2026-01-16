#!/usr/bin/env python3
"""
Unit Tests for src.infrastructure.container_manager.py

This test suite provides comprehensive unit test coverage for container management functions,
using mocks to test container operations, health checks, and edge cases without requiring
a real Docker instance.

For integration tests against real Docker containers, see:
- test_container_manager_integration.py
"""

import logging
from unittest.mock import patch, MagicMock
import docker

# Import the module under test
import sys
import os
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.infrastructure.container_manager import (
    stop_node,
    start_node,
    get_container_health_status,
    wait_for_container_healthy,
)


# ============================================================================
# Test: stop_node
# ============================================================================

class TestStopNode:
    """Tests for stop_node function
    
    Note: Basic success/failure scenarios are tested in integration tests.
    These unit tests focus on error handling and edge cases.
    """
    
    @patch('docker.from_env')
    def test_handles_docker_api_exception(self, mock_docker, caplog):
        """handles Docker API exceptions gracefully"""
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = Exception("Connection error")
        
        with caplog.at_level(logging.ERROR):
            result = stop_node("test-container")
        
        assert result is False
        assert "Error stopping container test-container" in caplog.text
    
    @patch('docker.from_env')
    def test_handles_container_not_found(self, mock_docker, caplog):
        """handles container not found errors"""
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = docker.errors.NotFound("Container not found")
        
        with caplog.at_level(logging.ERROR):
            result = stop_node("test-container")
        
        assert result is False
        assert "Failed to stop test-container" in caplog.text
    
    @patch('docker.from_env')
    def test_calls_docker_api_stop_with_correct_timeout(self, mock_docker):
        """calls Docker API stop with correct timeout"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        
        stop_node("my-container")
        
        mock_client.containers.get.assert_called_once_with("my-container")
        mock_container.stop.assert_called_once_with(timeout=30)


# ============================================================================
# Test: start_node
# ============================================================================

class TestStartNode:
    """Tests for start_node function
    
    Note: Basic success/failure scenarios are tested in integration tests.
    These unit tests focus on error handling and edge cases.
    """
    
    @patch('docker.from_env')
    def test_handles_docker_api_exception(self, mock_docker, caplog):
        """handles Docker API exceptions gracefully"""
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = Exception("Connection error")
        
        with caplog.at_level(logging.ERROR):
            result = start_node("test-container")
        
        assert result is False
        assert "Error starting container test-container" in caplog.text
    
    @patch('docker.from_env')
    def test_handles_container_not_found(self, mock_docker, caplog):
        """handles container not found errors"""
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = docker.errors.NotFound("Container not found")
        
        with caplog.at_level(logging.ERROR):
            result = start_node("test-container")
        
        assert result is False
        assert "Failed to start test-container" in caplog.text
    
    @patch('docker.from_env')
    def test_calls_docker_api_start(self, mock_docker):
        """calls Docker API start method"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        
        start_node("my-container")
        
        mock_client.containers.get.assert_called_once_with("my-container")
        mock_container.start.assert_called_once()


# ============================================================================
# Test: get_container_health_status
# ============================================================================

class TestGetContainerHealthStatus:
    """Tests for get_container_health_status function
    
    Note: Basic functionality is tested in integration tests.
    These unit tests focus on edge cases and error conditions.
    """
    
    @patch('docker.from_env')
    def test_handles_exception_gracefully(self, mock_docker):
        """handles exceptions gracefully and returns None"""
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = Exception("Connection error")
        
        result = get_container_health_status("test-container")
        
        assert result is None
    
    @patch('docker.from_env')
    def test_returns_healthy_status(self, mock_docker):
        """returns healthy status correctly"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        mock_container.attrs = {'State': {'Health': {'Status': 'healthy'}}}
        
        result = get_container_health_status("test-container")
        
        assert result == "healthy"
    
    @patch('docker.from_env')
    def test_returns_unhealthy_status(self, mock_docker):
        """returns unhealthy status correctly"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        mock_container.attrs = {'State': {'Health': {'Status': 'unhealthy'}}}
        
        result = get_container_health_status("test-container")
        
        assert result == "unhealthy"
    
    @patch('docker.from_env')
    def test_returns_starting_status(self, mock_docker):
        """returns starting status correctly"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        mock_container.attrs = {'State': {'Health': {'Status': 'starting'}}}
        
        result = get_container_health_status("test-container")
        
        assert result == "starting"
    
    @patch('docker.from_env')
    def test_returns_none_when_no_health_section(self, mock_docker):
        """returns None when container has no health section"""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.return_value = mock_container
        mock_container.attrs = {'State': {}}
        
        result = get_container_health_status("test-container")
        
        assert result is None


# ============================================================================
# Test: wait_for_container_healthy
# ============================================================================

class TestWaitForContainerHealthy:
    """Tests for wait_for_container_healthy function
    
    Note: Basic functionality is tested in integration tests.
    These unit tests focus on timeout scenarios and error conditions.
    """
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    def test_continues_waiting_when_unhealthy(self, mock_get_status, mock_time, mock_sleep, caplog):
        """continues waiting when container is unhealthy, hoping it recovers"""
        # First unhealthy, then healthy
        mock_get_status.side_effect = ["unhealthy", "healthy"]
        
        # Mock time: start at 0, then 1
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 1
        
        mock_time.side_effect = time_side_effect
        
        with caplog.at_level(logging.WARNING):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is True
        assert "Container test-container is unhealthy" in caplog.text
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    def test_logs_starting_status(self, mock_get_status, mock_time, mock_sleep, caplog):
        """logs starting status during wait"""
        mock_get_status.side_effect = ["starting", "healthy"]
        
        # Mock time: start at 0, then 1
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 1
        
        mock_time.side_effect = time_side_effect
        
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is True
        assert "healthcheck is starting" in caplog.text
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    @patch('docker.from_env')
    def test_handles_exception_when_checking_running_status(self, mock_docker, mock_get_status, mock_time, mock_sleep, caplog):
        """handles exception when checking if container is running"""
        mock_get_status.return_value = None  # No healthcheck
        
        # Mock time: start at 0, then timeout
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 181
        
        mock_time.side_effect = time_side_effect
        
        # Mock exception when checking running status
        mock_client = MagicMock()
        mock_docker.return_value = mock_client
        mock_client.containers.get.side_effect = Exception("Connection error")
        
        with caplog.at_level(logging.WARNING):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is False


# ============================================================================
# BugMagnet Session 2026-01-09: Docker Edge Case Coverage
# ============================================================================

class TestDockerEdgeCases:
    """Advanced edge case tests for Docker utilities"""
    
    # ========================================================================
    # String Edge Cases: Container Names
    # ========================================================================
    
    def test_handles_very_long_container_name(self, caplog):
        """handles container names at system limits (255+ characters)"""
        very_long_name = "a" * 255
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_container = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.return_value = mock_container
            
            with caplog.at_level(logging.INFO):
                result = stop_node(very_long_name)
            
            assert result is True
            # Verify the long name was used in the Docker API call
            mock_client.containers.get.assert_called_once_with(very_long_name)
    
    def test_handles_container_name_with_special_characters(self, caplog):
        """handles container names with special characters that might break shell commands"""
        special_chars = "test-container_123.456"
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_container = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.return_value = mock_container
            
            with caplog.at_level(logging.INFO):
                result = stop_node(special_chars)
            
            assert result is True
            # Verify Docker API was called (it handles special chars safely)
            mock_client.containers.get.assert_called_once_with(special_chars)
    
    def test_handles_container_name_with_unicode(self, caplog):
        """handles container names with unicode characters"""
        unicode_name = "cassandra-node-测试"
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_container = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.return_value = mock_container
            
            with caplog.at_level(logging.INFO):
                result = stop_node(unicode_name)
            
            assert result is True
            mock_client.containers.get.assert_called_once_with(unicode_name)
    
    # ========================================================================
    # Error Condition Edge Cases
    # ========================================================================
    
    def test_handles_docker_api_timeout_exception(self, caplog):
        """handles Docker API timeout exceptions specifically"""
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.side_effect = Exception("Timeout occurred")
            
            with caplog.at_level(logging.ERROR):
                result = stop_node("test-container")
            
            assert result is False
            assert "Error stopping container test-container" in caplog.text
    
    def test_handles_docker_api_error(self, caplog):
        """handles Docker API errors"""
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.side_effect = docker.errors.APIError("API Error")
            
            with caplog.at_level(logging.ERROR):
                result = stop_node("test-container")
            
            assert result is False
            assert "Error stopping container test-container" in caplog.text
    
    # ========================================================================
    # Complex Interactions
    # ========================================================================
    
    # Note: stop_then_start_same_container is tested in integration tests
    
    def test_multiple_container_operations_in_sequence(self, caplog):
        """handles multiple container operations on different containers"""
        containers = ["container1", "container2", "container3"]
        
        with patch('docker.from_env') as mock_docker:
            mock_client = MagicMock()
            mock_container = MagicMock()
            mock_docker.return_value = mock_client
            mock_client.containers.get.return_value = mock_container
            
            results = []
            for container in containers:
                with caplog.at_level(logging.INFO):
                    results.append(stop_node(container))
            
            assert all(results)
            assert mock_client.containers.get.call_count == 3
    
    # ========================================================================
    # Health Status Edge Cases
    # ========================================================================
    
    def test_handles_health_status_from_docker_api(self, caplog):
        """handles health status from Docker API correctly"""
        # Docker API returns clean status strings without whitespace
        status_values = ["healthy", "unhealthy", "starting"]
        
        for status in status_values:
            with patch('docker.from_env') as mock_docker:
                mock_client = MagicMock()
                mock_container = MagicMock()
                mock_docker.return_value = mock_client
                mock_client.containers.get.return_value = mock_container
                mock_container.attrs = {'State': {'Health': {'Status': status}}}
                
                result = get_container_health_status("test-container")
                
                # Should return the status as-is from Docker API
                assert result == status

