#!/usr/bin/env python3
"""
Tests for src.infrastructure.container_manager.py

This test suite provides comprehensive coverage for container management functions,
including container operations, health checks, and edge cases.
"""

import logging
import subprocess
import time
from unittest.mock import Mock, patch, MagicMock
import pytest

# Import the module under test
import sys
import os
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.infrastructure.container_manager import (
    log_docker_command,
    stop_node,
    start_node,
    get_container_health_status,
    wait_for_container_healthy,
    get_container_ip,
    map_replica_node_to_container,
)


# ============================================================================
# Test: log_docker_command
# ============================================================================

class TestLogDockerCommand:
    """Tests for log_docker_command function"""
    
    def test_logs_command_with_string_cmd(self, caplog):
        """logs docker command correctly when cmd is a string"""
        with caplog.at_level(logging.INFO):
            log_docker_command("stop", "container-name")
        
        assert "Docker Command: docker stop container-name" in caplog.text
    
    def test_logs_command_with_list_cmd(self, caplog):
        """logs docker command correctly when cmd is a list"""
        with caplog.at_level(logging.INFO):
            log_docker_command(["stop"], "container-name")
        
        assert "Docker Command: docker stop container-name" in caplog.text
    
    def test_logs_command_with_list_args(self, caplog):
        """logs docker command correctly when args is a list"""
        with caplog.at_level(logging.INFO):
            log_docker_command("exec", ["-it", "container-name", "bash"])
        
        assert "Docker Command: docker exec -it container-name bash" in caplog.text
    
    def test_logs_command_with_no_args(self, caplog):
        """logs docker command correctly when no args provided"""
        with caplog.at_level(logging.INFO):
            log_docker_command("ps")
        
        assert "Docker Command: docker ps" in caplog.text
    
    def test_logs_command_with_empty_string_args(self, caplog):
        """logs docker command correctly when args is empty string"""
        with caplog.at_level(logging.INFO):
            log_docker_command("stop", "")
        
        # Empty string is falsy, so it won't be appended
        assert "Docker Command: docker stop" in caplog.text
    
    def test_logs_command_with_special_characters_in_args(self, caplog):
        """logs docker command correctly when args contain special characters"""
        with caplog.at_level(logging.INFO):
            # log_docker_command only takes 2 args: cmd and args
            log_docker_command("exec", ["container-name", "echo", "'hello world'"])
        
        assert "Docker Command: docker exec container-name echo 'hello world'" in caplog.text


# ============================================================================
# Test: stop_node
# ============================================================================

class TestStopNode:
    """Tests for stop_node function"""
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_returns_true_when_stop_succeeds(self, mock_log, mock_run, caplog):
        """returns True when docker stop command succeeds"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        with caplog.at_level(logging.INFO):
            result = stop_node("test-container")
        
        assert result is True
        assert "Successfully stopped test-container" in caplog.text
        mock_run.assert_called_once()
        mock_log.assert_called_once_with("stop", "test-container")
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_returns_false_when_stop_fails(self, mock_log, mock_run, caplog):
        """returns False when docker stop command fails"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error: container not found"
        mock_run.return_value = mock_result
        
        with caplog.at_level(logging.ERROR):
            result = stop_node("nonexistent-container")
        
        assert result is False
        assert "Failed to stop nonexistent-container" in caplog.text
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_handles_subprocess_exception(self, mock_log, mock_run, caplog):
        """handles subprocess exceptions gracefully"""
        mock_run.side_effect = subprocess.TimeoutExpired("docker", 30)
        
        with caplog.at_level(logging.ERROR):
            result = stop_node("test-container")
        
        assert result is False
        assert "Error stopping container test-container" in caplog.text
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_calls_docker_stop_with_correct_args(self, mock_log, mock_run):
        """calls docker stop with correct arguments"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        stop_node("my-container")
        
        mock_run.assert_called_once_with(
            ["docker", "stop", "my-container"],
            capture_output=True,
            text=True,
            timeout=30
        )


# ============================================================================
# Test: start_node
# ============================================================================

class TestStartNode:
    """Tests for start_node function"""
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_returns_true_when_start_succeeds(self, mock_log, mock_run, caplog):
        """returns True when docker start command succeeds"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        with caplog.at_level(logging.INFO):
            result = start_node("test-container")
        
        assert result is True
        assert "Successfully started test-container" in caplog.text
        mock_run.assert_called_once()
        mock_log.assert_called_once_with("start", "test-container")
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_returns_false_when_start_fails(self, mock_log, mock_run, caplog):
        """returns False when docker start command fails"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "Error: container not found"
        mock_run.return_value = mock_result
        
        with caplog.at_level(logging.ERROR):
            result = start_node("nonexistent-container")
        
        assert result is False
        assert "Failed to start nonexistent-container" in caplog.text
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_handles_subprocess_exception(self, mock_log, mock_run, caplog):
        """handles subprocess exceptions gracefully"""
        mock_run.side_effect = subprocess.TimeoutExpired("docker", 30)
        
        with caplog.at_level(logging.ERROR):
            result = start_node("test-container")
        
        assert result is False
        assert "Error starting container test-container" in caplog.text
    
    @patch('subprocess.run')
    @patch('src.infrastructure.container_manager.log_docker_command')
    def test_calls_docker_start_with_correct_args(self, mock_log, mock_run):
        """calls docker start with correct arguments"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stderr = ""
        mock_run.return_value = mock_result
        
        start_node("my-container")
        
        mock_run.assert_called_once_with(
            ["docker", "start", "my-container"],
            capture_output=True,
            text=True,
            timeout=30
        )


# ============================================================================
# Test: get_container_health_status
# ============================================================================

class TestGetContainerHealthStatus:
    """Tests for get_container_health_status function"""
    
    @patch('subprocess.run')
    def test_returns_health_status_when_available(self, mock_run):
        """returns health status when container has healthcheck configured"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "healthy\n"
        mock_run.return_value = mock_result
        
        result = get_container_health_status("test-container")
        
        assert result == "healthy"
        mock_run.assert_called_once()
    
    @patch('subprocess.run')
    def test_returns_none_when_no_healthcheck(self, mock_run):
        """returns None when container has no healthcheck configured"""
        mock_result = MagicMock()
        mock_result.returncode = 1  # Command fails when no healthcheck
        mock_result.stderr = "No healthcheck configured"
        mock_run.return_value = mock_result
        
        result = get_container_health_status("test-container")
        
        assert result is None
    
    @patch('subprocess.run')
    def test_handles_exception_gracefully(self, mock_run):
        """handles exceptions gracefully and returns None"""
        mock_run.side_effect = Exception("Connection error")
        
        result = get_container_health_status("test-container")
        
        assert result is None
    
    @patch('subprocess.run')
    def test_strips_whitespace_from_status(self, mock_run):
        """strips whitespace from health status"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "  healthy  \n"
        mock_run.return_value = mock_result
        
        result = get_container_health_status("test-container")
        
        assert result == "healthy"
    
    @patch('subprocess.run')
    def test_returns_unhealthy_status(self, mock_run):
        """returns unhealthy status correctly"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "unhealthy\n"
        mock_run.return_value = mock_result
        
        result = get_container_health_status("test-container")
        
        assert result == "unhealthy"
    
    @patch('subprocess.run')
    def test_returns_starting_status(self, mock_run):
        """returns starting status correctly"""
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "starting\n"
        mock_run.return_value = mock_result
        
        result = get_container_health_status("test-container")
        
        assert result == "starting"


# ============================================================================
# Test: wait_for_container_healthy
# ============================================================================

class TestWaitForContainerHealthy:
    """Tests for wait_for_container_healthy function"""
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    def test_returns_true_when_container_becomes_healthy(self, mock_get_status, mock_time, mock_sleep, caplog):
        """returns True when container becomes healthy"""
        # First call returns "starting", second returns "healthy"
        mock_get_status.side_effect = ["starting", "healthy"]
        
        # Mock time: start at 0, then 1 for all subsequent calls (logging may call time.time() multiple times)
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 1
        
        mock_time.side_effect = time_side_effect
        
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is True
        assert "Container test-container is healthy" in caplog.text
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    def test_returns_false_when_timeout_exceeded(self, mock_get_status, mock_time, mock_sleep, caplog):
        """returns False when container does not become healthy within timeout"""
        mock_get_status.return_value = "starting"
        
        # Mock time: start at 0, then exceed timeout
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 181
        
        mock_time.side_effect = time_side_effect
        
        with caplog.at_level(logging.WARNING):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is False
        assert "did not become healthy within 180 seconds" in caplog.text
    
    @patch('src.infrastructure.container_manager.time.sleep')
    @patch('src.infrastructure.container_manager.time.time')
    @patch('src.infrastructure.container_manager.get_container_health_status')
    @patch('subprocess.run')
    def test_returns_true_when_no_healthcheck_but_container_running(self, mock_run, mock_get_status, mock_time, mock_sleep, caplog):
        """returns True when no healthcheck but container is running"""
        mock_get_status.return_value = None  # No healthcheck
        
        # Mock time: start at 0, then 1
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 1
        
        mock_time.side_effect = time_side_effect
        
        # Mock container running check
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "true\n"
        mock_run.return_value = mock_result
        
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy("test-container", max_wait=180)
        
        assert result is True
        assert "Container test-container is running (no healthcheck configured)" in caplog.text
    
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
    @patch('subprocess.run')
    def test_handles_exception_when_checking_running_status(self, mock_run, mock_get_status, mock_time, mock_sleep, caplog):
        """handles exception when checking if container is running"""
        mock_get_status.return_value = None  # No healthcheck
        
        # Mock time: start at 0, then timeout
        call_count = [0]
        def time_side_effect():
            call_count[0] += 1
            return 0 if call_count[0] == 1 else 181
        
        mock_time.side_effect = time_side_effect
        
        # Mock exception when checking running status
        mock_run.side_effect = Exception("Connection error")
        
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
        with patch('subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            
            with caplog.at_level(logging.INFO):
                result = stop_node(very_long_name)
            
            assert result is True
            # Verify the long name was used in the command
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert very_long_name in call_args
    
    def test_handles_container_name_with_special_characters(self, caplog):
        """handles container names with special characters that might break shell commands"""
        special_chars = "test-container_123.456"
        with patch('subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            
            with caplog.at_level(logging.INFO):
                result = stop_node(special_chars)
            
            assert result is True
            # Verify subprocess.run was called (it handles special chars safely)
            mock_run.assert_called_once()
    
    def test_handles_container_name_with_unicode(self, caplog):
        """handles container names with unicode characters"""
        unicode_name = "cassandra-node-测试"
        with patch('subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            
            with caplog.at_level(logging.INFO):
                result = stop_node(unicode_name)
            
            assert result is True
            mock_run.assert_called_once()
    
    # ========================================================================
    # Error Condition Edge Cases
    # ========================================================================
    
    def test_handles_subprocess_timeout_exception(self, caplog):
        """handles subprocess timeout exceptions specifically"""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("docker", 30)
            
            with caplog.at_level(logging.ERROR):
                result = stop_node("test-container")
            
            assert result is False
            assert "Error stopping container test-container" in caplog.text
    
    def test_handles_subprocess_called_process_error(self, caplog):
        """handles subprocess CalledProcessError exceptions"""
        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "docker", "Error")
            
            with caplog.at_level(logging.ERROR):
                result = stop_node("test-container")
            
            assert result is False
            assert "Error stopping container test-container" in caplog.text
    
    # ========================================================================
    # Complex Interactions
    # ========================================================================
    
    def test_stop_then_start_same_container(self, caplog):
        """handles stopping and then starting the same container"""
        container_name = "test-container"
        
        with patch('subprocess.run') as mock_run:
            # First call (stop) succeeds
            mock_result_stop = MagicMock()
            mock_result_stop.returncode = 0
            mock_result_stop.stderr = ""
            
            # Second call (start) succeeds
            mock_result_start = MagicMock()
            mock_result_start.returncode = 0
            mock_result_start.stderr = ""
            
            mock_run.side_effect = [mock_result_stop, mock_result_start]
            
            with caplog.at_level(logging.INFO):
                stop_result = stop_node(container_name)
                start_result = start_node(container_name)
            
            assert stop_result is True
            assert start_result is True
            assert "Successfully stopped" in caplog.text
            assert "Successfully started" in caplog.text
            assert mock_run.call_count == 2
    
    def test_multiple_container_operations_in_sequence(self, caplog):
        """handles multiple container operations on different containers"""
        containers = ["container1", "container2", "container3"]
        
        with patch('subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stderr = ""
            mock_run.return_value = mock_result
            
            results = []
            for container in containers:
                with caplog.at_level(logging.INFO):
                    results.append(stop_node(container))
            
            assert all(results)
            assert mock_run.call_count == 3
    
    # ========================================================================
    # Docker Command Edge Cases
    # ========================================================================
    
    def test_logs_docker_command_with_very_long_args_list(self, caplog):
        """handles docker commands with very long argument lists"""
        long_args = ["arg" + str(i) for i in range(100)]
        
        with caplog.at_level(logging.INFO):
            log_docker_command("run", long_args)
        
        assert "Docker Command:" in caplog.text
        # Should log the command even with many args
    
    # ========================================================================
    # Health Status Edge Cases
    # ========================================================================
    
    def test_handles_health_status_with_extra_whitespace_variations(self, caplog):
        """handles health status with various whitespace patterns"""
        whitespace_variations = [
            "\thealthy\t",
            "\nhealthy\n",
            "\r\nhealthy\r\n",
            "  healthy  ",
            "\t\n\r healthy \r\n\t"
        ]
        
        for status_output in whitespace_variations:
            with patch('subprocess.run') as mock_run:
                mock_result = MagicMock()
                mock_result.returncode = 0
                mock_result.stdout = status_output
                mock_run.return_value = mock_result
                
                result = get_container_health_status("test-container")
                
                # Should strip all whitespace and return "healthy"
                assert result == "healthy"

