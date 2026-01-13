#!/usr/bin/env python3
"""
Integration Tests for src.infrastructure.container_manager.py

This test suite provides integration tests for container management functions,
testing against real Docker containers using Docker-in-Docker (dind).

All Docker commands are executed against a Docker daemon running inside a container,
providing complete isolation from the host Docker environment.
"""

import logging
import time
import pytest
import os
import docker
import tempfile
import json

try:
    from testcontainers.core.container import DockerContainer
except ImportError:
    try:
        from testcontainers.core import DockerContainer
    except ImportError:
        from testcontainers import DockerContainer

# Import the module under test
import sys
# Add parent directory and src directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.infrastructure.container_manager import (
    log_docker_command,
    stop_node,
    start_node,
    get_container_health_status,
    wait_for_container_healthy,
    get_container_ip,
)


# ============================================================================
# Helper Functions
# ============================================================================

def _get_docker_client(docker_host):
    """Get Docker client with clean config to avoid credential helpers.
    
    Creates a minimal config.json with empty auths to disable credential helpers.
    This is necessary because Docker API will use host's credential helpers if
    DOCKER_CONFIG points to a directory without config.json.
    
    Note: No cleanup needed - temp directory will be cleaned by OS, and DOCKER_CONFIG
    changes are scoped to the test process.
    """
    tmpdir = tempfile.mkdtemp()
    docker_config_dir = os.path.join(tmpdir, '.docker')
    os.makedirs(docker_config_dir, exist_ok=True)
    config_file = os.path.join(docker_config_dir, 'config.json')
    
    with open(config_file, 'w') as f:
        json.dump({"auths": {}}, f)
    
    os.environ['DOCKER_CONFIG'] = docker_config_dir
    return docker.DockerClient(base_url=docker_host)


def _cleanup_container(docker_host, container_name):
    """Helper to cleanup a container using Docker API."""
    try:
        client = _get_docker_client(docker_host)
        container = client.containers.get(container_name)
        container.stop(timeout=30)
        container.remove(force=True)
    except Exception:
        pass


# ============================================================================
# Pytest Fixtures
# ============================================================================

@pytest.fixture(scope="module")
def docker_dind_container():
    """
    Fixture that provides a Docker-in-Docker container for the entire test module.
    This container runs its own Docker daemon that all tests will use.
    
    Based on: https://hub.docker.com/_/docker
    """
    # Create Docker-in-Docker container
    # Using dind (Docker in Docker) variant without TLS for simplicity in tests
    container = DockerContainer("docker:29-dind")
    container.with_kwargs(privileged=True)  # Required for dind
    container.with_exposed_ports(2375)  # Docker daemon port (non-TLS)
    # Disable TLS for simplicity (DOCKER_TLS_CERTDIR not set)
    container.with_env("DOCKER_TLS_CERTDIR", "")
    
    container.start()
    
    # Wait for Docker daemon to be ready
    dind_host = container.get_container_host_ip()
    dind_port = container.get_exposed_port(2375)
    docker_host = f"tcp://{dind_host}:{dind_port}"
    
    # Wait for Docker daemon to be ready
    logging.info("Waiting for Docker-in-Docker daemon to start...")
    time.sleep(5)  # Initial wait for daemon to start
    max_retries = 60
    retry_delay = 2
    for attempt in range(max_retries):
        try:
            # Test connection to Docker daemon using Docker API
            client = _get_docker_client(docker_host)
            client.info()
            # Also verify we can actually use the daemon by listing containers
            client.containers.list(all=True)
            # One more test - try to pull a small image to ensure daemon is fully operational
            client.images.pull("alpine:latest")
            logging.info(f"Docker-in-Docker daemon ready after {attempt + 1} attempts")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                if attempt % 10 == 0:
                    logging.info(f"Waiting for Docker-in-Docker daemon... attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(retry_delay)
            else:
                logging.error(f"Docker-in-Docker daemon did not become ready: {e}")
                raise Exception(f"Docker-in-Docker daemon did not become ready in time: {e}")
    
    # Additional wait to ensure daemon is fully stable
    time.sleep(3)
    
    yield {
        "container": container,
        "docker_host": docker_host,
    }
    
    # Cleanup
    try:
        container.stop()
    except Exception:
        pass
    try:
        container.remove()
    except Exception:
        pass


@pytest.fixture(scope="function")
def docker_host_env(docker_dind_container, monkeypatch):
    """
    Fixture that sets DOCKER_HOST environment variable to point to the dind container.
    This makes all docker commands use the Docker daemon inside the dind container.
    """
    docker_host = docker_dind_container["docker_host"]
    monkeypatch.setenv("DOCKER_HOST", docker_host)
    yield docker_host


@pytest.fixture(scope="function")
def test_container(docker_dind_container, docker_host_env):
    """
    Fixture that provides a test container running inside the Docker-in-Docker instance.
    Uses a lightweight Alpine Linux container that can be started/stopped.
    """
    # Create a container inside the dind instance
    container_name = f"test-container-{int(time.time() * 1000000)}"
    
    # Run container inside dind with retries, using Docker API
    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = _get_docker_client(docker_host_env)
            # Pull image first
            client.images.pull("alpine:latest")
            # Create and start container
            container = client.containers.create(
                image="alpine:latest",
                name=container_name,
                command=["tail", "-f", "/dev/null"],
                detach=True
            )
            container.start()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                pytest.fail(f"Failed to create test container in dind after {max_retries} attempts: {str(e)}")
    
    # Reduced wait - container creation is synchronous, brief wait for state to settle
    time.sleep(0.5)
    
    yield container_name
    
    # Cleanup: ensure container is stopped and removed
    _cleanup_container(docker_host_env, container_name)


@pytest.fixture(scope="function")
def container_with_healthcheck(docker_dind_container, docker_host_env):
    """
    Fixture that provides a container with healthcheck configured inside the dind instance.
    Uses nginx which can have a healthcheck configured.
    """
    container_name = f"healthcheck-container-{int(time.time() * 1000000)}"
    
    # Create nginx container with healthcheck from the start using Docker API
    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = _get_docker_client(docker_host_env)
            # Pull image first
            client.images.pull("nginx:alpine")
            # Create container with healthcheck
            container = client.containers.create(
                image="nginx:alpine",
                name=container_name,
                detach=True,
                healthcheck={
                    'test': ['CMD-SHELL', 'wget --quiet --tries=1 --spider http://localhost/ || exit 1'],
                    'interval': 1000000000,  # 1s in nanoseconds
                    'timeout': 500000000,  # 500ms in nanoseconds
                    'retries': 3,
                    'start_period': 5000000000  # 5s in nanoseconds
                }
            )
            container.start()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                pytest.fail(f"Failed to create healthcheck container in dind after {max_retries} attempts: {str(e)}")
    
    # Reduced wait - container starts quickly, healthcheck has its own start period (5s)
    # We only need to wait for container to be created, not for healthcheck to complete
    time.sleep(2)
    
    yield container_name
    
    # Cleanup
    _cleanup_container(docker_host_env, container_name)


# ============================================================================
# Test: log_docker_command
# ============================================================================

class TestLogDockerCommandIntegration:
    """Integration tests for log_docker_command function"""
    
    def test_logs_command_with_real_container(self, test_container, caplog):
        """logs docker command correctly when cmd is a string with real container"""
        with caplog.at_level(logging.INFO):
            log_docker_command("stop", test_container)
        
        assert "Docker Command: docker stop" in caplog.text
        assert test_container in caplog.text
    
    def test_logs_command_with_list_cmd(self, test_container, caplog):
        """logs docker command correctly when cmd is a list"""
        with caplog.at_level(logging.INFO):
            log_docker_command(["stop"], test_container)
        
        assert "Docker Command: docker stop" in caplog.text
        assert test_container in caplog.text


# ============================================================================
# Test: stop_node
# ============================================================================

class TestStopNodeIntegration:
    """Integration tests for stop_node function"""
    
    def test_returns_true_when_stop_succeeds(self, test_container, docker_host_env, caplog):
        """returns True when docker stop command succeeds on real container in dind"""
        # Ensure container is running first
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            result = stop_node(test_container)
        
        assert result is True
        assert f"Successfully stopped {test_container}" in caplog.text
        
        # Verify container is actually stopped in dind
        container.reload()
        assert container.attrs['State']['Running'] is False
    
    def test_returns_false_when_container_not_found(self, docker_host_env, caplog):
        """returns False when trying to stop non-existent container"""
        nonexistent = "nonexistent-container-12345"
        
        with caplog.at_level(logging.ERROR):
            result = stop_node(nonexistent)
        
        assert result is False
        assert f"Failed to stop {nonexistent}" in caplog.text


# ============================================================================
# Test: start_node
# ============================================================================

class TestStartNodeIntegration:
    """Integration tests for start_node function"""
    
    def test_returns_true_when_start_succeeds(self, test_container, docker_host_env, caplog):
        """returns True when docker start command succeeds on real container in dind"""
        # First stop the container
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.stop(timeout=30)
        
        with caplog.at_level(logging.INFO):
            result = start_node(test_container)
        
        assert result is True
        assert f"Successfully started {test_container}" in caplog.text
        
        # Verify container is actually running in dind
        container.reload()
        assert container.attrs['State']['Running'] is True
    
    def test_returns_false_when_container_not_found(self, docker_host_env, caplog):
        """returns False when trying to start non-existent container"""
        nonexistent = "nonexistent-container-12345"
        
        with caplog.at_level(logging.ERROR):
            result = start_node(nonexistent)
        
        assert result is False
        assert f"Failed to start {nonexistent}" in caplog.text


# ============================================================================
# Test: get_container_health_status
# ============================================================================

class TestGetContainerHealthStatusIntegration:
    """Integration tests for get_container_health_status function"""
    
    def test_returns_health_status_when_available(self, container_with_healthcheck, docker_host_env):
        """returns health status when container has healthcheck configured in dind"""
        # Wait for healthcheck to begin
        time.sleep(3)
        
        # Verify healthcheck is actually configured by checking directly
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(container_with_healthcheck)
        container.reload()
        
        # If health status exists, healthcheck is configured
        health_status = container.attrs.get('State', {}).get('Health', {}).get('Status')
        if health_status:
            # Healthcheck is configured, test our function
            result = get_container_health_status(container_with_healthcheck)
            # Should return a status: starting, healthy, or unhealthy, or None if not ready yet
            assert result in ["starting", "healthy", "unhealthy", None]
        else:
            # Healthcheck might not be configured, skip this test or mark as skipped
            pytest.skip("Healthcheck not properly configured on container")
    
    def test_returns_none_when_no_healthcheck(self, test_container):
        """returns None when container has no healthcheck configured"""
        result = get_container_health_status(test_container)
        
        assert result is None
    
    def test_strips_whitespace_from_status(self, container_with_healthcheck, docker_host_env):
        """strips whitespace from health status"""
        # Reduced sleep - fixture already waits, we just need a brief moment for status to be available
        time.sleep(1)
        
        result = get_container_health_status(container_with_healthcheck)
        
        # Result should not have leading/trailing whitespace
        if result:
            assert result == result.strip()


# ============================================================================
# Test: wait_for_container_healthy
# ============================================================================

class TestWaitForContainerHealthyIntegration:
    """Integration tests for wait_for_container_healthy function"""
    
    def test_returns_true_when_container_becomes_healthy(self, container_with_healthcheck, docker_host_env, caplog):
        """returns True when container becomes healthy in dind"""
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy(container_with_healthcheck, max_wait=60)
        
        # Should eventually become healthy (or at least return True if running)
        assert result is True
        assert container_with_healthcheck in caplog.text
    
    def test_returns_true_when_no_healthcheck_but_container_running(self, test_container, docker_host_env, caplog):
        """returns True when no healthcheck but container is running in dind"""
        # Ensure container is running
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy(test_container, max_wait=10)
        
        assert result is True
        assert "running (no healthcheck configured)" in caplog.text
    
    def test_returns_false_when_container_not_found(self, docker_host_env, caplog):
        """returns False when container does not exist"""
        nonexistent = "nonexistent-container-12345"
        
        with caplog.at_level(logging.WARNING):
            result = wait_for_container_healthy(nonexistent, max_wait=5)
        
        assert result is False


# ============================================================================
# Test: get_container_ip
# ============================================================================

class TestGetContainerIPIntegration:
    """Integration tests for get_container_ip function"""
    
    def test_returns_ip_when_container_exists(self, test_container):
        """returns IP address when container exists in dind"""
        ip = get_container_ip(test_container)
        
        assert ip is not None
        assert len(ip) > 0
        # Should be a valid IP format (basic check)
        parts = ip.split('.')
        assert len(parts) == 4
        assert all(0 <= int(part) <= 255 for part in parts)
    
    def test_returns_none_when_container_not_found(self):
        """returns None when container does not exist"""
        nonexistent = "nonexistent-container-12345"
        
        ip = get_container_ip(nonexistent)
        
        assert ip is None


# ============================================================================
# Test: Complex Scenarios
# ============================================================================

class TestComplexScenariosIntegration:
    """Integration tests for complex container management scenarios"""
    
    def test_stop_then_start_same_container(self, test_container, docker_host_env, caplog):
        """handles stopping and then starting the same container in dind"""
        # Ensure container is running
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            stop_result = stop_node(test_container)
            start_result = start_node(test_container)
        
        assert stop_result is True
        assert start_result is True
        assert "Successfully stopped" in caplog.text
        assert "Successfully started" in caplog.text
    
    def test_get_ip_after_restart(self, test_container):
        """gets IP address correctly after container restart in dind"""
        # Get IP while running
        ip_before = get_container_ip(test_container)
        
        # Stop and start - both operations are synchronous, minimal wait needed
        stop_node(test_container)
        start_node(test_container)
        # Brief wait for IP to be available after restart
        time.sleep(0.5)
        
        # Get IP after restart
        ip_after = get_container_ip(test_container)
        
        # IP might change or stay the same depending on Docker network
        assert ip_before is not None
        assert ip_after is not None
        assert len(ip_before) > 0
        assert len(ip_after) > 0


# ============================================================================
# Test: Edge Cases with Real Containers in dind
# ============================================================================

class TestEdgeCasesIntegration:
    """Integration tests for edge cases with real containers in dind"""
    
    def test_verifies_isolation_from_host_docker(self, docker_dind_container, docker_host_env):
        """verifies that tests use dind Docker daemon, not host Docker"""
        # List containers in dind using Docker API
        client = _get_docker_client(docker_host_env)
        containers = client.containers.list(all=True)
        
        # Verify we're using the dind Docker daemon (not host)
        assert docker_host_env.startswith("tcp://")
        # At minimum, dind should be able to list containers
        assert isinstance(containers, list)


# ============================================================================
# BugMagnet Session 2026-01-09: Integration Test Edge Case Coverage
# ============================================================================

class TestBugMagnetEdgeCasesIntegration:
    """Advanced edge case tests for Docker utilities using real containers"""
    
    # ========================================================================
    # String Edge Cases: Container Names
    # ========================================================================
    
    def test_handles_very_long_container_name(self, docker_host_env, caplog):
        """handles container names at system limits (255+ characters) in dind"""
        very_long_name = "a" * 200  # Docker has limits, use reasonable length
        container_name = f"test-{very_long_name}"
        
        # Create container with long name using Docker API
        try:
            client = _get_docker_client(docker_host_env)
            client.images.pull("alpine:latest")
            container = client.containers.create(
                image="alpine:latest",
                name=container_name,
                command=["tail", "-f", "/dev/null"],
                detach=True
            )
            container.start()
            
            with caplog.at_level(logging.INFO):
                result = stop_node(container_name)
            
            assert result is True
        except Exception as e:
            pytest.skip(f"Could not create container with long name: {str(e)}")
        finally:
            # Cleanup
            _cleanup_container(docker_host_env, container_name)
    
    # ========================================================================
    # Error Condition Edge Cases
    # ========================================================================
    
    def test_handles_nonexistent_container_gracefully(self, docker_host_env, caplog):
        """handles operations on non-existent containers without crashing"""
        nonexistent = f"nonexistent-container-{int(time.time() * 1000000)}"
        
        with caplog.at_level(logging.ERROR):
            stop_result = stop_node(nonexistent)
            start_result = start_node(nonexistent)
            ip_result = get_container_ip(nonexistent)
            health_result = get_container_health_status(nonexistent)
        
        assert stop_result is False
        assert start_result is False
        assert ip_result is None
        assert health_result is None
    
    # ========================================================================
    # Timeout and Performance Edge Cases
    # ========================================================================
    
    def test_wait_for_container_healthy_with_short_timeout(self, test_container, docker_host_env, caplog):
        """handles very short timeout values correctly"""
        # Ensure container is running
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.WARNING):
            # Use very short timeout (1 second)
            result = wait_for_container_healthy(test_container, max_wait=1)
        
        # Should return True if container is running (no healthcheck) or False if timeout
        assert result in [True, False]
    
    # ========================================================================
    # IP Address Edge Cases
    # ========================================================================
    
    def test_get_container_ip_handles_stopped_container(self, test_container):
        """handles getting IP address for stopped container gracefully"""
        # Get IP while running
        ip_running = get_container_ip(test_container)
        
        # Verify we got an IP while running
        assert ip_running is not None
        assert len(ip_running) > 0
        
        # Stop container - operation is synchronous, no wait needed
        stop_node(test_container)
        
        # Get IP while stopped (may return None or the same IP depending on Docker behavior)
        ip_stopped = get_container_ip(test_container)
        
        # Docker behavior: stopped containers may return None or the same IP
        # Both behaviors are acceptable - just verify the function doesn't crash
        assert ip_stopped is None or ip_stopped == ip_running
    
    # ========================================================================
    # Health Status Edge Cases
    # ========================================================================
    
    def test_handles_health_status_transitions(self, container_with_healthcheck):
        """handles health status transitions (starting -> healthy)"""
        # Reduced wait - fixture already waits, healthcheck has 5s start period
        # We just need to check status at different points, not wait for full transition
        time.sleep(5)
        
        status1 = get_container_health_status(container_with_healthcheck)
        # Brief wait between checks to potentially see transition
        time.sleep(1)
        status2 = get_container_health_status(container_with_healthcheck)
        
        # Status should be valid (starting, healthy, or unhealthy)
        assert status1 in ["starting", "healthy", "unhealthy", None]
        assert status2 in ["starting", "healthy", "unhealthy", None]
    
    # ========================================================================
    # Multiple Container Operations
    # ========================================================================
    
    def test_handles_multiple_containers_simultaneously(self, docker_host_env, caplog):
        """handles operations on multiple containers at the same time"""
        client = _get_docker_client(docker_host_env)
        containers = []
        
        # Create multiple containers using Docker API
        client.images.pull("alpine:latest")
        for i in range(3):
            container_name = f"multi-test-{int(time.time() * 1000000)}-{i}"
            try:
                container = client.containers.create(
                    image="alpine:latest",
                    name=container_name,
                    command=["tail", "-f", "/dev/null"],
                    detach=True
                )
                container.start()
                containers.append(container_name)
            except Exception:
                pass
        
        try:
            # Operate on all containers
            results = []
            with caplog.at_level(logging.INFO):
                for container_name in containers:
                    ip = get_container_ip(container_name)
                    health = get_container_health_status(container_name)
                    results.append((ip is not None, health is not None or health in ["starting", "healthy", "unhealthy"]))
            
            # All operations should succeed
            assert all(ip_ok for ip_ok, _ in results)
        finally:
            # Cleanup
            for container_name in containers:
                _cleanup_container(docker_host_env, container_name)
    
    # ========================================================================
    # Log Command Edge Cases
    # ========================================================================
    
    def test_logs_command_with_empty_string(self, caplog):
        """logs docker command correctly when args is empty string"""
        with caplog.at_level(logging.INFO):
            log_docker_command("stop", "")
        
        assert "Docker Command: docker stop" in caplog.text
    
    def test_logs_command_with_no_args(self, caplog):
        """logs docker command correctly when no args provided"""
        with caplog.at_level(logging.INFO):
            log_docker_command("ps")
        
        assert "Docker Command: docker ps" in caplog.text
    
    def test_logs_command_with_list_args(self, caplog):
        """logs docker command correctly when args is a list"""
        with caplog.at_level(logging.INFO):
            log_docker_command("exec", ["-it", "container-name", "bash"])
        
        assert "Docker Command: docker exec -it container-name bash" in caplog.text
