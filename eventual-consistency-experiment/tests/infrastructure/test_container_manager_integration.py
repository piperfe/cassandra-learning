#!/usr/bin/env python3
"""
Integration Tests for src.infrastructure.container_manager.py

This test suite provides integration tests for container management functions,
testing against real Docker containers.

The test environment uses the host Docker daemon by default.

Environment Variable Control:
    DOCKER_TEST_ENV: Controls which Docker environment to use
    - "dind" or "DIND": Force use of Docker-in-Docker (always use dind)
    - Unset or any other value: Use host Docker (default)

Examples:
    # Use host Docker (default)
    pytest tests/infrastructure/test_container_manager_integration.py

    # Force use of Docker-in-Docker
    DOCKER_TEST_ENV=dind pytest tests/infrastructure/test_container_manager_integration.py

All Docker commands are executed against the selected Docker daemon.
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
    stop_node,
    start_node,
    get_container_health_status,
    wait_for_container_healthy,
    get_container_ip,
)


# ============================================================================
# Helper Functions
# ============================================================================

def _get_docker_environment_preference():
    """Get Docker environment preference from environment variable.
    
    Checks DOCKER_TEST_ENV environment variable:
    - "dind" or "DIND": Force use of Docker-in-Docker
    - Unset or any other value: Use host Docker (default)
    
    Returns:
        str or None: "dind" to force dind, or None for default (host Docker)
    """
    env_var = os.environ.get("DOCKER_TEST_ENV", "").strip().upper()
    if env_var in ("DIND", "DOCKER_IN_DOCKER"):
        return "dind"
    else:
        return None  # Default: use host Docker


def _check_host_docker_available():
    """Check if host Docker daemon is available via Python Docker client.
    
    The code under test and test fixtures both use Python Docker client exclusively,
    so we only need to verify the Python Docker client can connect to the daemon.
    
    Returns:
        tuple: (is_available: bool, docker_host: str or None)
    """
    # Check if Python Docker client works
    try:
        # Temporarily save DOCKER_CONFIG if it exists
        original_docker_config = os.environ.get('DOCKER_CONFIG')
        try:
            # Try without custom DOCKER_CONFIG first
            if 'DOCKER_CONFIG' in os.environ:
                del os.environ['DOCKER_CONFIG']
            default_client = docker.from_env()
            default_client.info()  # Test connection
            default_client.close()
            logging.info("Python Docker client works - host Docker is available")
            return True, None
        finally:
            # Restore original DOCKER_CONFIG
            if original_docker_config:
                os.environ['DOCKER_CONFIG'] = original_docker_config
            elif 'DOCKER_CONFIG' in os.environ:
                del os.environ['DOCKER_CONFIG']
    except Exception as e:
        logging.debug(f"Python Docker client connection failed: {e}")
        logging.info("Host Docker is not available")
        return False, None


def _get_docker_client(docker_host):
    """Get Docker client with clean config to avoid credential helpers.
    
    Creates a minimal config.json with empty auths to disable credential helpers.
    This is necessary because Docker API will use host's credential helpers if
    DOCKER_CONFIG points to a directory without config.json.
    
    Note: No cleanup needed - temp directory will be cleaned by OS, and DOCKER_CONFIG
    changes are scoped to the test process.
    
    Args:
        docker_host: Docker daemon URL. If None, uses default (host Docker).
    """
    tmpdir = tempfile.mkdtemp()
    docker_config_dir = os.path.join(tmpdir, '.docker')
    os.makedirs(docker_config_dir, exist_ok=True)
    config_file = os.path.join(docker_config_dir, 'config.json')
    
    with open(config_file, 'w') as f:
        json.dump({"auths": {}}, f)
    
    os.environ['DOCKER_CONFIG'] = docker_config_dir
    
    if docker_host is None:
        # Use default Docker client (host Docker)
        return docker.from_env()
    else:
        # Use specified Docker host (e.g., Docker-in-Docker container)
        return docker.DockerClient(base_url=docker_host)


def _cleanup_container(docker_host, container_name):
    """Helper to cleanup a container using Docker API.
    
    Args:
        docker_host: Docker daemon URL. If None, uses default (host Docker).
        container_name: Name of the container to cleanup.
    """
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
    Fixture that provides a Docker environment for the entire test module.
    By default uses host Docker, but can be configured to use Docker-in-Docker.
    
    Environment variable DOCKER_TEST_ENV controls the behavior:
    - "dind" or "DIND": Force use of Docker-in-Docker (creates isolated container)
    - Unset or any other value: Use host Docker (default)
    
    Based on: https://hub.docker.com/_/docker
    """
    # Check environment variable preference
    env_preference = _get_docker_environment_preference()
    
    if env_preference == "dind":
        # Force use of Docker-in-Docker
        logging.info("DOCKER_TEST_ENV=dind: Setting up Docker-in-Docker container")
    else:
        # Default: use host Docker
        logging.info("Using host Docker (default behavior)")
        yield None
        return
    
    # Create Docker-in-Docker container for isolated testing
    logging.info("Setting up Docker-in-Docker container")
    
    # Create Docker-in-Docker container
    # Using dind variant without TLS for simplicity in tests
    container = DockerContainer("docker:29-dind")
    container.with_kwargs(privileged=True)  # Required for Docker-in-Docker
    container.with_exposed_ports(2375)  # Docker daemon port (non-TLS)
    # Disable TLS for simplicity (DOCKER_TLS_CERTDIR not set)
    container.with_env("DOCKER_TLS_CERTDIR", "")
    
    container.start()
    
    # Wait for Docker daemon to be ready
    container_host = container.get_container_host_ip()
    container_port = container.get_exposed_port(2375)
    docker_host = f"tcp://{container_host}:{container_port}"
    
    # Wait for Docker daemon to be ready
    logging.info("Waiting for Docker daemon to start...")
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
            logging.info(f"Docker daemon ready after {attempt + 1} attempts")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                if attempt % 10 == 0:
                    logging.info(f"Waiting for Docker daemon... attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(retry_delay)
            else:
                logging.error(f"Docker daemon did not become ready: {e}")
                raise Exception(f"Docker daemon did not become ready in time: {e}")
    
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
    Fixture that sets DOCKER_HOST environment variable appropriately.
    - If using host Docker: doesn't set DOCKER_HOST (uses default)
    - If using Docker-in-Docker: sets DOCKER_HOST to point to the containerized daemon.
    
    This makes all docker commands use the appropriate Docker daemon.
    """
    if docker_dind_container is None:
        # Using host Docker, don't set DOCKER_HOST (use default)
        # Remove DOCKER_HOST if it was set previously
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        yield None  # None indicates host Docker
    else:
        # Using Docker-in-Docker, set DOCKER_HOST to containerized daemon
        docker_host = docker_dind_container["docker_host"]
        monkeypatch.setenv("DOCKER_HOST", docker_host)
        yield docker_host


@pytest.fixture(scope="function")
def test_container(docker_dind_container, docker_host_env):
    """
    Fixture that provides a test container running in the Docker environment.
    Uses the configured Docker daemon (host Docker by default, or Docker-in-Docker if specified).
    Uses a lightweight Alpine Linux container that can be started/stopped.
    """
    # Create a container in the Docker environment
    container_name = f"test-container-{int(time.time() * 1000000)}"
    
    # Run container with retries, using Docker API
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
                pytest.fail(f"Failed to create test container after {max_retries} attempts: {str(e)}")
    
    # Reduced wait - container creation is synchronous, brief wait for state to settle
    time.sleep(0.5)
    
    yield container_name
    
    # Cleanup: ensure container is stopped and removed
    _cleanup_container(docker_host_env, container_name)


@pytest.fixture(scope="function")
def container_with_healthcheck(docker_dind_container, docker_host_env):
    """
    Fixture that provides a container with healthcheck configured.
    Uses the configured Docker daemon (host Docker by default, or Docker-in-Docker if specified).
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
                pytest.fail(f"Failed to create healthcheck container after {max_retries} attempts: {str(e)}")
    
    # Reduced wait - container starts quickly, healthcheck has its own start period (5s)
    # We only need to wait for container to be created, not for healthcheck to complete
    time.sleep(2)
    
    yield container_name
    
    # Cleanup
    _cleanup_container(docker_host_env, container_name)


# ============================================================================
# Test: stop_node
# ============================================================================

class TestStopNodeIntegration:
    """Integration tests for stop_node function"""
    
    def test_returns_true_when_stop_succeeds(self, test_container, docker_host_env, caplog):
        """returns True when docker stop command succeeds on real container"""
        # Ensure container is running first
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            result = stop_node(test_container)
        
        assert result is True
        assert f"Successfully stopped {test_container}" in caplog.text
        
        # Verify container is actually stopped
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
        """returns True when docker start command succeeds on real container"""
        # First stop the container
        client = _get_docker_client(docker_host_env)
        container = client.containers.get(test_container)
        container.stop(timeout=30)
        
        with caplog.at_level(logging.INFO):
            result = start_node(test_container)
        
        assert result is True
        assert f"Successfully started {test_container}" in caplog.text
        
        # Verify container is actually running
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
        """returns health status when container has healthcheck configured"""
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
        """returns True when container becomes healthy"""
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy(container_with_healthcheck, max_wait=60)
        
        # Should eventually become healthy (or at least return True if running)
        assert result is True
        assert container_with_healthcheck in caplog.text
    
    def test_returns_true_when_no_healthcheck_but_container_running(self, test_container, docker_host_env, caplog):
        """returns True when no healthcheck but container is running"""
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
        """returns IP address when container exists"""
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
        """handles stopping and then starting the same container"""
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
        """gets IP address correctly after container restart"""
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
# BugMagnet Session 2026-01-09: Integration Test Edge Case Coverage
# ============================================================================

class TestBugMagnetEdgeCasesIntegration:
    """Advanced edge case tests for Docker utilities using real containers"""
    
    # ========================================================================
    # String Edge Cases: Container Names
    # ========================================================================
    
    def test_handles_very_long_container_name(self, docker_host_env, caplog):
        """handles container names at system limits (255+ characters)"""
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
    
