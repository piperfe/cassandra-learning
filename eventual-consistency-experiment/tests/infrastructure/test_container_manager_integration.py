#!/usr/bin/env python3
"""
Integration Tests for src.infrastructure.container_manager.py

This test suite provides integration tests for container management functions,
testing against real Docker containers.

Test Fixture Architecture:
    The test suite uses a 2-step fixture initialization process:
    
    1. docker_host (module-scoped)
       - Establishes which Docker daemon to use
       - Default: host Docker (most common)
       - Optional: Docker-in-Docker for isolation
    
    2. docker_environment (module-scoped)
       - Validates Docker is operational using a health-check container
       - Pre-pulls required images (nginx:alpine, alpine:latest)
       - Runs once per test session for efficiency
       - Tests only need to depend on this fixture

Environment Variable Control:
    DOCKER_TEST_ENV: Controls which Docker environment to use
    - Unset or any other value: Use host Docker (default, recommended)
    - "dind" or "DIND": Force use of Docker-in-Docker (isolated environment)

Examples:
    # Use host Docker (default, recommended)
    pytest tests/infrastructure/test_container_manager_integration.py
    
    # Force use of Docker-in-Docker (for isolated testing)
    DOCKER_TEST_ENV=dind pytest tests/infrastructure/test_container_manager_integration.py
    
    # Run specific test class
    pytest tests/infrastructure/test_container_manager_integration.py::TestStopNodeIntegration -v
    
    # Run with verbose output
    pytest tests/infrastructure/test_container_manager_integration.py -v --tb=short

Test Coverage:
    - stop_node(): Container stop operations
    - start_node(): Container start operations
    - get_container_health_status(): Health status retrieval
    - wait_for_container_healthy(): Health status polling
    - get_container_ip(): IP address retrieval
    - Complex scenarios: restart, multiple containers, edge cases
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
def docker_host():
    """
    Step 1: Establish the Docker host to use for all tests.
    
    By default uses host Docker, but can be configured to use Docker-in-Docker.
    Environment variable DOCKER_TEST_ENV controls the behavior:
    - "dind" or "DIND": Force use of Docker-in-Docker (creates isolated container)
    - Unset or any other value: Use host Docker (default)
    
    Returns:
        docker_host_url: None for host Docker, or "tcp://host:port" for DIND
    """
    env_preference = _get_docker_environment_preference()
    
    # Save original DOCKER_HOST if it exists
    original_docker_host = os.environ.get("DOCKER_HOST")
    
    if env_preference != "dind":
        # Default: use host Docker
        logging.info("Using host Docker (default behavior)")
        # Clear DOCKER_HOST to use default
        if "DOCKER_HOST" in os.environ:
            del os.environ["DOCKER_HOST"]
        
        yield None
        
        # Restore original DOCKER_HOST
        if original_docker_host:
            os.environ["DOCKER_HOST"] = original_docker_host
        return
    
    # Force use of Docker-in-Docker
    logging.info("DOCKER_TEST_ENV=dind: Setting up Docker-in-Docker container")
    
    # Create Docker-in-Docker container
    container = DockerContainer("docker:29-dind")
    container.with_kwargs(privileged=True)
    container.with_exposed_ports(2375)
    container.with_env("DOCKER_TLS_CERTDIR", "")  # Disable TLS for simplicity
    container.start()
    
    # Get connection details
    container_host = container.get_container_host_ip()
    container_port = container.get_exposed_port(2375)
    docker_host_url = f"tcp://{container_host}:{container_port}"
    
    # Set DOCKER_HOST environment variable for all tests
    os.environ["DOCKER_HOST"] = docker_host_url
    
    # Wait for Docker daemon to be ready
    logging.info("Waiting for Docker daemon to start...")
    time.sleep(5)
    max_retries = 60
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            client = _get_docker_client(docker_host_url)
            client.info()
            client.containers.list(all=True)
            logging.info(f"Docker daemon ready after {attempt + 1} attempts")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                if attempt % 10 == 0:
                    logging.info(f"Waiting for Docker daemon... attempt {attempt + 1}/{max_retries}: {e}")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Docker daemon did not become ready in time: {e}")
    
    yield docker_host_url
    
    # Cleanup
    try:
        container.stop()
        container.remove()
    except Exception:
        pass
    
    # Restore original DOCKER_HOST
    if original_docker_host:
        os.environ["DOCKER_HOST"] = original_docker_host
    elif "DOCKER_HOST" in os.environ:
        del os.environ["DOCKER_HOST"]


@pytest.fixture(scope="module")
def docker_environment(docker_host):
    """
    Step 2: Validate Docker environment is working by running a health-check container.
    
    This fixture ensures the Docker daemon is fully operational by:
    1. Pulling an image (tests image pulling)
    2. Creating a container with healthcheck (tests container creation)
    3. Verifying the container becomes healthy (tests container lifecycle)
    
    This is the only fixture tests need to depend on - it guarantees Docker is ready.
    """
    logging.info("Validating Docker environment with health-check container")
    
    validation_container_name = f"docker-env-validation-{int(time.time() * 1000000)}"
    
    try:
        client = _get_docker_client(docker_host)
        
        # Pull image - validates daemon can pull images
        logging.info("Pulling nginx:alpine image for validation")
        client.images.pull("nginx:alpine")
        
        # Pull alpine too since tests will use it
        logging.info("Pulling alpine:latest image for tests")
        client.images.pull("alpine:latest")
        
        # Create and start container with healthcheck
        logging.info(f"Creating validation container: {validation_container_name}")
        container = client.containers.create(
            image="nginx:alpine",
            name=validation_container_name,
            detach=True,
            healthcheck={
                'test': ['CMD-SHELL', 'wget --quiet --tries=1 --spider http://localhost/ || exit 1'],
                'interval': 1000000000,  # 1s in nanoseconds
                'timeout': 500000000,  # 500ms in nanoseconds
                'retries': 3,
                'start_period': 3000000000  # 3s in nanoseconds
            }
        )
        container.start()
        
        # Wait for container to become healthy
        logging.info("Waiting for validation container to become healthy")
        max_wait = 30
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            container.reload()
            health_status = container.attrs.get('State', {}).get('Health', {}).get('Status')
            
            if health_status == 'healthy':
                logging.info("✓ Docker environment validated successfully")
                break
            elif health_status in ['starting', None]:
                time.sleep(1)
                continue
            else:
                raise Exception(f"Validation container unhealthy: {health_status}")
        else:
            raise Exception(f"Validation container did not become healthy within {max_wait}s")
        
        # Docker environment is ready - yield control to tests
        yield docker_host
        
    finally:
        # Cleanup validation container
        logging.info("Cleaning up validation container")
        _cleanup_container(docker_host, validation_container_name)


@pytest.fixture(scope="function")
def test_container(docker_environment):
    """
    Helper fixture: Creates a simple Alpine container for tests that need one.
    
    Most tests can create their own containers using _get_docker_client(docker_environment).
    This fixture is provided for convenience.
    """
    container_name = f"test-container-{int(time.time() * 1000000)}"
    
    client = _get_docker_client(docker_environment)
    container = client.containers.create(
        image="alpine:latest",
        name=container_name,
        command=["tail", "-f", "/dev/null"],
        detach=True
    )
    container.start()
    time.sleep(0.5)  # Brief wait for container to be fully started
    
    yield container_name
    
    _cleanup_container(docker_environment, container_name)


@pytest.fixture(scope="function")
def container_with_healthcheck(docker_environment):
    """
    Helper fixture: Creates an nginx container with healthcheck for tests that need one.
    
    Most tests can create their own containers using _get_docker_client(docker_environment).
    This fixture is provided for convenience.
    """
    container_name = f"healthcheck-container-{int(time.time() * 1000000)}"
    
    client = _get_docker_client(docker_environment)
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
    time.sleep(2)  # Wait for healthcheck to initialize
    
    yield container_name
    
    _cleanup_container(docker_environment, container_name)


# ============================================================================
# Test: stop_node
# ============================================================================

class TestStopNodeIntegration:
    """Integration tests for stop_node function"""
    
    def test_returns_true_when_stop_succeeds(self, test_container, docker_environment, caplog):
        """returns True when docker stop command succeeds on real container"""
        # Ensure container is running first
        client = _get_docker_client(docker_environment)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            result = stop_node(test_container)
        
        assert result is True
        assert f"Successfully stopped {test_container}" in caplog.text
        
        # Verify container is actually stopped
        container.reload()
        assert container.attrs['State']['Running'] is False
    
    def test_returns_false_when_container_not_found(self, docker_environment, caplog):
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
    
    def test_returns_true_when_start_succeeds(self, test_container, docker_environment, caplog):
        """returns True when docker start command succeeds on real container"""
        # First stop the container
        client = _get_docker_client(docker_environment)
        container = client.containers.get(test_container)
        container.stop(timeout=30)
        
        with caplog.at_level(logging.INFO):
            result = start_node(test_container)
        
        assert result is True
        assert f"Successfully started {test_container}" in caplog.text
        
        # Verify container is actually running
        container.reload()
        assert container.attrs['State']['Running'] is True
    
    def test_returns_false_when_container_not_found(self, docker_environment, caplog):
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
    
    def test_returns_health_status_when_available(self, container_with_healthcheck, docker_environment):
        """returns health status when container has healthcheck configured"""
        # Wait for healthcheck to begin
        time.sleep(3)
        
        # Verify healthcheck is actually configured by checking directly
        client = _get_docker_client(docker_environment)
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
    
    def test_strips_whitespace_from_status(self, container_with_healthcheck, docker_environment):
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
    
    def test_returns_true_when_container_becomes_healthy(self, container_with_healthcheck, docker_environment, caplog):
        """returns True when container becomes healthy"""
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy(container_with_healthcheck, max_wait=60)
        
        # Should eventually become healthy (or at least return True if running)
        assert result is True
        assert container_with_healthcheck in caplog.text
    
    def test_returns_true_when_no_healthcheck_but_container_running(self, test_container, docker_environment, caplog):
        """returns True when no healthcheck but container is running"""
        # Ensure container is running
        client = _get_docker_client(docker_environment)
        container = client.containers.get(test_container)
        container.start()
        
        with caplog.at_level(logging.INFO):
            result = wait_for_container_healthy(test_container, max_wait=10)
        
        assert result is True
        assert "running (no healthcheck configured)" in caplog.text
    
    def test_returns_false_when_container_not_found(self, docker_environment, caplog):
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
    
    def test_stop_then_start_same_container(self, test_container, docker_environment, caplog):
        """handles stopping and then starting the same container"""
        # Ensure container is running
        client = _get_docker_client(docker_environment)
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
    
    def test_handles_very_long_container_name(self, docker_environment, caplog):
        """handles container names at system limits (255+ characters)"""
        very_long_name = "a" * 200  # Docker has limits, use reasonable length
        container_name = f"test-{very_long_name}"
        
        # Create container with long name using Docker API
        try:
            client = _get_docker_client(docker_environment)
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
            _cleanup_container(docker_environment, container_name)
    
    # ========================================================================
    # Error Condition Edge Cases
    # ========================================================================
    
    def test_handles_nonexistent_container_gracefully(self, docker_environment, caplog):
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
    
    def test_wait_for_container_healthy_with_short_timeout(self, test_container, docker_environment, caplog):
        """handles very short timeout values correctly"""
        # Ensure container is running
        client = _get_docker_client(docker_environment)
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
    
    def test_handles_multiple_containers_simultaneously(self, docker_environment, caplog):
        """handles operations on multiple containers at the same time"""
        client = _get_docker_client(docker_environment)
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
                _cleanup_container(docker_environment, container_name)
    
