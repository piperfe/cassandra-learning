#!/usr/bin/env python3
"""
Docker utilities for managing Cassandra containers

This module provides functions for:
- Stopping and starting Docker containers
- Checking container health status
- Mapping cluster node addresses to container names

Uses Docker Python API for all container operations.
"""

import logging
import time
import docker


def stop_node(container_name):
    """Stop a Docker container using Docker Python API.
    
    Args:
        container_name: Name of the Docker container to stop
        
    Returns:
        bool: True if container was stopped successfully, False otherwise
        
    Raises:
        No exceptions are raised; errors are logged and False is returned
    """
    logging.info(f"Stopping container: {container_name}")
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        container.stop(timeout=30)
        logging.info(f"✓ Successfully stopped {container_name}")
        return True
    except docker.errors.NotFound:
        logging.error(f"Failed to stop {container_name}")
        return False
    except Exception as e:
        logging.error(f"Error stopping container {container_name}: {e}")
        return False


def start_node(container_name):
    """Start a Docker container using Docker Python API.
    
    Args:
        container_name: Name of the Docker container to start
        
    Returns:
        bool: True if container was started successfully, False otherwise
        
    Raises:
        No exceptions are raised; errors are logged and False is returned
    """
    logging.info(f"Starting container: {container_name}")
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        container.start()
        logging.info(f"✓ Successfully started {container_name}")
        return True
    except docker.errors.NotFound:
        logging.error(f"Failed to start {container_name}")
        return False
    except Exception as e:
        logging.error(f"Error starting container {container_name}: {e}")
        return False


def get_container_health_status(container_name):
    """Get the health status of a Docker container using Docker Python API.
    
    Args:
        container_name: Name of the Docker container to check
        
    Returns:
        str or None: Health status string ('healthy', 'unhealthy', 'starting') 
                     or None if no healthcheck is configured or container not found
        
    Note:
        Returns None if the container has no healthcheck configured or 
        if the container cannot be found
    """
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        container.reload()
        health = container.attrs.get('State', {}).get('Health', {})
        status = health.get('Status')
        return status if status else None
    except Exception as e:
        logging.debug(f"Could not get health status for {container_name}: {e}")
        return None


def wait_for_container_healthy(container_name, max_wait=180):
    """Wait for a container to become healthy based on its healthcheck.
    
    If the container has no healthcheck configured, this function will check
    if the container is running instead.
    
    Args:
        container_name: Name of the Docker container to wait for
        max_wait: Maximum time to wait in seconds (default: 180)
        
    Returns:
        bool: True if container became healthy (or is running with no healthcheck),
              False if timeout was reached
    """
    logging.info(f"Waiting for container {container_name} to become healthy...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        health_status = get_container_health_status(container_name)
        
        if health_status == "healthy":
            logging.info(f"✓ Container {container_name} is healthy")
            return True
        elif health_status == "unhealthy":
            logging.warning(f"⚠ Container {container_name} is unhealthy")
            # Continue waiting in case it recovers
        elif health_status == "starting":
            logging.info(f"  Container {container_name} healthcheck is starting...")
        elif health_status is None:
            # No healthcheck configured, check if container is running instead
            try:
                client = docker.from_env()
                container = client.containers.get(container_name)
                container.reload()
                is_running = container.attrs.get('State', {}).get('Running', False)
                if is_running:
                    logging.info(f"  Container {container_name} is running (no healthcheck configured)")
                    # If no healthcheck, just wait a bit and return
                    time.sleep(5)
                    return True
            except Exception:
                pass
        
        time.sleep(2)
    
    logging.warning(f"⚠ Container {container_name} did not become healthy within {max_wait} seconds")
    return False


def get_container_ip(container_name):
    """Get the IP address of a Docker container using Docker Python API.
    
    Gets the IP address from the first available network the container is connected to.
    
    Args:
        container_name: Name of the Docker container
        
    Returns:
        str or None: IP address of the container, or None if not found or no IP assigned
    """
    try:
        client = docker.from_env()
        container = client.containers.get(container_name)
        container.reload()
        networks = container.attrs.get('NetworkSettings', {}).get('Networks', {})
        # Get IP from first available network
        for network_name, network_config in networks.items():
            ip_address = network_config.get('IPAddress')
            if ip_address:
                return ip_address
        return None
    except Exception as e:
        logging.info(f"Could not inspect container {container_name}: {e}")
        return None


def map_replica_node_to_container(replica_node_address, container_names, cluster=None):
    """
    Map a replica node address to a container name
    
    Args:
        replica_node_address: The address of the replica node
        container_names: List of container names to check
        cluster: Optional cluster object for metadata-based matching
    
    Returns:
        Container name if found, None otherwise
    """
    logging.info("  Mapping replica node to container...")
    logging.info(f"  Replica node address: {replica_node_address}")
    
    # First, try to match by checking each container's IP
    for container in container_names:
        container_ip = get_container_ip(container)
        if container_ip:
            logging.info(f"  Container {container} has IP: {container_ip}")
            # Check if this IP matches the replica node address
            if str(container_ip) == str(replica_node_address) or \
               str(replica_node_address) in str(container_ip) or \
               str(container_ip) in str(replica_node_address):
                logging.info(f"  ✓ Matched! Container {container} (IP: {container_ip}) holds the data")
                return container
    
    # Fallback: try to match by host metadata if cluster is provided
    if cluster:
        logging.info("  Trying alternative method: matching by host metadata...")
        for host in cluster.metadata.all_hosts():
            host_address = str(host.address)
            broadcast_address = str(host.broadcast_address) if host.broadcast_address else None
            
            logging.info(f"  Host: {host_address} (broadcast: {broadcast_address})")
            
            # Check if this host matches the replica node
            if host_address == str(replica_node_address) or \
               (broadcast_address and broadcast_address == str(replica_node_address)):
                # Try to match container by checking all containers
                for container in container_names:
                    container_ip = get_container_ip(container)
                    if container_ip and (container_ip == host_address or container_ip == broadcast_address):
                        logging.info(f"  ✓ Matched container {container}")
                        return container
    
    return None
