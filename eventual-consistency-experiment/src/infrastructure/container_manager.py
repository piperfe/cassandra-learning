#!/usr/bin/env python3
"""
Docker utilities for managing Cassandra containers

This module provides functions for:
- Stopping and starting Docker containers
- Checking container health status
- Mapping cluster node addresses to container names
- Logging Docker commands
"""

import logging
import subprocess
import time


def log_docker_command(cmd, args=None):
    """Log Docker command being executed"""
    if isinstance(cmd, list):
        full_cmd = ['docker'] + cmd
    else:
        full_cmd = ['docker', cmd]
    if args:
        if isinstance(args, list):
            full_cmd.extend(args)
        else:
            full_cmd.append(args)
    logging.info(f"Docker Command: {' '.join(full_cmd)}")


def stop_node(container_name):
    """Stop a Docker container"""
    logging.info(f"Stopping container: {container_name}")
    try:
        log_docker_command("stop", container_name)
        result = subprocess.run(
            ["docker", "stop", container_name],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            logging.info(f"✓ Successfully stopped {container_name}")
            return True
        else:
            logging.error(f"Failed to stop {container_name}: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Error stopping container {container_name}: {e}")
        return False


def start_node(container_name):
    """Start a Docker container"""
    logging.info(f"Starting container: {container_name}")
    try:
        log_docker_command("start", container_name)
        result = subprocess.run(
            ["docker", "start", container_name],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            logging.info(f"✓ Successfully started {container_name}")
            return True
        else:
            logging.error(f"Failed to start {container_name}: {result.stderr}")
            return False
    except Exception as e:
        logging.error(f"Error starting container {container_name}: {e}")
        return False


def get_container_health_status(container_name):
    """Get the health status of a Docker container"""
    try:
        inspect_cmd = ["inspect", "--format", "{{.State.Health.Status}}", container_name]
        result = subprocess.run(
            ["docker"] + inspect_cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            status = result.stdout.strip()
            return status
        else:
            # Container might not have healthcheck configured
            return None
    except Exception as e:
        logging.debug(f"Could not get health status for {container_name}: {e}")
        return None


def wait_for_container_healthy(container_name, max_wait=180):
    """Wait for a container to become healthy based on its healthcheck"""
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
                inspect_cmd = ["inspect", "--format", "{{.State.Running}}", container_name]
                result = subprocess.run(
                    ["docker"] + inspect_cmd,
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0 and result.stdout.strip() == "true":
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
    """Get the IP address of a Docker container"""
    try:
        inspect_cmd = ["inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container_name]
        log_docker_command(inspect_cmd)
        result = subprocess.run(
            ["docker"] + inspect_cmd,
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            container_ip = result.stdout.strip()
            return container_ip if container_ip else None
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

