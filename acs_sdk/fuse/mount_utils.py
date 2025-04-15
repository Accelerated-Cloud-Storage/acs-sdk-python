# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Mount utilities for ACS FUSE filesystem.

This module provides functions for mounting and unmounting ACS buckets
as local filesystems using FUSE.
"""

import os
import sys
import signal
import subprocess
import time
from fuse import FUSE
from .utils import logger, time_function

def unmount(mountpoint, fuse_ops_class=None):
    """
    Unmount the filesystem using fusermount (Linux).
    
    This function safely unmounts a FUSE filesystem, ensuring that
    all buffers are cleared before unmounting.
    
    Args:
        mountpoint (str): Path where the filesystem is mounted
        fuse_ops_class (class, optional): The FUSE operations class to look for in active operations
    """
    logger.info(f"Unmounting filesystem at {mountpoint}")
    start_time = time.time()
    
    # Normalize mountpoint (remove trailing slash)
    mountpoint = mountpoint.rstrip('/')
    try:
        # Check if the mountpoint is mounted
        cp = subprocess.run(["mountpoint", "-q", mountpoint])
        if cp.returncode != 0:
            logger.warning(f"{mountpoint} is not mounted, nothing to unmount.")
            print(f"{mountpoint} is not mounted, nothing to unmount.")
            time_function("unmount", start_time)
            return
            
        # Clear all buffers before unmounting
        if fuse_ops_class:
            fuse_ops = next((fuse for fuse in FUSE._active_fuseops if isinstance(fuse, fuse_ops_class)), None)
            if fuse_ops:
                # Clear read buffer
                fuse_ops.read_buffer.clear()
                logger.info("Cleared read buffer")
                
                # Note: Write buffers are flushed on release, so we don't need to clear them here
            
        subprocess.run(["fusermount", "-u", mountpoint], check=True)
        logger.info(f"Unmounted {mountpoint} gracefully.")
        print(f"Unmounted {mountpoint} gracefully.")
        time_function("unmount", start_time)
    except Exception as e:
        logger.error(f"Error during unmounting: {e}")
        print(f"Error during unmounting: {e}")
        time_function("unmount", start_time)

def setup_signal_handlers(mountpoint, unmount_func):
    """
    Set up signal handlers for graceful unmounting.
    
    This function sets up handlers for SIGINT and SIGTERM to ensure
    that the filesystem is properly unmounted when the process is terminated.
    
    Args:
        mountpoint (str): Path where the filesystem is mounted
        unmount_func (callable): Function to call for unmounting
        
    Returns:
        callable: The signal handler function
    """
    def signal_handler(sig, frame):
        """
        Signal handler for SIGINT and SIGTERM.
        
        Args:
            sig (int): Signal number
            frame: Current stack frame
        """
        logger.info(f"Signal {sig} received, unmounting...")
        print("Signal received, unmounting...")
        unmount_func(mountpoint)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    return signal_handler

def get_mount_options(foreground=True, allow_other=False):
    """
    Get standard mount options for FUSE.
    
    This function returns a dictionary of options to use when mounting
    a FUSE filesystem, with options set to support memory mapping for ML workloads.
    
    Args:
        foreground (bool, optional): Run in foreground. Defaults to True.
        allow_other (bool, optional): Allow other users to access the mount. 
            Requires 'user_allow_other' in /etc/fuse.conf. Defaults to False.
        
    Returns:
        dict: Dictionary of mount options
    """
    options = {
        'foreground': foreground,
        'nonempty': True,
        'debug': True,
        'default_permissions': True,
        # For memory mapping to work, direct_io must be OFF
        'direct_io': False,
        'rw': True,
        'big_writes': True,
        # Use moderate buffer sizes
        'max_read': 8 * 1024 * 1024,      # 8MB read size
        'max_write': 8 * 1024 * 1024,     # 8MB write size
        'max_readahead': 8 * 1024 * 1024, # 8MB readahead
        # For memory mapping to work, enable kernel caching
        'kernel_cache': True,
        'auto_cache': True,
        # For interrupting stuck operations
        'intr': True,
        # Ensure hard_remove and atomic operations
        'hard_remove': True,
        'atomic_o_trunc': True,
    }
    
    # Only add allow_other if explicitly requested
    if allow_other:
        options['allow_other'] = True
        
    return options 