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
    Options are optimized for maximum I/O performance with aggressive caching and
    large buffer sizes suitable for high-performance computing and ML workloads.
    
    Args:
        foreground (bool, optional): Run in foreground. Defaults to True.
        allow_other (bool, optional): Allow other users to access the mount. 
            Requires 'user_allow_other' in /etc/fuse.conf. Defaults to False.
        
    Returns:
        dict: Dictionary of mount options
    """
    # Calculate optimal buffer sizes
    # Use 8GB for read/write buffers on modern systems
    BUFFER_SIZE = 2 * 1024 * 1024 * 1024  # 2GB
    # Use 8GB readahead for sequential access optimization
    READAHEAD_SIZE = 8 * 1024 * 1024 * 1024  # 8GB
    # Extended timeouts for better caching (24 hours)
    ATTR_TIMEOUT = 24 * 3600  # 24 hours
    
    options = {
        'foreground': foreground,
        'nonempty': True,
        'debug': True,
        'default_permissions': True,
        # Core performance options
        'direct_io': False,    # Must be OFF for memory mapping
        'kernel_cache': True,  # Enable kernel-level caching
        'auto_cache': True,    # Enable automatic caching
        'rw': True,           # Enable read-write mode
        
        # Enhanced I/O options
        'big_writes': True,    # Enable large write operations
        'hard_remove': True,   # Improve rename/unlink operations
        'max_read': BUFFER_SIZE,
        'max_write': BUFFER_SIZE,
        'max_readahead': READAHEAD_SIZE,
        
        # Aggressive caching timeouts
        'entry_timeout': ATTR_TIMEOUT,      # Cache directory entries
        'negative_timeout': ATTR_TIMEOUT,   # Cache negative lookups
        'attr_timeout': ATTR_TIMEOUT,       # Cache file/dir attributes
        'ac_attr_timeout': ATTR_TIMEOUT,    # Cache file attributes on access
        
        # Additional optimizations
        'splice_read': True,   # Use splice for better read performance
        'splice_write': True,  # Use splice for better write performance
        'splice_move': True,   # Use splice for better move operations
        'atomic_o_trunc': True,  # Enable atomic truncation
        'max_background': 64,  # Increase background workers
        'congestion_threshold': 48  # Set congestion threshold
    }
    
    # Only add allow_other if explicitly requested
    if allow_other:
        options['allow_other'] = True
        
    return options 