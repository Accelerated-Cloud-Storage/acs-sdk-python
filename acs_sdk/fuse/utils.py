# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Utility functions for ACS FUSE filesystem.

This module provides logging configuration and utility functions
for the ACS FUSE filesystem implementation.
"""

import logging
import time
import os

# Enable a debug trace for all file operations if requested
TRACE_OPERATIONS = os.environ.get('ACS_FUSE_TRACE_OPS', '').lower() in ('true', '1', 'yes')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s'
)
logger = logging.getLogger('ACSFuse')
logger.setLevel(logging.DEBUG)

def time_function(func_name, start_time):
    """
    Helper function for timing operations.
    
    Calculates and logs the elapsed time for a function call.
    
    Args:
        func_name (str): Name of the function being timed
        start_time (float): Start time from time.time()
        
    Returns:
        float: Elapsed time in seconds
    """
    elapsed = time.time() - start_time
    logger.info(f"{func_name} completed in {elapsed:.4f} seconds")
    return elapsed 

def trace_op(operation, path, **details):
    """
    Trace a file operation for debugging purposes.
    
    This function logs detailed information about file operations
    when the ACS_FUSE_TRACE_OPS environment variable is set.
    
    Args:
        operation (str): The file operation being performed
        path (str): The path of the file being operated on
        **details: Additional details to log
    """
    if TRACE_OPERATIONS:
        detail_str = ', '.join(f"{k}={v}" for k, v in details.items())
        logger.debug(f"TRACE: {operation} on {path} {detail_str}") 