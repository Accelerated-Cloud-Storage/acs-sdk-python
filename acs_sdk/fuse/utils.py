# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Utility functions for ACS FUSE filesystem.

This module provides logging configuration and utility functions
for the ACS FUSE filesystem implementation.
"""

import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ACSFuse')

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