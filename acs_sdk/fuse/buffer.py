# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Buffer management for ACS FUSE filesystem.

This module provides high-performance buffer implementations for reading and writing files
in the ACS FUSE filesystem, optimized specifically for machine learning workloads with
large model files, featuring memory-efficient chunked storage and minimal overhead.
"""

import time
import threading
from threading import RLock
from io import BytesIO
from .utils import logger
import weakref
import os
import tempfile
import shutil
import math

# Buffer configuration
MIN_TTL = 300  # 5 minutes TTL for small files (≤ 1KB)
MAX_TTL = 86400  # 24 hours TTL for large files (≥ 100GB)
MIN_SIZE = 1024  # 1 KB
MAX_SIZE = 100 * 1024 * 1024 * 1024  # 100 GB
# --- Spool Size ---
# Spool to disk after 32GB in RAM for buffer entries
DEFAULT_SPOOL_MAX_SIZE = 32 * 1024 * 1024 * 1024  # 32GB

def calculate_ttl(size: int) -> int:
    """
    Calculate dynamic TTL for buffer entries based on file size.
    
    Uses logarithmic scaling between min and max TTL values:
    - Files ≤ 1 KB: 1 minute TTL
    - Files ≥ 100 GB: 600 minutes TTL
    - Files in between: logarithmically scaled
    
    Args:
        size (int): Size of the file in bytes
        
    Returns:
        int: TTL in seconds, dynamically calculated based on size
    """
    if size <= MIN_SIZE:
        return MIN_TTL
    elif size >= MAX_SIZE:
        return MAX_TTL
    
    # Calculate logarithmic scale factor
    # log(size) - log(MIN_SIZE) gives us a value between 0 and log(MAX_SIZE/MIN_SIZE)
    # We normalize this to 0-1 by dividing by log(MAX_SIZE/MIN_SIZE)
    # Then multiply by (MAX_TTL - MIN_TTL) and add MIN_TTL
    scale = (math.log(size) - math.log(MIN_SIZE)) / (math.log(MAX_SIZE) - math.log(MIN_SIZE))
    ttl = MIN_TTL + scale * (MAX_TTL - MIN_TTL)
    return int(ttl)

class BufferEntry:
    """
    Represents a buffered file with access time tracking, using SpooledTemporaryFile.
    
    Attributes:
        spooled_file (tempfile.SpooledTemporaryFile): Stores the file content efficiently.
        last_access (float): Timestamp of the last access.
        timer (threading.Timer): Timer for automatic removal.
        ttl (int): Time-to-live in seconds.
    """
    
    def __init__(self, data: bytes):
        """
        Initialize a new buffer entry using SpooledTemporaryFile.
        
        Args:
            data (bytes): The file content to buffer.
        """
        self.last_access = time.time()
        self.timer = None
        self.ttl = calculate_ttl(len(data))
        
        # Create a SpooledTemporaryFile to hold the data
        self.spooled_file = tempfile.SpooledTemporaryFile(
            max_size=DEFAULT_SPOOL_MAX_SIZE, 
            mode='w+b'
        )
        if data:
            self.spooled_file.write(data)
            self.spooled_file.seek(0) # Reset position after writing initial data

    def get_data(self, offset=0, size=None):
        """
        Get data efficiently from the SpooledTemporaryFile buffer.
        
        Args:
            offset (int): Starting offset.
            size (int, optional): Amount to read, or None for all data from offset.
            
        Returns:
            bytes: The requested data. Returns b"" if offset is beyond EOF.
        """
        if not self.spooled_file or self.spooled_file.closed:
             logger.error("Attempted to read from a closed or invalid SpooledTemporaryFile in BufferEntry.")
             return b""
             
        try:
            current_size = self.get_size()
            if offset >= current_size:
                return b"" # Read past EOF
                
            self.spooled_file.seek(offset)
            
            if size is None:
                # Read from offset to end
                return self.spooled_file.read()
            else:
                # Read specific size, respecting EOF
                # Calculate bytes remaining from offset
                bytes_remaining = current_size - offset
                read_size = min(size, bytes_remaining)
                return self.spooled_file.read(read_size)
                
        except Exception as e:
            logger.error(f"Error reading from spooled file in BufferEntry: {e}", exc_info=True)
            return b""

    def get_size(self) -> int:
        """
        Gets the current size of the data stored in the SpooledTemporaryFile.

        Returns:
            int: The size of the stored data in bytes. Returns 0 if file is invalid/closed.
        """
        if not self.spooled_file or self.spooled_file.closed:
            return 0
        try:
            original_pos = self.spooled_file.tell()
            self.spooled_file.seek(0, 2) # os.SEEK_END
            size = self.spooled_file.tell()
            self.spooled_file.seek(original_pos) # Restore position
            return size
        except Exception as e:
            logger.error(f"Error getting size from spooled file in BufferEntry: {e}", exc_info=True)
            return 0 # Return 0 on error

    def close(self) -> None:
        """
        Explicitly closes the SpooledTemporaryFile to release resources.
        """
        if self.spooled_file and not self.spooled_file.closed:
            try:
                self.spooled_file.close()
                logger.debug("Closed SpooledTemporaryFile in BufferEntry.")
            except Exception as e:
                 logger.error(f"Error closing spooled file in BufferEntry: {e}", exc_info=True)
        self.spooled_file = None # Ensure reference is cleared

class ReadBuffer:
    """
    Fast dictionary-based buffer for file contents with TTL expiration.
    
    This class provides a memory cache for file contents to avoid repeated
    requests to the object storage. Entries are automatically expired based
    on their size and access patterns.
    
    Attributes:
        buffer (dict): Dictionary mapping keys to BufferEntry objects
        lock (threading.RLock): Lock for thread-safe operations
        _total_size (int): Approximate memory usage/disk space of buffer contents
    """
    
    def __init__(self):
        """
        Initialize an empty read buffer.
        """
        self.buffer = {}  # Simple dict for faster lookups
        self.lock = RLock()  # Reentrant lock for nested operations
        self._total_size = 0  # Track approximate memory usage
        self._lock_timeout = 5.0  # 5 second timeout for lock acquisition
        
        # Setup finalizer to clean up timers and files
        self._finalizer = weakref.finalize(self, self._cleanup_resources, self.buffer.copy())

    def _cleanup_resources(self, buffer_copy):
        """Clean up all timers and close spooled files during garbage collection"""
        logger.debug(f"ReadBuffer Finalizer: Cleaning up {len(buffer_copy)} entries.")
        for key, entry in buffer_copy.items():
            if entry:
                if entry.timer:
                    try:
                        entry.timer.cancel()
                    except:
                        pass # Ignore errors during cleanup
                # Close the spooled file
                entry.close()
        logger.debug("ReadBuffer Finalizer: Cleanup complete.")

    def _timed_lock_acquire(self, operation_name):
        """Attempt to acquire lock with timeout and logging"""
        start_time = time.time()
        acquired = self.lock.acquire(timeout=self._lock_timeout)
        elapsed = time.time() - start_time
        
        if acquired:
            if elapsed > 0.1:  # Log slow lock acquisitions
                logger.warning(f"ReadBuffer lock acquisition for {operation_name} took {elapsed:.4f} seconds")
            return True
        else:
            logger.error(f"ReadBuffer lock acquisition timeout ({self._lock_timeout}s) for {operation_name}")
            return False

    def get(self, key: str, offset=0, size=None) -> bytes:
        """Get data from buffer with enhanced timing and lock monitoring"""
        start_time = time.time()
        
        if not self._timed_lock_acquire(f"get({key})"):
            return None
            
        try:
            entry = self.buffer.get(key)
            if entry:
                # Update last access time
                entry.last_access = time.time()
                
                # Reset TTL timer with timing
                timer_start = time.time()
                if entry.timer:
                    entry.timer.cancel()
                entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
                entry.timer.daemon = True
                entry.timer.start()
                timer_elapsed = time.time() - timer_start
                if timer_elapsed > 0.1:
                    logger.warning(f"Timer reset for {key} took {timer_elapsed:.4f} seconds")
                
                # Return data with timing
                data_start = time.time()
                result = entry.get_data(offset, size)
                data_elapsed = time.time() - data_start
                if data_elapsed > 0.1:
                    logger.warning(f"Data retrieval for {key} took {data_elapsed:.4f} seconds")
                return result
            return None
        finally:
            self.lock.release()
            total_elapsed = time.time() - start_time
            if total_elapsed > 0.5:  # Log operations taking more than 500ms
                logger.warning(f"Complete get operation for {key} took {total_elapsed:.4f} seconds")
            else:
                logger.debug(f"Complete get operation for {key} took {total_elapsed:.4f} seconds")

    def put(self, key: str, data: bytes) -> None:
        """
        Add data to buffer with dynamic TTL based on size and memory management using BufferEntry.
        
        Args:
            key (str): The key identifying the file
            data (bytes): The file content to buffer
        """
        if len(data) == 0:
            # Skip caching empty data
            return
            
        with self.lock:
            data_size = len(data) # Get size before creating entry
            
            # Remove existing entry if present
            if key in self.buffer:
                old_entry = self.buffer[key]
                if old_entry.timer:
                    old_entry.timer.cancel()
                
                # Update size tracking using entry's size
                old_size = old_entry.get_size()
                self._total_size -= old_size
                
                # Explicitly close the old spooled file
                old_entry.close() 
                
                del self.buffer[key]
            
            # Create new entry (initializes SpooledTemporaryFile)
            try:
                 entry = BufferEntry(data)
            except Exception as e:
                 logger.error(f"Failed to create BufferEntry for key {key}: {e}", exc_info=True)
                 return # Don't proceed if entry creation fails

            # Set up timer with dynamically calculated TTL
            entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
            entry.timer.daemon = True
            entry.timer.start()
            
            # Add to buffer
            self.buffer[key] = entry
            
            # Update size tracking using actual entry size
            current_entry_size = entry.get_size()
            self._total_size += current_entry_size
            
            logger.debug(f"Added to read buffer: {key} (size: {current_entry_size/1024/1024:.2f}MB, TTL: {entry.ttl}s)")

    def remove(self, key: str) -> None:
        """
        Remove an entry from buffer and close its spooled file.
        
        Args:
            key (str): The key identifying the file to remove
        """
        with self.lock:
            if key in self.buffer:
                entry = self.buffer.pop(key) # Remove from dict first
                
                if entry.timer:
                    entry.timer.cancel()
                
                # Get size for logging and tracking before closing
                size = entry.get_size()
                self._total_size -= size
                self._total_size = max(0, self._total_size) # Ensure non-negative
                
                # Explicitly close the spooled file
                entry.close()
                
                logger.debug(f"Removed from read buffer: {key} (size: {size/1024/1024:.2f}MB)")

    def clear(self) -> None:
        """Clear all entries from buffer, closing their files."""
        with self.lock:
             logger.debug(f"Clearing ReadBuffer ({len(self.buffer)} entries)...")
             for key, entry in self.buffer.items():
                 if entry.timer:
                     entry.timer.cancel()
                 # Close the spooled file
                 entry.close()
             self.buffer.clear()
             self._total_size = 0
             logger.debug("ReadBuffer cleared")

class WriteBuffer:
    """
    High-performance write buffer using SpooledTemporaryFile with enhanced monitoring.
    """
    
    def __init__(self):
        self.buffers = {}
        self.lock = RLock()
        self._total_size = 0
        self._lock_timeout = 5.0  # 5 second timeout for lock acquisition
        
    def _timed_lock_acquire(self, operation_name):
        """Attempt to acquire lock with timeout and logging"""
        start_time = time.time()
        acquired = self.lock.acquire(timeout=self._lock_timeout)
        elapsed = time.time() - start_time
        
        if acquired:
            if elapsed > 0.1:
                logger.warning(f"WriteBuffer lock acquisition for {operation_name} took {elapsed:.4f} seconds")
            return True
        else:
            logger.error(f"WriteBuffer lock acquisition timeout ({self._lock_timeout}s) for {operation_name}")
            return False

    def write(self, key: str, data: bytes, offset: int) -> int:
        """Write data with enhanced monitoring and timeout protection"""
        start_time = time.time()
        
        if not self._timed_lock_acquire(f"write({key})"):
            raise IOError("Failed to acquire lock for write operation")
            
        try:
            if key not in self.buffers:
                logger.warning(f"Write called on non-initialized buffer {key}. Auto-initializing.")
                self.initialize_buffer(key)

            buffer = self.buffers[key]
            
            # Track size changes
            original_size = self.get_size(key)
            potential_end_offset = offset + len(data)
            size_increase = max(0, potential_end_offset - original_size)
            self._total_size += size_increase

            # Perform write with timing
            write_start = time.time()
            buffer.seek(offset)
            bytes_written = buffer.write(data)
            write_elapsed = time.time() - write_start
            
            if write_elapsed > 0.1:
                logger.warning(f"Buffer write operation for {key} took {write_elapsed:.4f} seconds")
            
            if bytes_written != len(data):
                logger.error(f"Partial write occurred for {key}! Expected {len(data)}, wrote {bytes_written}")
                self._total_size -= (len(data) - bytes_written)
                
            return bytes_written
            
        finally:
            self.lock.release()
            total_elapsed = time.time() - start_time
            if total_elapsed > 0.5:
                logger.warning(f"Complete write operation for {key} took {total_elapsed:.4f} seconds")
            else:
                logger.debug(f"Complete write operation for {key} took {total_elapsed:.4f} seconds")

    def initialize_buffer(self, key: str, data: bytes = b"") -> None:
        """
        Initialize a buffer for a file using SpooledTemporaryFile.
        
        Args:
            key (str): The key identifying the file
            data (bytes, optional): Initial data for the buffer. Defaults to empty.
        """
        with self.lock:
            if key not in self.buffers:
                # Create a SpooledTemporaryFile
                # 'w+b' allows reading and writing in binary mode
                spooled_file = tempfile.SpooledTemporaryFile(
                    max_size=DEFAULT_SPOOL_MAX_SIZE, 
                    mode='w+b' 
                )
                if data:
                    spooled_file.write(data)
                    spooled_file.seek(0) # Reset position after writing initial data
                    self._total_size += len(data)
                    
                self.buffers[key] = spooled_file
                
                logger.debug(f"Initialized buffer for {key} with {len(data)/1024/1024:.2f}MB (Spooled)")
            else:
                # If buffer already exists, maybe clear and re-initialize?
                # Current behavior: do nothing if exists. Consider if overwrite is needed.
                logger.warning(f"initialize_buffer called for existing key {key}. Ignoring.")

    def read(self, key: str, offset: int = 0, size: int = None) -> bytes:
        """
        Read contents from a SpooledTemporaryFile buffer.
        If size is None, reads the *entire* contents (needed for flushing).
        If size is specified, reads a specific chunk from the given offset.
        
        Args:
            key (str): The key identifying the file.
            offset (int, optional): Starting offset for chunked read. Defaults to 0.
            size (int, optional): Amount to read for chunked read. Defaults to None (read all).
            
        Returns:
            bytes: The buffer contents (full or chunk) or None if the buffer doesn't exist or error occurs.
        """
        with self.lock:
            if key not in self.buffers:
                logger.warning(f"read called on non-existent buffer {key}")
                return None
            
            buffer = self.buffers[key]
            if not buffer or buffer.closed:
                logger.error(f"read attempted on closed or invalid buffer for {key}")
                if key in self.buffers: del self.buffers[key] # Clean up broken ref
                return None

            try:
                original_pos = buffer.tell() # Store original position
                
                if size is None: 
                    # Read ALL data (original behavior for flushing)
                    logger.debug(f"Reading entire buffer for {key} (size=None)")
                    buffer.seek(0)
                    data = buffer.read()
                    buffer.seek(original_pos) # Restore original position
                    return data
                else:
                    # Read a specific chunk
                    current_buffer_size = self.get_size(key)
                    if offset >= current_buffer_size:
                        logger.debug(f"read chunk for {key}: offset {offset} is beyond current size {current_buffer_size}")
                        return b"" # Read past EOF
                    
                    buffer.seek(offset)
                    bytes_remaining = current_buffer_size - offset
                    read_size = min(size, bytes_remaining)
                    
                    if read_size <= 0:
                         logger.debug(f"read chunk for {key}: calculated read_size is {read_size} at offset {offset}")
                         return b"" # Nothing to read
                         
                    logger.debug(f"Reading chunk from buffer {key}: offset={offset}, read_size={read_size}")
                    data_chunk = buffer.read(read_size)
                    
                    # Restore original position after chunk read too
                    buffer.seek(original_pos)
                    
                    if len(data_chunk) != read_size:
                         logger.warning(f"read chunk for {key}: Expected {read_size} bytes but got {len(data_chunk)}")

                    return data_chunk

            except Exception as e:
                logger.error(f"Error reading from spooled file for key {key} (offset={offset}, size={size}): {e}", exc_info=True)
                # Attempt to restore position even on error
                try:
                    if buffer and not buffer.closed:
                        buffer.seek(original_pos)
                except Exception as e_seek:
                    logger.error(f"Error restoring seek position for {key} after read error: {e_seek}")
                return None # Indicate error

    def truncate(self, key: str, length: int) -> None:
        """
        Truncate a SpooledTemporaryFile buffer to the specified length.
        
        Args:
            key (str): The key identifying the file
            length (int): The length to truncate to
        """
        with self.lock:
            if key in self.buffers:
                buffer = self.buffers[key]
                
                # --- Size Tracking ---
                original_size = self.get_size(key)
                size_change = length - original_size
                self._total_size += size_change
                # Ensure total_size doesn't go negative
                self._total_size = max(0, self._total_size) 
                # --- End Size Tracking ---
                
                buffer.truncate(length)
                # Ensure position is within new bounds
                if buffer.tell() > length:
                    buffer.seek(length) 
                    
                logger.debug(f"Truncated buffer {key} to {length/1024/1024:.2f}MB")

    def get_size(self, key: str) -> int:
        """
        Gets the current size of the buffer for a given key.
        Works for both in-memory and on-disk SpooledTemporaryFile.

        Args:
            key (str): The key identifying the file buffer.

        Returns:
            int: The size of the buffer in bytes. Returns 0 if key not found.
        """
        with self.lock:
            if key not in self.buffers:
                # Return 0 or raise KeyError? Returning 0 might be safer.
                logger.warning(f"get_size called on non-existent buffer {key}")
                return 0 
            buffer = self.buffers[key]
            # Store current position, seek to end, get size, restore position
            original_pos = buffer.tell()
            buffer.seek(0, 2) # os.SEEK_END
            size = buffer.tell()
            buffer.seek(original_pos) # Restore position
            return size

    def remove(self, key: str) -> None:
        """
        Remove a buffer and explicitly close the SpooledTemporaryFile.
        
        Args:
            key (str): The key identifying the file to remove
        """
        with self.lock:
            if key in self.buffers:
                buffer = self.buffers.pop(key) # Remove from dict first
                try:
                    # Get size for tracking *before* closing. Need to seek.
                    original_pos = buffer.tell()
                    buffer.seek(0, 2)
                    size = buffer.tell()
                    buffer.seek(original_pos) 
                    self._total_size -= size
                    self._total_size = max(0, self._total_size)
                except Exception: 
                     # Error getting size likely means buffer already closed/invalid
                     pass 
                     
                try:
                    # Explicitly close the file handle to ensure resources are released
                    # and temp file (if any) is deleted.
                    buffer.close() 
                except Exception as e:
                    logger.error(f"Error closing spooled file for key {key}: {e}", exc_info=True)
                    
                # Cancel any background flush (if logic exists)
                # if key in self._background_flushes:
                #     del self._background_flushes[key]
                    
                logger.debug(f"Removed buffer for {key}")
                
    def has_buffer(self, key: str) -> bool:
        """
        Check if a buffer exists for the specified key.
        
        Args:
            key (str): The key to check
            
        Returns:
            bool: True if a buffer exists, False otherwise
        """
        with self.lock:
            # Additionally check if the file object is still valid/open? Maybe overkill.
            return key in self.buffers 