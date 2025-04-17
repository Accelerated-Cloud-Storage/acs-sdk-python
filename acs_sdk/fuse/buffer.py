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

# Buffer configuration
FIXED_TTL = 3600  # 60 minutes TTL for all files
MIN_LOG_SIZE = 0  # Only log detailed info for files >10MB
# --- Spool Size ---
# Spool to disk after 10GB in RAM for buffer entries
DEFAULT_SPOOL_MAX_SIZE = 10 * 1024 * 1024 * 1024 

def calculate_ttl(size: int) -> int:
    """
    Calculate TTL for buffer entries.
    
    For ML workloads, we use a fixed high TTL for all files regardless of size
    to maximize cache retention and performance.
    
    Args:
        size (int): Size of the file in bytes (ignored in ML-optimized version)
        
    Returns:
        int: TTL in seconds (fixed value)
    """
    return FIXED_TTL

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
        self.ttl = FIXED_TTL
        
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
    on their access patterns and memory pressure.
    
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
        self._spool_max_size = DEFAULT_SPOOL_MAX_SIZE # Keep track of spool size per entry
        
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

    def get(self, key: str, offset=0, size=None) -> bytes:
        """
        Get data from buffer and update access time.
        
        Args:
            key (str): The key identifying the file
            offset (int): Starting offset for read
            size (int, optional): Amount to read, or None for all
            
        Returns:
            bytes: The file content or None if not in buffer
        """
        with self.lock:
            entry = self.buffer.get(key)
            if entry:
                # Update last access time
                entry.last_access = time.time()
                
                # Reset TTL timer
                if entry.timer:
                    entry.timer.cancel()
                entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
                entry.timer.daemon = True
                entry.timer.start()
                
                # Return data (whole or partial)
                return entry.get_data(offset, size)
            return None

    def put(self, key: str, data: bytes) -> None:
        """
        Add data to buffer with TTL and memory management using BufferEntry.
        
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

            entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
            entry.timer.daemon = True
            entry.timer.start()
            
            # Add to buffer
            self.buffer[key] = entry
            
            # Update size tracking using actual entry size
            current_entry_size = entry.get_size()
            self._total_size += current_entry_size
            
            # Only log detailed info for larger files
            if current_entry_size > MIN_LOG_SIZE:
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
                
                # Only log for larger files
                if size > MIN_LOG_SIZE:
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
    High-performance write buffer using SpooledTemporaryFile to handle large files efficiently.
    
    Uses an in-memory buffer up to a certain size, then spills to disk automatically.
    
    Attributes:
        buffers (dict): Dictionary mapping keys to SpooledTemporaryFile objects
        lock (threading.RLock): Lock for thread-safe operations
        _total_size (int): Approximate memory usage/disk space of buffer contents
        SPOOL_MAX_SIZE (int): Max size (bytes) to keep in memory before spilling to disk.
    """
    
    def __init__(self):
        """Initialize an empty write buffer manager."""
        self.buffers = {}  # Dictionary to store file buffers (SpooledTemporaryFile)
        self.lock = RLock()  # Lock for thread-safe buffer access
        # Note: _total_size now represents potential disk usage as well
        self._total_size = 0  
        
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
                
                if len(data) > MIN_LOG_SIZE:
                    logger.debug(f"Initialized buffer for {key} with {len(data)/1024/1024:.2f}MB (Spooled)")
            else:
                # If buffer already exists, maybe clear and re-initialize?
                # Current behavior: do nothing if exists. Consider if overwrite is needed.
                logger.warning(f"initialize_buffer called for existing key {key}. Ignoring.")

    def write(self, key: str, data: bytes, offset: int) -> int:
        """
        Write data to a SpooledTemporaryFile buffer at the specified offset.
        
        Args:
            key (str): The key identifying the file
            data (bytes): The data to write
            offset (int): The offset at which to write the data
            
        Returns:
            int: The number of bytes written
        """
        with self.lock:
            if key not in self.buffers:
                # Auto-initialize if write is called before initialize
                logger.warning(f"Write called on non-initialized buffer {key}. Initializing.")
                self.initialize_buffer(key) 
                # If initialization failed somehow, raise error? For now, assume it works.

            buffer = self.buffers[key]
            
            # --- Size Tracking ---
            # This is trickier with overwrites. We estimate the increase.
            original_size = self.get_size(key) # Get current size before write
            potential_end_offset = offset + len(data)
            size_increase = max(0, potential_end_offset - original_size)
            self._total_size += size_increase
            # --- End Size Tracking ---

            buffer.seek(offset)
            bytes_written = buffer.write(data)

            if bytes_written != len(data):
                 logger.error(f"Partial write occurred for {key}! Expected {len(data)}, wrote {bytes_written}")
                 # Adjust size tracking if write was partial
                 self._total_size -= (len(data) - bytes_written) 
                 # Consider raising an error?
            
            if len(data) > MIN_LOG_SIZE:
                logger.debug(f"Wrote {bytes_written/1024/1024:.2f}MB to buffer for {key} at offset {offset/1024/1024:.2f}MB")
            
            # Note: Background flushing logic might need adjustment if based on memory size.
            # SpooledTemporaryFile handles spilling transparently.

            return bytes_written
            
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

    def read_chunk(self, key: str, offset: int, size: int) -> bytes:
        """
        Read a specific chunk of data from a SpooledTemporaryFile buffer.
        This avoids loading the entire file into memory when only a part is needed.

        Args:
            key (str): The key identifying the file buffer.
            offset (int): The starting byte offset to read from.
            size (int): The maximum number of bytes to read.

        Returns:
            bytes: The requested chunk of data, or None if the buffer doesn't exist or an error occurs.
        """
        with self.lock:
            if key not in self.buffers:
                logger.warning(f"read_chunk called on non-existent buffer {key}")
                return None
            
            buffer = self.buffers[key]
            if not buffer or buffer.closed:
                logger.error(f"read_chunk attempted on closed or invalid buffer for {key}")
                # Clean up potentially broken buffer reference
                if key in self.buffers: del self.buffers[key]
                return None
                
            try:
                current_size = self.get_size(key) # Use internal get_size to check bounds
                if offset >= current_size:
                    logger.debug(f"read_chunk for {key}: offset {offset} is beyond current size {current_size}")
                    return b"" # Read past EOF
                    
                buffer.seek(offset)
                # Calculate how much to actually read, respecting EOF
                bytes_remaining = current_size - offset
                read_size = min(size, bytes_remaining)
                
                if read_size <= 0:
                    logger.debug(f"read_chunk for {key}: calculated read_size is {read_size} at offset {offset}")
                    return b"" # Nothing to read

                logger.debug(f"read_chunk for {key}: Reading {read_size} bytes from offset {offset}")
                data_chunk = buffer.read(read_size)
                
                if len(data_chunk) != read_size:
                    # This might happen if the file was changed concurrently, log a warning
                    logger.warning(f"read_chunk for {key}: Expected to read {read_size} bytes but got {len(data_chunk)}")
                
                return data_chunk
                
            except Exception as e:
                logger.error(f"Error during read_chunk for key {key} at offset {offset}, size {size}: {e}", exc_info=True)
                # Don't return partial data on error, return None to indicate failure
                return None 