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
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunk size for large files
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100MB threshold for chunked storage
MIN_LOG_SIZE = 10 * 1024 * 1024  # Only log detailed info for files >10MB

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
    Represents a buffered file with access time tracking.
    
    Uses a memory-efficient storage approach for ML model files.
    
    Attributes:
        data (bytes or dict): The file content (whole or chunked)
        last_access (float): Timestamp of the last access
        timer (threading.Timer): Timer for automatic removal
        ttl (int): Time-to-live in seconds
        is_chunked (bool): Whether this entry uses chunked storage
    """
    
    def __init__(self, data: bytes):
        """
        Initialize a new buffer entry with smart storage.
        
        Args:
            data (bytes): The file content to buffer
        """
        self.last_access = time.time()
        self.timer = None
        self.ttl = FIXED_TTL
        self.is_chunked = len(data) > LARGE_FILE_THRESHOLD
        
        # Store large files as chunks to reduce memory pressure
        if self.is_chunked:
            self.data = {}
            for i in range(0, len(data), CHUNK_SIZE):
                chunk_idx = i // CHUNK_SIZE
                chunk_end = min(i + CHUNK_SIZE, len(data))
                self.data[chunk_idx] = data[i:chunk_end]
        else:
            self.data = data

    def get_data(self, offset=0, size=None):
        """
        Get data efficiently from buffer, handling chunked storage.
        
        Args:
            offset (int): Starting offset
            size (int, optional): Amount to read, or None for all data
            
        Returns:
            bytes: The requested data
        """
        if not self.is_chunked:
            # Handle non-chunked data simply
            data_len = len(self.data)
            if offset >= data_len:
                return b"" # Read past EOF
            end = data_len if size is None else min(offset + size, data_len)
            return self.data[offset:end]
        
        # For chunked storage, only load the required chunks
        # --- Refined Chunked Data Retrieval --- 
        result = bytearray()
        current_pos = offset
        bytes_to_read = size

        # Determine the total size represented by chunks (needed if size is None)
        total_chunked_size = 0
        if self.data: # Check if data dictionary is not empty
             max_chunk_index = max(self.data.keys())
             if max_chunk_index in self.data:
                 total_chunked_size = max_chunk_index * CHUNK_SIZE + len(self.data[max_chunk_index])
        
        # If size is None, read until the end of the represented data
        if bytes_to_read is None:
            if offset >= total_chunked_size:
                return b""
            bytes_to_read = total_chunked_size - offset

        # Calculate the absolute end position
        end_pos = offset + bytes_to_read

        while current_pos < end_pos:
            chunk_idx = current_pos // CHUNK_SIZE
            offset_in_chunk = current_pos % CHUNK_SIZE

            if chunk_idx not in self.data:
                # This indicates a serious problem - a chunk is missing!
                # This might happen if eviction logic incorrectly removed a single chunk.
                logger.error(f"ReadBuffer Error: Chunk {chunk_idx} missing while reading data.")
                # Returning partial data is risky for mmap. Best to stop here.
                break 

            chunk = self.data[chunk_idx]
            bytes_in_chunk = len(chunk)

            # How many bytes to read from *this* chunk
            read_from_chunk = min(bytes_in_chunk - offset_in_chunk, end_pos - current_pos)

            if read_from_chunk <= 0:
                # Should not happen if end_pos logic is correct, but safety break
                break 
                
            result.extend(chunk[offset_in_chunk : offset_in_chunk + read_from_chunk])
            current_pos += read_from_chunk

        # The loop condition ensures we don't read more than requested.
        # No final trim needed unless the loop breaks unexpectedly.
        return bytes(result)

class ReadBuffer:
    """
    Fast dictionary-based buffer for file contents with TTL expiration.
    
    This class provides a memory cache for file contents to avoid repeated
    requests to the object storage. Entries are automatically expired based
    on their access patterns and memory pressure.
    
    Attributes:
        buffer (dict): Dictionary mapping keys to BufferEntry objects
        lock (threading.RLock): Lock for thread-safe operations
        _total_size (int): Approximate memory usage of buffer
    """
    
    def __init__(self, max_size_gb=None):
        """
        Initialize an empty read buffer with optional size limit.
        
        Args:
            max_size_gb (float, optional): Maximum buffer size in GB
        """
        self.buffer = {}  # Simple dict for faster lookups
        self.lock = RLock()  # Reentrant lock for nested operations
        self._total_size = 0  # Track approximate memory usage
        self._max_size = max_size_gb * 1024 * 1024 * 1024 if max_size_gb else None
        
        # Setup finalizer to clean up timers
        self._finalizer = weakref.finalize(self, self._cleanup_timers, self.buffer.copy())

    def _cleanup_timers(self, buffer_copy):
        """Clean up all timers during garbage collection"""
        for entry in buffer_copy.values():
            if entry.timer:
                try:
                    entry.timer.cancel()
                except:
                    pass

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
        Add data to buffer with TTL and memory management.
        
        Args:
            key (str): The key identifying the file
            data (bytes): The file content to buffer
        """
        if len(data) == 0:
            # Skip caching empty data
            return
            
        with self.lock:
            # Remove existing entry if present
            if key in self.buffer:
                old_entry = self.buffer[key]
                if old_entry.timer:
                    old_entry.timer.cancel()
                
                # Update size tracking (estimate)
                if old_entry.is_chunked:
                    old_size = sum(len(chunk) for chunk in old_entry.data.values())
                else:
                    old_size = len(old_entry.data)
                self._total_size -= old_size
                
                del self.buffer[key]
            
            # Check memory pressure and clear oldest entries if needed
            if self._max_size and self._total_size + len(data) > self._max_size:
                self._evict_oldest_entries(len(data))
            
            # Create new entry with fixed TTL
            entry = BufferEntry(data)
            entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
            entry.timer.daemon = True
            entry.timer.start()
            
            # Add to buffer
            self.buffer[key] = entry
            
            # Update size tracking
            self._total_size += len(data)
            
            # Only log detailed info for larger files
            if len(data) > MIN_LOG_SIZE:
                logger.debug(f"Added to buffer: {key} (size: {len(data)/1024/1024:.2f}MB, TTL: {entry.ttl}s)")

    def _evict_oldest_entries(self, required_space):
        """
        Evict oldest entries until enough space is available.
        
        Args:
            required_space (int): Space needed in bytes
        """
        entries = [(k, v.last_access) for k, v in self.buffer.items()]
        entries.sort(key=lambda x: x[1])  # Sort by last_access time
        
        for key, _ in entries:
            entry = self.buffer[key]
            # Skip if timer already cancelled
            if entry.timer:
                entry.timer.cancel()
            
            # Estimate size
            if entry.is_chunked:
                size = sum(len(chunk) for chunk in entry.data.values())
            else:
                size = len(entry.data)
                
            self._total_size -= size
            del self.buffer[key]
            
            logger.debug(f"Evicted from buffer: {key} due to memory pressure")
            
            # Check if we've freed enough space
            if self._total_size + required_space <= self._max_size:
                break

    def remove(self, key: str) -> None:
        """
        Remove an entry from buffer.
        
        Args:
            key (str): The key identifying the file to remove
        """
        with self.lock:
            if key in self.buffer:
                entry = self.buffer[key]
                if entry.timer:
                    entry.timer.cancel()
                
                # Update size tracking
                if entry.is_chunked:
                    size = sum(len(chunk) for chunk in entry.data.values())
                else:
                    size = len(entry.data)
                    
                self._total_size -= size
                del self.buffer[key]
                
                # Only log for larger files
                if size > MIN_LOG_SIZE:
                    logger.debug(f"Removed from buffer: {key}")

    def clear(self) -> None:
        """Clear all entries from buffer."""
        with self.lock:
            for entry in self.buffer.values():
                if entry.timer:
                    entry.timer.cancel()
            self.buffer.clear()
            self._total_size = 0
            logger.debug("Buffer cleared")

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
    
    # Spool to disk after 512MB in RAM
    SPOOL_MAX_SIZE = 512 * 1024 * 1024 

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
                    max_size=self.SPOOL_MAX_SIZE, 
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
            
    def read(self, key: str) -> bytes:
        """
        Read the *entire* contents of a SpooledTemporaryFile buffer.
        Required for compatibility with the unchanged ACSClient.put_object.
        
        Args:
            key (str): The key identifying the file
            
        Returns:
            bytes: The buffer contents or None if the buffer doesn't exist
        """
        with self.lock:
            if key in self.buffers:
                buffer = self.buffers[key]
                buffer.seek(0) # Go to the start
                try:
                    # Read everything into memory - needed for existing put_object
                    data = buffer.read() 
                    return data
                except Exception as e:
                    logger.error(f"Error reading from spooled file for key {key}: {e}", exc_info=True)
                    return None # Or raise?
                finally:
                     # It's generally good practice to seek back to 0 after full read
                     # although the current _flush_buffer doesn't rely on position.
                     try:
                         buffer.seek(0)
                     except Exception: 
                         pass # Ignore error if file is closed/invalid
            return None
            
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