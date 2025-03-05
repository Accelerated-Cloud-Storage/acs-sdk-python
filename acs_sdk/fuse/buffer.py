# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Buffer management for ACS FUSE filesystem.

This module provides buffer implementations for reading and writing files in the ACS FUSE filesystem.
It includes classes for managing read and write operations with appropriate caching strategies.
"""

import time
import math
import threading
from threading import RLock, Lock
from io import BytesIO
from .utils import logger

# Buffer configuration
MIN_TTL = 1  # 1 second for small files
MAX_TTL = 600  # 10 minutes maximum TTL
MIN_SIZE = 1 * 1024  # 1KB
MAX_SIZE = 5 * 1024 * 1024 * 1024 * 1024  # 5TB

def calculate_ttl(size: int) -> int:
    """
    Calculate TTL based on file size using logarithmic scaling.
    
    Args:
        size (int): Size of the file in bytes
        
    Returns:
        int: TTL in seconds, between MIN_TTL and MAX_TTL
    """
    if size <= MIN_SIZE:
        return MIN_TTL
    if size >= MAX_SIZE:
        return MAX_TTL
        
    # Use logarithmic scaling to calculate TTL
    # This gives a smoother curve between MIN_TTL and MAX_TTL
    log_min = math.log(MIN_SIZE)
    log_max = math.log(MAX_SIZE)
    log_size = math.log(size)
    
    # Calculate percentage between min and max (in log space)
    percentage = (log_size - log_min) / (log_max - log_min)
    
    # Calculate TTL
    ttl = MIN_TTL + percentage * (MAX_TTL - MIN_TTL)
    return int(ttl)

class BufferEntry:
    """
    Represents a buffered file with access time tracking.
    
    This class stores file data in memory along with metadata about
    when it was last accessed and how long it should be kept in the buffer.
    
    Attributes:
        data (bytes): The file content
        last_access (float): Timestamp of the last access
        timer (threading.Timer): Timer for automatic removal
        ttl (int): Time-to-live in seconds
    """
    
    def __init__(self, data: bytes):
        """
        Initialize a new buffer entry.
        
        Args:
            data (bytes): The file content to buffer
        """
        self.data = data
        self.last_access = time.time()
        self.timer = None
        self.ttl = calculate_ttl(len(data))

class ReadBuffer:
    """
    Fast dictionary-based buffer for file contents with size-based TTL expiration.
    
    This class provides a memory cache for file contents to avoid repeated
    requests to the object storage. Entries are automatically expired based
    on their size and access patterns.
    
    Attributes:
        buffer (dict): Dictionary mapping keys to BufferEntry objects
        lock (threading.RLock): Lock for thread-safe operations
    """
    
    def __init__(self):
        """Initialize an empty read buffer."""
        self.buffer = {}  # Simple dict for faster lookups
        self.lock = RLock()  # Reentrant lock for nested operations

    def get(self, key: str) -> bytes:
        """
        Get data from buffer and update access time.
        
        Args:
            key (str): The key identifying the file
            
        Returns:
            bytes: The file content or None if not in buffer
        """
        with self.lock:
            entry = self.buffer.get(key)
            if entry:
                # Update last access time
                entry.last_access = time.time()
                
                # Reset TTL timer with size-based TTL
                if entry.timer:
                    entry.timer.cancel()
                entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
                entry.timer.daemon = True
                entry.timer.start()
                
                logger.debug(f"Buffer hit for {key} (size: {len(entry.data)}, TTL: {entry.ttl}s)")
                return entry.data
            return None

    def put(self, key: str, data: bytes) -> None:
        """
        Add data to buffer with size-based TTL.
        
        Args:
            key (str): The key identifying the file
            data (bytes): The file content to buffer
        """
        with self.lock:
            # Remove existing entry if present
            if key in self.buffer and self.buffer[key].timer:
                self.buffer[key].timer.cancel()
            
            # Create new entry with size-based TTL
            entry = BufferEntry(data)
            entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
            entry.timer.daemon = True
            entry.timer.start()
            
            logger.debug(f"Added to buffer: {key} (size: {len(data)}, TTL: {entry.ttl}s)")
            self.buffer[key] = entry

    def remove(self, key: str) -> None:
        """
        Remove an entry from buffer.
        
        Args:
            key (str): The key identifying the file to remove
        """
        with self.lock:
            if key in self.buffer:
                if self.buffer[key].timer:
                    self.buffer[key].timer.cancel()
                logger.debug(f"Removed from buffer: {key}")
                del self.buffer[key]

    def clear(self) -> None:
        """Clear all entries from buffer."""
        with self.lock:
            for entry in self.buffer.values():
                if entry.timer:
                    entry.timer.cancel()
            self.buffer.clear()
            logger.debug("Buffer cleared")

class WriteBuffer:
    """
    Manages write buffers for files being modified.
    
    This class provides in-memory buffers for files that are being written to,
    allowing changes to be accumulated before being flushed to object storage.
    
    Attributes:
        buffers (dict): Dictionary mapping keys to BytesIO objects
        lock (threading.Lock): Lock for thread-safe operations
    """
    
    def __init__(self):
        """Initialize an empty write buffer manager."""
        self.buffers = {}  # Dictionary to store file buffers
        self.lock = RLock()  # Lock for thread-safe buffer access
    
    def initialize_buffer(self, key: str, data: bytes = b"") -> None:
        """
        Initialize a buffer for a file with existing data or empty.
        
        Args:
            key (str): The key identifying the file
            data (bytes, optional): Initial data for the buffer. Defaults to empty.
        """
        with self.lock:
            if key not in self.buffers:
                self.buffers[key] = BytesIO(data)
                logger.debug(f"Initialized buffer for {key} with {len(data)} bytes")
    
    def write(self, key: str, data: bytes, offset: int) -> int:
        """
        Write data to a buffer at the specified offset.
        
        Args:
            key (str): The key identifying the file
            data (bytes): The data to write
            offset (int): The offset at which to write the data
            
        Returns:
            int: The number of bytes written
        """
        with self.lock:
            if key not in self.buffers:
                # This should not happen as initialize_buffer should be called first
                logger.warning(f"Buffer not initialized for {key}, creating empty buffer")
                self.buffers[key] = BytesIO()
            
            buffer = self.buffers[key]
            
            # Ensure buffer is large enough
            buffer.seek(0, 2)  # Seek to end
            if buffer.tell() < offset:
                buffer.write(b'\x00' * (offset - buffer.tell()))
                logger.debug(f"Extended buffer for {key} to offset {offset}")
            
            # Write data at offset
            buffer.seek(offset)
            buffer.write(data)
            logger.debug(f"Wrote {len(data)} bytes to buffer for {key} at offset {offset}")
            
            return len(data)
    
    def read(self, key: str) -> bytes:
        """
        Read the entire contents of a buffer.
        
        Args:
            key (str): The key identifying the file
            
        Returns:
            bytes: The buffer contents or None if the buffer doesn't exist
        """
        with self.lock:
            if key in self.buffers:
                buffer = self.buffers[key]
                buffer.seek(0)
                return buffer.read()
            return None
    
    def truncate(self, key: str, length: int) -> None:
        """
        Truncate a buffer to the specified length.
        
        Args:
            key (str): The key identifying the file
            length (int): The length to truncate to
        """
        with self.lock:
            if key in self.buffers:
                buffer = self.buffers[key]
                buffer.seek(0)
                data = buffer.read()
                
                # Truncate data
                if length < len(data):
                    data = data[:length]
                    logger.debug(f"Truncated data to {length} bytes")
                elif length > len(data):
                    data += b'\x00' * (length - len(data))
                    logger.debug(f"Extended data to {length} bytes")
                
                # Update buffer
                buffer.seek(0)
                buffer.write(data)
                buffer.truncate()
    
    def remove(self, key: str) -> None:
        """
        Remove a buffer.
        
        Args:
            key (str): The key identifying the file to remove
        """
        with self.lock:
            if key in self.buffers:
                del self.buffers[key]
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
            return key in self.buffers 