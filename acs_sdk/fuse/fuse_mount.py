# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
from fuse import FUSE, FuseOSError, Operations
import errno
import os
import sys 
import time 
from datetime import datetime
from acs_sdk.client.client import ACSClient
from acs_sdk.client.client import Session
from acs_sdk.client.types import ListObjectsOptions
from io import BytesIO
from threading import Lock, RLock
import subprocess
import signal
import logging
import time
import threading
import math

# Configure logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ACSFuse')

# Helper function for timing operations
def time_function(func_name, start_time):
    elapsed = time.time() - start_time
    logger.info(f"{func_name} completed in {elapsed:.4f} seconds")
    return elapsed

# Cache configuration
MIN_TTL = 1  # 1 second for small files
MAX_TTL = 600  # 10 minutes maximum TTL
MIN_SIZE = 1 * 1024  # 1KB
MAX_SIZE = 5 * 1024 * 1024 * 1024 * 1024  # 5TB

def calculate_ttl(size: int) -> int:
    """Calculate TTL based on file size using logarithmic scaling.
    
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

class CacheEntry:
    """Represents a cached file with access time tracking."""
    def __init__(self, data: bytes):
        self.data = data
        self.last_access = time.time()
        self.timer = None
        self.ttl = calculate_ttl(len(data))

class ReadCache:
    """Fast dictionary-based cache for file contents with size-based TTL expiration."""
    def __init__(self):
        self.cache = {}  # Simple dict for faster lookups
        self.lock = RLock()  # Reentrant lock for nested operations

    def get(self, key: str) -> bytes:
        """Get data from cache and update access time."""
        with self.lock:
            entry = self.cache.get(key)
            if entry:
                # Update last access time
                entry.last_access = time.time()
                
                # Reset TTL timer with size-based TTL
                if entry.timer:
                    entry.timer.cancel()
                entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
                entry.timer.daemon = True
                entry.timer.start()
                
                logger.debug(f"Cache hit for {key} (size: {len(entry.data)}, TTL: {entry.ttl}s)")
                return entry.data
            return None

    def put(self, key: str, data: bytes) -> None:
        """Add data to cache with size-based TTL."""
        with self.lock:
            # Remove existing entry if present
            if key in self.cache and self.cache[key].timer:
                self.cache[key].timer.cancel()
            
            # Create new entry with size-based TTL
            entry = CacheEntry(data)
            entry.timer = threading.Timer(entry.ttl, lambda: self.remove(key))
            entry.timer.daemon = True
            entry.timer.start()
            
            logger.debug(f"Added to cache: {key} (size: {len(data)}, TTL: {entry.ttl}s)")
            self.cache[key] = entry

    def remove(self, key: str) -> None:
        """Remove an entry from cache."""
        with self.lock:
            if key in self.cache:
                if self.cache[key].timer:
                    self.cache[key].timer.cancel()
                logger.debug(f"Removed from cache: {key}")
                del self.cache[key]

    def clear(self) -> None:
        """Clear all entries from cache."""
        with self.lock:
            for entry in self.cache.values():
                if entry.timer:
                    entry.timer.cancel()
            self.cache.clear()
            logger.debug("Cache cleared")

class ACSFuse(Operations):
    """FUSE implementation for Accelerated Cloud Storage."""

    def __init__(self, bucket_name):
        """Initialize the FUSE filesystem with ACS client.
        
        Args:
            bucket_name (str): Name of the bucket to mount
        """
        logger.info(f"Initializing ACSFuse with bucket: {bucket_name}")
        start_time = time.time()
        
        # Get bucket region and create session with it
        temp_client = ACSClient(Session())
        
        client_start = time.time()
        bucket_info = temp_client.head_bucket(bucket_name)
        logger.info(f"head_bucket call completed in {time.time() - client_start:.4f} seconds")
        
        self.client = ACSClient(Session(region=bucket_info.region)) # Create client with bucket region
        self.bucket = bucket_name # Each mount is tied to one bucket
        self.buffers = {}  # Dictionary to store file buffers
        self.buffer_lock = Lock()  # Lock for thread-safe buffer access
        
        # Initialize read cache
        self.read_cache = ReadCache()

        # Verify bucket exists
        try:
            client_start = time.time()
            self.client.head_bucket(bucket_name)
            logger.info(f"Verification head_bucket call completed in {time.time() - client_start:.4f} seconds")
        except Exception as e:
            logger.error(f"Failed to access bucket {bucket_name}: {str(e)}")
            raise ValueError(f"Failed to access bucket {bucket_name}: {str(e)}")
            
        time_function("__init__", start_time)

    def _get_path(self, path):
        """Convert FUSE path to ACS key."""
        logger.debug(f"Converting path: {path}")
        start_time = time.time()
        result = path.lstrip('/')
        time_function("_get_path", start_time)
        return result

    def getattr(self, path, fh=None):
        """Get file attributes."""
        logger.info(f"getattr: {path}")
        start_time = time.time()
        
        now = datetime.now().timestamp()
        base_stat = {
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_atime': now,
            'st_mtime': now,
            'st_ctime': now,
        }

        if path == '/':
            time_function("getattr", start_time)
            return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}

        try:
            key = self._get_path(path)
            # First check if it's a directory by checking with trailing slash
            dir_key = key if key.endswith('/') else key + '/'
            try:
                client_start = time.time()
                # List objects with this prefix to check if it's a directory
                objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                logger.info(f"list_objects call for directory check {dir_key} completed in {time.time() - client_start:.4f} seconds")
                
                if objects:  # If we found any objects with this prefix, it's a directory
                    result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}
                    time_function("getattr", start_time)
                    return result
            except Exception as dir_e:
                logger.debug(f"Directory check failed for {dir_key}: {str(dir_e)}")

            # If not a directory, try as a regular file
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.info(f"head_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                
                # Regular file
                result = {**base_stat,
                        'st_mode': 0o100644,  # Regular file mode
                        'st_size': metadata.content_length,
                        'st_mtime': metadata.last_modified.timestamp(),
                        'st_nlink': 1}
                time_function("getattr", start_time)
                return result
            except Exception as e:
                if "NoSuchKey" in str(e):
                    logger.debug(f"Object {key} does not exist")
                else:
                    logger.error(f"Error checking file {key}: {str(e)}")
                time_function("getattr", start_time)
                raise FuseOSError(errno.ENOENT)
                
        except Exception as e:
            logger.info(f"getattr error for {path}: {str(e)}")
            time_function("getattr", start_time)
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        """List directory contents."""
        logger.info(f"readdir: {path}")
        start_time = time.time()
        
        try:
            prefix = self._get_path(path)
            if prefix and not prefix.endswith('/'):
                prefix += '/'

            entries = {'.', '..'}
            
            try:
                # Get all objects with prefix
                client_start = time.time()
                objects = self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=prefix)
                )
                logger.info(f"list_objects call for {prefix} completed in {time.time() - client_start:.4f} seconds")
                
                # Filter to get only immediate children
                seen = set()
                filtered_objects = []
                for obj in objects:
                    if not obj.startswith(prefix):
                        continue
                        
                    rel_path = obj[len(prefix):]
                    if not rel_path:
                        continue
                        
                    # Get first segment of remaining path
                    parts = rel_path.split('/')
                    if parts[0]:
                        seen.add(parts[0] + ('/' if len(parts) > 1 else ''))
                objects = list(seen)  # Convert filtered results back to list
                
                # Prepare entries
                for key in objects:
                    # Remove trailing slash for directory entries
                    if key.endswith('/'):
                        key = key[:-1]
                    entries.add(key)
                
                result = list(entries)
                time_function("readdir", start_time)
                return result

            except Exception as e:
                logger.error(f"Error in readdir list_objects: {str(e)}")
                result = list(entries)
                time_function("readdir", start_time)
                return result
                
        except Exception as e:
            logger.error(f"Error in readdir: {str(e)}")
            time_function("readdir", start_time)
            raise FuseOSError(errno.EIO)

    def rename(self, old, new):
        """Rename a file or directory."""
        logger.info(f"rename: {old} to {new}")
        start_time = time.time()
        
        old_key = self._get_path(old)
        new_key = self._get_path(new)

        try:
            # Get the object data for the source
            client_start = time.time()
            data = self.client.get_object(self.bucket, old_key)
            logger.info(f"get_object call for {old_key} completed in {time.time() - client_start:.4f} seconds")
        except Exception as e:
            logger.error(f"Error getting source object {old_key}: {str(e)}")
            time_function("rename", start_time)
            raise FuseOSError(errno.ENOENT)

        try:
            # Write data to the destination key
            client_start = time.time()
            self.client.put_object(self.bucket, new_key, data)
            logger.info(f"put_object call for {new_key} completed in {time.time() - client_start:.4f} seconds")
            
            # Delete the original object
            client_start = time.time()
            self.client.delete_object(self.bucket, old_key)
            logger.info(f"delete_object call for {old_key} completed in {time.time() - client_start:.4f} seconds")
            
            time_function("rename", start_time)
        except Exception as e:
            logger.error(f"Error in rename operation: {str(e)}")
            time_function("rename", start_time)
            raise FuseOSError(errno.EIO)
    
    def read(self, path, size, offset, fh):
        """Read file contents, checking cache first."""
        logger.info(f"read: {path}, size={size}, offset={offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Fast path: Check in-memory cache first
            cache_entry = self.read_cache.get(key)
            
            if cache_entry is None:
                # Cache miss - fetch from object storage
                logger.debug(f"Cache miss for {key}, fetching from object storage")
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    
                    # Store in cache
                    self.read_cache.put(key, data)
                except Exception as e:
                    logger.error(f"Error fetching {key} from object storage: {str(e)}")
                    raise
            else:
                logger.debug(f"Cache hit for {key}")
                data = cache_entry
            
            # Return requested portion from cache
            if offset >= len(data):
                logger.info(f"Offset {offset} is beyond file size {len(data)}, returning empty bytes")
                time_function("read", start_time)
                return b""
                
            end_offset = min(offset + size, len(data))
            result = data[offset:end_offset]
            
            time_function("read", start_time)
            return result
            
        except Exception as e:
            logger.error(f"Error reading {key}: {str(e)}")
            time_function("read", start_time)
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """Write data to an in-memory buffer, to be flushed on close."""
        logger.info(f"write: {path}, size={len(data)}, offset={offset}")
        start_time = time.time()
        
        try:
            with self.buffer_lock:
                key = self._get_path(path)
                if key not in self.buffers:
                    # Initialize buffer with existing data or empty
                    logger.info(f"Initializing buffer for {key}")
                    try:
                        client_start = time.time()
                        current_data = self.client.get_object(self.bucket, key)
                        logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    except:
                        logger.info(f"No existing data found for {key}, initializing empty buffer")
                        current_data = b""
                    self.buffers[key] = BytesIO(current_data)

                # Ensure buffer is large enough
                buffer = self.buffers[key]
                buffer.seek(0, 2)  # Seek to end
                if buffer.tell() < offset:
                    buffer.write(b'\x00' * (offset - buffer.tell()))
                    logger.debug(f"Extended buffer for {key} to offset {offset}")

                # Write data at offset
                buffer.seek(offset)
                buffer.write(data)
                logger.debug(f"Wrote {len(data)} bytes to buffer for {key} at offset {offset}")
                
                # Invalidate read cache entry since file has changed
                self.read_cache.remove(key)
                
                time_function("write", start_time)
                return len(data)
        except Exception as e:
            logger.error(f"Error writing to {key}: {str(e)}")
            time_function("write", start_time)
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """Create a new file."""
        logger.info(f"create: {path}, mode={mode}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Create empty object in Object Storage first
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.info(f"put_object call for {key} completed in {time.time() - client_start:.4f} seconds")

            # Initialize buffer
            with self.buffer_lock:
                self.buffers[key] = BytesIO()
                logger.debug(f"Initialized empty buffer for {key}")
                
            time_function("create", start_time)
            return 0
        except Exception as e:
            logger.error(f"Error creating {key}: {str(e)}")
            time_function("create", start_time)
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        """Delete a file if it exists."""
        logger.info(f"unlink: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            client_start = time.time()
            self.client.delete_object(self.bucket, key)
            logger.info(f"delete_object call for {key} completed in {time.time() - client_start:.4f} seconds")
            
            time_function("unlink", start_time)
        except Exception as e:
            logger.error(f"Error unlinking {key}: {str(e)}")
            time_function("unlink", start_time)
            raise FuseOSError(errno.EIO)

    def mkdir(self, path, mode):
        """Create a directory."""
        logger.info(f"mkdir: {path}, mode={mode}")
        start_time = time.time()
        
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.info(f"put_object call for directory {key} completed in {time.time() - client_start:.4f} seconds")
            
            time_function("mkdir", start_time)
        except Exception as e:
            logger.error(f"Error creating directory {key}: {str(e)}")
            time_function("mkdir", start_time)
            raise FuseOSError(errno.EIO)

    def rmdir(self, path):
        """Remove a directory."""
        logger.info(f"rmdir: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            # Check if directory is empty
            client_start = time.time()
            contents = list(self.client.list_objects(
                self.bucket,
                ListObjectsOptions(prefix=key, max_keys=2)
            ))
            logger.info(f"list_objects call for {key} completed in {time.time() - client_start:.4f} seconds")
            
            if len(contents) > 1:
                logger.warning(f"Directory {key} is not empty, cannot remove")
                time_function("rmdir", start_time)
                raise FuseOSError(errno.ENOTEMPTY)
                
            client_start = time.time()
            self.client.delete_object(self.bucket, key)
            logger.info(f"delete_object call for directory {key} completed in {time.time() - client_start:.4f} seconds")
            
            time_function("rmdir", start_time)
        except FuseOSError:
            # Re-raise FUSE errors
            raise
        except Exception as e:
            logger.error(f"Error removing directory {key}: {str(e)}")
            time_function("rmdir", start_time)
            raise FuseOSError(errno.EIO)
    
    def truncate(self, path, length, fh=None):
        """Truncate file to specified length."""
        logger.info(f"truncate: {path}, length={length}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            with self.buffer_lock:
                if key in self.buffers:
                    # Modify buffer if it exists
                    buffer = self.buffers[key]
                    buffer.seek(0)
                    data = buffer.read()
                    logger.debug(f"Using existing buffer for {key}")
                else:
                    # Read from ACS if no buffer exists
                    try:
                        client_start = time.time()
                        data = self.client.get_object(self.bucket, key)
                        logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    except:
                        logger.info(f"No existing data found for {key}, initializing empty buffer")
                        data = b""
                    self.buffers[key] = BytesIO()
                    logger.debug(f"Created new buffer for {key}")

                # Truncate data
                if length < len(data):
                    data = data[:length]
                    logger.debug(f"Truncated data to {length} bytes")
                elif length > len(data):
                    data += b'\x00' * (length - len(data))
                    logger.debug(f"Extended data to {length} bytes")

                # Update buffer
                buffer = self.buffers[key]
                buffer.seek(0)
                buffer.write(data)
                buffer.truncate()
                
            time_function("truncate", start_time)
        except Exception as e:
            logger.error(f"Error truncating {key}: {str(e)}")
            time_function("truncate", start_time)
            raise FuseOSError(errno.EIO)
        return 0

    def _flush_buffer(self, path):
        """Flush the in-memory buffer for a file to ACS storage."""
        logger.info(f"_flush_buffer: {path}")
        start_time = time.time()
        
        with self.buffer_lock:
            key = self._get_path(path)
            if key in self.buffers:
                # Get buffer data and write to ACS
                buffer = self.buffers[key]
                buffer.seek(0)
                data = buffer.read()
                try:
                    client_start = time.time()
                    self.client.put_object(self.bucket, key, data)
                    logger.info(f"put_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    
                    # Invalidate read cache entry since file has been updated
                    self.read_cache.remove(key)
                    
                    time_function("_flush_buffer", start_time)
                except Exception as e:
                    logger.error(f"Error flushing {key} to storage: {str(e)}")
                    time_function("_flush_buffer", start_time)
                    raise
            else:
                logger.debug(f"No buffer to flush for {key}")
                time_function("_flush_buffer", start_time)

    def release(self, path, fh):
        """Release the file handle and flush the write buffer to ACS storage."""
        logger.info(f"release: {path}")
        start_time = time.time()
        
        # Called after the last file descriptor is closed
        self._flush_buffer(path)
        key = self._get_path(path)
        
        with self.buffer_lock:
            if key in self.buffers:
                del self.buffers[key]
                logger.debug(f"Removed buffer for {key}")
        
        # Remove from read cache when file is no longer being accessed
        self.read_cache.remove(key)
        logger.debug(f"Removed {key} from read cache")
                
        time_function("release", start_time)
        return 0

    def link(self, target, name):
        """Create hard link by copying the object (since true hard links aren't supported in object storage)."""
        logger.info(f"link: target={target}, name={name}")
        start_time = time.time()
        
        try:
            target_key = self._get_path(target)
            new_key = self._get_path(name)
            
            # First verify target exists
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, target_key)
                logger.info(f"head_object call for {target_key} completed in {time.time() - client_start:.4f} seconds")
            except Exception as e:
                logger.error(f"Target object {target_key} does not exist: {str(e)}")
                raise FuseOSError(errno.ENOENT)
            
            # Get the source object data
            client_start = time.time()
            data = self.client.get_object(self.bucket, target_key)
            logger.info(f"get_object call for {target_key} completed in {time.time() - client_start:.4f} seconds")
            
            # Create the new object with the same data
            client_start = time.time()
            self.client.put_object(self.bucket, new_key, data)
            logger.info(f"put_object call for {new_key} completed in {time.time() - client_start:.4f} seconds")
            
            time_function("link", start_time)
            return 0
        except Exception as e:
            logger.error(f"Error creating link from {target} to {name}: {str(e)}")
            time_function("link", start_time)
            raise FuseOSError(errno.EIO)

    def flock(self, path, op, fh):
        """File locking operation (implemented as a no-op since object storage doesn't support file locking)."""
        logger.info(f"flock: {path}, op={op}")
        start_time = time.time()
        
        # This is a no-op operation since object storage doesn't support file locking
        # Always return success regardless of the operation requested
        time_function("flock", start_time)
        return 0

def unmount(mountpoint):
    """Unmount the filesystem using fusermount (Linux)."""
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
            
        # Clear all caches before unmounting
        fuse_ops = next((fuse for fuse in FUSE._active_fuseops if isinstance(fuse, ACSFuse)), None)
        if fuse_ops:
            fuse_ops.read_cache.clear()
            logger.info("Cleared read cache")
            
        subprocess.run(["fusermount", "-u", mountpoint], check=True)
        logger.info(f"Unmounted {mountpoint} gracefully.")
        print(f"Unmounted {mountpoint} gracefully.")
        time_function("unmount", start_time)
    except Exception as e:
        logger.error(f"Error during unmounting: {e}")
        print(f"Error during unmounting: {e}")
        time_function("unmount", start_time)

def mount(bucket: str, mountpoint: str, foreground: bool = True):
    """Mount an ACS bucket at the specified mountpoint.
    
    Args:
        bucket (str): Name of the bucket to mount
        mountpoint (str): Local path where the filesystem should be mounted
        foreground (bool, optional): Run in foreground. Defaults to True.
    """
    logger.info(f"Mounting bucket {bucket} at {mountpoint}")
    start_time = time.time()
    
    os.environ["GRPC_VERBOSITY"] = "ERROR"
    options = {
        'foreground': foreground,
        'nonempty': True,
        'debug': False,
        'default_permissions': True,
        'direct_io': False,  
        'rw': True,
        'big_writes': True,
        'max_read': 1024 * 1024 * 1024,  # 1GB read size
        'max_write': 1024 * 1024 * 1024,  # 1GB write size
        'kernel_cache': True,  # Enable kernel caching
        'auto_cache': True,   # Enable automatic cache management
        'max_readahead': 1024 * 1024 * 1024,  # 1GB readahead
    }

    # Define a signal handler to catch SIGINT and SIGTERM
    def signal_handler(sig, frame):
        logger.info(f"Signal {sig} received, unmounting...")
        print("Signal received, unmounting...")
        unmount(mountpoint)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logger.info(f"Starting FUSE mount with options: {options}")
        mount_start = time.time()
        FUSE(ACSFuse(bucket), mountpoint, **options)
        logger.info(f"FUSE mount completed in {time.time() - mount_start:.4f} seconds")
        time_function("mount", start_time)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, unmounting...")
        print("Keyboard interrupt received, unmounting...")
        unmount(mountpoint)
        time_function("mount", start_time)
    except Exception as e:
        logger.error(f"Error during mount: {e}")
        print(f"Error: {e}")
        unmount(mountpoint)
        time_function("mount", start_time)

def main():
    """CLI entry point for mounting ACS buckets."""
    logger.info(f"Starting ACS FUSE CLI with arguments: {sys.argv}")
    start_time = time.time()
    
    if len(sys.argv) != 3:
        logger.error(f"Invalid arguments: {sys.argv}")
        print(f"Usage: {sys.argv[0]} <bucket> <mountpoint>")
        time_function("main", start_time)
        sys.exit(1)

    bucket = sys.argv[1]
    mountpoint = sys.argv[2]
    logger.info(f"Mounting bucket {bucket} at {mountpoint}")
    mount(bucket, mountpoint)
    time_function("main", start_time)

if __name__ == '__main__':
    main()
