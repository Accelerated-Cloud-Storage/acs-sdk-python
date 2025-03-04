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
from threading import Lock
import subprocess
import signal
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ACSFuse')

# Helper function for timing operations
def time_function(func_name, start_time):
    elapsed = time.time() - start_time
    logger.info(f"{func_name} completed in {elapsed:.4f} seconds")
    return elapsed

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
            # First try to get object metadata directly
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.info(f"head_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                
                result = {**base_stat,
                        'st_mode': 0o100644,
                        'st_size': metadata.content_length,
                        'st_mtime': metadata.last_modified.timestamp(),
                        'st_nlink': 1}
                time_function("getattr", start_time)
                return result
            except:
                # If not found as a file, check if it's a directory
                dir_key = key if key.endswith('/') else key + '/'
                # Check for directory by getting metadata
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, dir_key)
                logger.info(f"head_object call for directory {dir_key} completed in {time.time() - client_start:.4f} seconds")
                
                result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}
                time_function("getattr", start_time)
                return result
                
        except Exception as e:
            logger.error(f"getattr error for {path}: {str(e)}")
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
        """Read file contents, checking buffer first."""
        logger.info(f"read: {path}, size={size}, offset={offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # First, get the file size using head_object
            client_start = time.time()
            head_response = self.client.head_object(self.bucket, key)
            logger.info(f"head_object call for {key} completed in {time.time() - client_start:.4f} seconds")
            
            file_size = head_response.content_length
            
            # If offset is beyond the end of the file, return empty bytes
            if offset >= file_size:
                logger.info(f"Offset {offset} is beyond file size {file_size}, returning empty bytes")
                time_function("read", start_time)
                return b""
            
            # Adjust the size if it would read beyond the end of the file
            if offset + size > file_size:
                size = file_size - offset
                logger.info(f"Adjusted read size to {size} bytes")
            
            # Read from Object Storage with adjusted range
            client_start = time.time()
            data = self.client.get_object(self.bucket, key, byte_range=f"bytes={offset}-{offset + size - 1}")
            logger.info(f"get_object call for {key} (range: bytes={offset}-{offset + size - 1}) completed in {time.time() - client_start:.4f} seconds")
            
            time_function("read", start_time)
            return data
        except Exception as e:
            logger.error(f"Error reading {key}: {str(e)}")
            time_function("read", start_time)
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """Write file contents to buffer."""
        logger.info(f"write: {path}, data_size={len(data)}, offset={offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            with self.buffer_lock:
                if key not in self.buffers:
                    # Initialize buffer with existing content if file exists
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
                    
                    time_function("_flush_buffer", start_time)
                except Exception as e:
                    logger.error(f"Error flushing buffer for {key}: {str(e)}")
                    time_function("_flush_buffer", start_time)
                    raise FuseOSError(errno.EIO)
            else:
                logger.debug(f"No buffer to flush for {path}")
                time_function("_flush_buffer", start_time)

    def release(self, path, fh):
        """Release the file handle and flush the write buffer to ACS storage."""
        logger.info(f"release: {path}")
        start_time = time.time()
        
        # Called after the last file descriptor is closed
        self._flush_buffer(path)
        with self.buffer_lock:
            key = self._get_path(path)
            if key in self.buffers:
                del self.buffers[key]
                logger.debug(f"Removed buffer for {key}")
                
        time_function("release", start_time)
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
        'debug': True,
        'default_permissions': True,
        'direct_io': True,
        'rw': True,
        'big_writes': True,
        'max_read': 100 * 1024 * 1024,  # 100 MB 
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
