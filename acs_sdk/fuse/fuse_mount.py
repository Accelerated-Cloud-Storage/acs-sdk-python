# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
from fuse import FUSE, FuseOSError, Operations
import errno
import os
import sys 
import time 
from datetime import datetime
from acs_sdk.client.client import ACSClient
from acs_sdk.client.types import ListObjectsOptions
from io import BytesIO
from threading import Lock

class ACSFuse(Operations):
    """FUSE implementation for Accelerated Cloud Storage."""

    def __init__(self, bucket_name):
        """Initialize the FUSE filesystem with ACS client.
        
        Args:
            bucket_name (str): Name of the bucket to mount
        """
        self.client = ACSClient()
        self.bucket = bucket_name # Each mount is tied to one bucket
        self.buffers = {}  # Dictionary to store file buffers
        self.buffer_lock = Lock()  # Lock for thread-safe buffer access

        # Verify bucket exists
        try:
            self.client.head_bucket(bucket_name)
        except Exception as e:
            raise ValueError(f"Failed to access bucket {bucket_name}: {str(e)}")

    def _get_path(self, path):
        """Convert FUSE path to ACS key."""
        return path.lstrip('/')

    def getattr(self, path, fh=None):
        """Get file attributes."""
        now = datetime.now().timestamp()
        base_stat = {
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_atime': now,
            'st_mtime': now,
            'st_ctime': now,
        }

        if path == '/':
            return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}

        try:
            key = self._get_path(path)
            
            # First try to get object metadata directly
            try:
                metadata = self.client.head_object(self.bucket, key)
                return {**base_stat,
                        'st_mode': 0o100644,
                        'st_size': metadata.content_length,
                        'st_mtime': metadata.last_modified.timestamp(),
                        'st_nlink': 1}
            except:
                # If not found as a file, check if it's a directory
                dir_key = key if key.endswith('/') else key + '/'
                # Check for directory by getting metadata
                metadata = self.client.head_object(self.bucket, dir_key)
                return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}
                
        except Exception as e:
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        """List directory contents."""
        try:
            prefix = self._get_path(path)
            if prefix and not prefix.endswith('/'):
                prefix += '/'

            entries = {'.', '..'}
            
            try:
                # Get all objects with prefix
                objects = self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=prefix)
                )
                
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
                
                for key in objects:
                    # Remove trailing slash for directory entries
                    if key.endswith('/'):
                        key = key[:-1]
                    entries.add(key)

                return list(entries)
            except Exception as e:
                return list(entries)
                
        except Exception as e:
            raise FuseOSError(errno.EIO)

    def rename(self, old, new):
        """Rename a file or directory."""
        old_key = self._get_path(old)
        new_key = self._get_path(new)
        try:
            # Get the object data for the source
            data = self.client.get_object(self.bucket, old_key)
        except Exception as e:
            raise FuseOSError(errno.ENOENT)
        try:
            # Write data to the destination key
            self.client.put_object(self.bucket, new_key, data)
            # Delete the original object
            self.client.delete_object(self.bucket, old_key)
        except Exception as e:
            raise FuseOSError(errno.EIO)
    
    def read(self, path, size, offset, fh):
        """Read file contents, checking buffer first."""
        key = self._get_path(path)
        try:
            # Read from ACS
            data = self.client.get_object(self.bucket, key)
            return data[offset:offset + size]
        except Exception as e:
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """Write file contents to buffer."""
        key = self._get_path(path)
        try:
            with self.buffer_lock:
                if key not in self.buffers:
                    # Initialize buffer with existing content if file exists
                    try:
                        current_data = self.client.get_object(self.bucket, key)
                    except:
                        current_data = b""
                    self.buffers[key] = BytesIO(current_data)

                # Ensure buffer is large enough
                buffer = self.buffers[key]
                buffer.seek(0, 2)  # Seek to end
                if buffer.tell() < offset:
                    buffer.write(b'\x00' * (offset - buffer.tell()))

                # Write data at offset
                buffer.seek(offset)
                buffer.write(data)
                return len(data)
        except Exception as e:
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """Create a new file."""
        key = self._get_path(path)
        try:
            # Create empty object in Object Storage first
            self.client.put_object(self.bucket, key, b"")
            # Initialize buffer
            with self.buffer_lock:
                self.buffers[key] = BytesIO()
            return 0
        except Exception as e:
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        """Delete a file if it exists."""
        key = self._get_path(path)
        try:
            self.client.delete_object(self.bucket, key)
        except:
            raise FuseOSError(errno.EIO)

    def mkdir(self, path, mode):
        """Create a directory."""
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
        self.client.put_object(self.bucket, key, b"")

    def rmdir(self, path):
        """Remove a directory."""
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        # Check if directory is empty
        contents = list(self.client.list_objects(
            self.bucket,
            ListObjectsOptions(prefix=key, max_keys=2)
        ))
        if len(contents) > 1:
            raise FuseOSError(errno.ENOTEMPTY)
            
        self.client.delete_object(self.bucket, key)
    
    def truncate(self, path, length, fh=None):
        """Truncate file to specified length."""
        key = self._get_path(path)
        try:
            with self.buffer_lock:
                if key in self.buffers:
                    # Modify buffer if it exists
                    buffer = self.buffers[key]
                    buffer.seek(0)
                    data = buffer.read()
                else:
                    # Read from ACS if no buffer exists
                    try:
                        data = self.client.get_object(self.bucket, key)
                    except:
                        data = b""
                    self.buffers[key] = BytesIO()

                # Truncate data
                if length < len(data):
                    data = data[:length]
                elif length > len(data):
                    data += b'\x00' * (length - len(data))

                # Update buffer
                buffer = self.buffers[key]
                buffer.seek(0)
                buffer.write(data)
                buffer.truncate()
        except Exception as e:
            raise FuseOSError(errno.EIO)
        return 0

    def _flush_buffer(self, path):
        """Flush the in-memory buffer for a file to ACS storage."""
        with self.buffer_lock:
            key = self._get_path(path)
            if key in self.buffers:
                buffer = self.buffers[key]
                buffer.seek(0)
                data = buffer.read()
                try:
                    self.client.put_object(self.bucket, key, data)
                except Exception as e:
                    raise FuseOSError(errno.EIO)

    def release(self, path, fh):
        """Release the file handle and flush the write buffer to ACS storage."""
        self._flush_buffer(path)
        with self.buffer_lock:
            key = self._get_path(path)
            if key in self.buffers:
                del self.buffers[key]
        return 0

def mount(bucket: str, mountpoint: str, foreground: bool = True):
    """Mount an ACS bucket at the specified mountpoint.
    
    Args:
        bucket (str): Name of the bucket to mount
        mountpoint (str): Local path where the filesystem should be mounted
        foreground (bool, optional): Run in foreground. Defaults to True.
    """
    """Mount an ACS bucket at the specified mountpoint."""
    os.environ["GRPC_VERBOSITY"] = "ERROR"
    options = {
        'foreground': foreground,
        'nonempty': True,
        'debug': False,
        'default_permissions': True,
        'direct_io': True,
        'rw': True,
        'big_writes': True,
        'max_read': 100 * 1024 * 1024,  # 100 MB 
    }
    FUSE(ACSFuse(bucket), mountpoint, **options)

def main():
    """CLI entry point for mounting ACS buckets."""
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <bucket> <mountpoint>")
        sys.exit(1)

    bucket = sys.argv[1]
    mountpoint = sys.argv[2]
    mount(bucket, mountpoint)

if __name__ == '__main__':
    main()
