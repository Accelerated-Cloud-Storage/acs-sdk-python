# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
#!/usr/bin/env python3
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
        self.bucket = bucket_name
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
        start_time = time.perf_counter()
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
                duration = time.perf_counter() - start_time
                print(f"Found object {key} in {duration:.6f} seconds")
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
                duration = time.perf_counter() - start_time
                print(f"Found directory {dir_key} in {duration:.6f} seconds")
                return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}
                
        except Exception as e:
            duration = time.perf_counter() - start_time
            print(f"Error getting attributes for {path}: {str(e)} in {duration:.6f} seconds")
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        """List directory contents."""
        start_time = time.perf_counter()
        try:
            prefix = self._get_path(path)
            if prefix and not prefix.endswith('/'):
                prefix += '/'

            entries = {'.', '..'}
            print(f"Listing directory {prefix}")
            
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
                print(f"Filtered to {len(objects)} objects", objects)
                
                for key in objects:
                    print(f"Processing key {key}")
                    # Remove trailing slash for directory entries
                    if key.endswith('/'):
                        key = key[:-1]
                    entries.add(key)

                duration = time.perf_counter() - start_time
                print(f"Found {len(entries)} entries in {duration:.6f} seconds", entries)
                return list(entries)
            except Exception as e:
                print(f"Error listing objects: {str(e)}")
                return list(entries)
                
        except Exception as e:
            print(f"Fatal error in readdir: {str(e)}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            raise FuseOSError(errno.EIO)

    def rename(self, old, new):
        """Rename a file or directory."""
        old_key = self._get_path(old)
        new_key = self._get_path(new)
        try:
            # Get the object data for the source
            data = self.client.get_object(self.bucket, old_key)
        except Exception as e:
            print(f"Error reading {old_key}: {str(e)}")
            raise FuseOSError(errno.ENOENT)
        try:
            # Write data to the destination key
            self.client.put_object(self.bucket, new_key, data)
            # Delete the original object
            self.client.delete_object(self.bucket, old_key)
            print(f"Renamed {old_key} to {new_key}")
        except Exception as e:
            print(f"Error renaming {old_key} to {new_key}: {str(e)}")
            raise FuseOSError(errno.EIO)
    
    def read(self, path, size, offset, fh):
        """Read file contents, checking buffer first."""
        key = self._get_path(path)
        try:
            with self.buffer_lock:
                if key in self.buffers:
                    # Read from buffer if it exists
                    buffer = self.buffers[key]
                    buffer.seek(offset)
                    return buffer.read(size)

            # Fall back to reading from ACS
            data = self.client.get_object(self.bucket, key)
            print(f"Read {len(data)} bytes from {key} at offset {offset}")
            return data[offset:offset + size]
        except Exception as e:
            print(f"Read error for {path}: {str(e)}")
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """Write file contents to buffer."""
        start_time = time.perf_counter()
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
                duration = time.perf_counter() - start_time
                print(f"Buffered {len(data)} bytes to {key} at offset {offset} in {duration:.6f} seconds")
                return len(data)
        except Exception as e:
            print(f"Write error for {path}: {str(e)}")
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """Create a new file."""
        start_time = time.perf_counter()
        key = self._get_path(path)
        try:
            # Create empty object in ACS first
            self.client.put_object(self.bucket, key, b"")
            # Initialize buffer
            with self.buffer_lock:
                self.buffers[key] = BytesIO()
            duration = time.perf_counter() - start_time
            print(f"Created file {key} in {duration:.6f} seconds")
            return 0
        except Exception as e:
            print(f"Create error for {path}: {str(e)}")
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        """Delete a file and its buffer if it exists."""
        key = self._get_path(path)
        with self.buffer_lock:
            if key in self.buffers:
                del self.buffers[key]
        try:
            self.client.delete_object(self.bucket, key)
        except:
            raise FuseOSError(errno.EIO)
        print(f"Deleted file {key}")

    def mkdir(self, path, mode):
        """Create a directory."""
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
        self.client.put_object(self.bucket, key, b"")
        print(f"Created directory {key}")

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
        print(f"Deleted directory {key}")
    
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
            print(f"Truncate error for {path}: {str(e)}")
            raise FuseOSError(errno.EIO)
        return 0

    def _flush_buffer(self, path):
        """Flush the in-memory buffer for a file to ACS storage."""
        start_time = time.perf_counter()
        with self.buffer_lock:
            key = self._get_path(path)
            if key in self.buffers:
                buffer = self.buffers[key]
                buffer.seek(0)
                data = buffer.read()
                try:
                    self.client.put_object(self.bucket, key, data)
                    duration = time.perf_counter() - start_time
                    print(f"Flushed buffer for {path} to ACS storage in {duration:.6f} seconds")
                except Exception as e:
                    print(f"Error flushing buffer for {path}: {str(e)}")
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
    FUSE(ACSFuse(bucket), mountpoint, **options)

def main():
    """CLI entry point for mounting ACS buckets."""
    import sys
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <bucket> <mountpoint>")
        sys.exit(1)

    bucket = sys.argv[1]
    mountpoint = sys.argv[2]
    mount(bucket, mountpoint)

if __name__ == '__main__':
    main()
