#!/usr/bin/env python3

from fuse import FUSE, FuseOSError, Operations
import errno
import os
import sys 
from datetime import datetime
from ..client.client import ACSClient
from ..client.types import ListObjectsOptions

class ACSFuse(Operations):
    """FUSE implementation for Accelerated Cloud Storage."""

    def __init__(self, bucket_name):
        """Initialize the FUSE filesystem with ACS client.
        
        Args:
            bucket_name (str): Name of the bucket to mount
        """
        self.client = ACSClient()
        self.bucket = bucket_name
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
                print(f"Found object {key}")
                return {**base_stat,
                        'st_mode': 0o100644,
                        'st_size': metadata.content_length,
                        'st_mtime': metadata.last_modified.timestamp(),
                        'st_nlink': 1}
            except:
                # If not found as a file, check if it's a directory
                dir_key = key if key.endswith('/') else key + '/'
                # Get objects with prefix and filter for directory-like entries
                all_objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                # Filter to only include objects that start with dir_key
                objects = [obj for obj in all_objects if obj.startswith(dir_key)]
                if objects:
                    print(f"Found directory {dir_key}")
                    return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2}

                print(f"Path not found: {path}")
                raise FuseOSError(errno.ENOENT)
                
        except Exception as e:
            print(f"Error getting attributes for {path}: {str(e)}")
            raise FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        """List directory contents."""
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

                print(f"Found {len(entries)} entries", entries)
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
        """Read file contents."""
        key = self._get_path(path)
        try:
            data = self.client.get_object(self.bucket, key)
            print(f"Read {len(data)} bytes from {key} at offset {offset}")
            return data[offset:offset + size]
        except:
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """Write file contents."""
        key = self._get_path(path)
        try:
            # Read existing data
            try:
                current_data = self.client.get_object(self.bucket, key)
            except:
                current_data = b""

            # Extend if needed
            if len(current_data) < offset:
                current_data += b'\x00' * (offset - len(current_data))

            # Combine data
            new_data = current_data[:offset] + data
            if offset + len(data) < len(current_data):
                new_data += current_data[offset + len(data):]

            # Write back
            self.client.put_object(self.bucket, key, new_data)
            print(f"Wrote {len(data)} bytes to {key} at offset {offset}")
            return len(data)
        except:
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """Create a new file."""
        key = self._get_path(path)
        self.client.put_object(self.bucket, key, b"")
        print(f"Created file {key}")
        return 0

    def unlink(self, path):
        """Delete a file."""
        key = self._get_path(path)
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
        try:
            key = self._get_path(path)
            try:
                data = self.client.get_object(self.bucket, key)
            except:
                data = b""
                
            if length < len(data):
                data = data[:length]
            elif length > len(data):
                data += b'\x00' * (length - len(data))
                
            self.client.put_object(self.bucket, key, data)
        except Exception as e:
            print(f"Truncate error for {path}: {str(e)}")
            raise FuseOSError(errno.EIO)

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
