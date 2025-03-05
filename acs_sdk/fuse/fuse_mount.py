# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
FUSE implementation for ACS.

This module provides the core FUSE implementation for mounting ACS buckets
as local filesystems. It handles file operations like read, write, create,
and delete by translating them to ACS API calls.

Usage:
    # Create a mount point
    mkdir -p /mnt/acs-bucket

    # Mount the bucket
    python -m acs_sdk.fuse my-bucket /mnt/acs-bucket

    # Now you can work with the files as if they were local
    ls /mnt/acs-bucket
    cat /mnt/acs-bucket/example.txt
"""

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

# Import from our new modules
from .utils import logger, time_function
from .buffer import ReadBuffer, BufferEntry, WriteBuffer, calculate_ttl
from .mount_utils import unmount, setup_signal_handlers, get_mount_options

class ACSFuse(Operations):
    """
    FUSE implementation for Accelerated Cloud Storage.
    
    This class implements the FUSE operations interface to provide
    filesystem access to ACS buckets. It handles file operations by
    translating them to ACS API calls and manages buffers for efficient
    read and write operations.
    
    Attributes:
        client (ACSClient): Client for ACS API calls
        bucket (str): Name of the bucket being mounted
        read_buffer (ReadBuffer): Buffer for read operations
        write_buffer (WriteBuffer): Buffer for write operations
    """

    def __init__(self, bucket_name):
        """
        Initialize the FUSE filesystem with ACS client.
        
        Args:
            bucket_name (str): Name of the bucket to mount
        
        Raises:
            ValueError: If the bucket cannot be accessed
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
        
        # Initialize buffers
        self.read_buffer = ReadBuffer()
        self.write_buffer = WriteBuffer()

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
        """
        Convert FUSE path to ACS key.
        
        Args:
            path (str): FUSE path
            
        Returns:
            str: ACS object key
        """
        logger.debug(f"Converting path: {path}")
        start_time = time.time()
        result = path.lstrip('/')
        time_function("_get_path", start_time)
        return result

    def getattr(self, path, fh=None):
        """
        Get file attributes.
        
        This method returns the attributes of a file or directory,
        such as size, permissions, and modification time.
        
        Args:
            path (str): Path to the file or directory
            fh (int, optional): File handle. Defaults to None.
            
        Returns:
            dict: File attributes
            
        Raises:
            FuseOSError: If the file or directory does not exist
        """
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
        """
        List directory contents.
        
        This method returns the contents of a directory, including
        files and subdirectories.
        
        Args:
            path (str): Path to the directory
            fh (int): File handle
            
        Returns:
            list: List of directory entries
            
        Raises:
            FuseOSError: If an error occurs while listing the directory
        """
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
        """
        Rename a file or directory.
        
        This method renames a file or directory by copying the object
        to a new key and deleting the old one.
        
        Args:
            old (str): Old path
            new (str): New path
            
        Raises:
            FuseOSError: If the source file does not exist or an error occurs
        """
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
        """
        Read file contents, checking buffer first.
        
        This method reads data from a file, first checking the read buffer
        and falling back to object storage if necessary.
        
        Args:
            path (str): Path to the file
            size (int): Number of bytes to read
            offset (int): Offset in the file to start reading from
            fh (int): File handle
            
        Returns:
            bytes: The requested data
            
        Raises:
            FuseOSError: If an error occurs while reading the file
        """
        logger.info(f"read: {path}, size={size}, offset={offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Fast path: Check in-memory buffer first
            buffer_entry = self.read_buffer.get(key)
            
            if buffer_entry is None:
                # Buffer miss - fetch from object storage
                logger.debug(f"Buffer miss for {key}, fetching from object storage")
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    
                    # Store in buffer
                    self.read_buffer.put(key, data)
                except Exception as e:
                    logger.error(f"Error fetching {key} from object storage: {str(e)}")
                    raise
            else:
                logger.debug(f"Buffer hit for {key}")
                data = buffer_entry
            
            # Return requested portion from buffer
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
        """
        Write data to an in-memory buffer, to be flushed on close.
        
        This method writes data to a file by storing it in a write buffer,
        which will be flushed to object storage when the file is closed.
        
        Args:
            path (str): Path to the file
            data (bytes): Data to write
            offset (int): Offset in the file to start writing at
            fh (int): File handle
            
        Returns:
            int: Number of bytes written
            
        Raises:
            FuseOSError: If an error occurs while writing the file
        """
        logger.info(f"write: {path}, size={len(data)}, offset={offset}")
        start_time = time.time()
        
        try:
            key = self._get_path(path)
            
            # Initialize buffer if it doesn't exist
            if not self.write_buffer.has_buffer(key):
                logger.info(f"Initializing buffer for {key}")
                try:
                    client_start = time.time()
                    current_data = self.client.get_object(self.bucket, key)
                    logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                except:
                    logger.info(f"No existing data found for {key}, initializing empty buffer")
                    current_data = b""
                self.write_buffer.initialize_buffer(key, current_data)

            # Write data to buffer
            bytes_written = self.write_buffer.write(key, data, offset)
            
            # Invalidate read buffer entry since file has changed
            self.read_buffer.remove(key)
            
            time_function("write", start_time)
            return bytes_written
            
        except Exception as e:
            logger.error(f"Error writing to {path}: {str(e)}")
            time_function("write", start_time)
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """
        Create a new file.
        
        This method creates a new empty file in the object storage
        and initializes a write buffer for it.
        
        Args:
            path (str): Path to the file
            mode (int): File mode
            fi (dict, optional): File info. Defaults to None.
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If an error occurs while creating the file
        """
        logger.info(f"create: {path}, mode={mode}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Create empty object in Object Storage first
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.info(f"put_object call for {key} completed in {time.time() - client_start:.4f} seconds")

            # Initialize buffer
            self.write_buffer.initialize_buffer(key)
            logger.debug(f"Initialized empty buffer for {key}")
                
            time_function("create", start_time)
            return 0
        except Exception as e:
            logger.error(f"Error creating {key}: {str(e)}")
            time_function("create", start_time)
            raise FuseOSError(errno.EIO)

    def unlink(self, path):
        """
        Delete a file if it exists.
        
        This method deletes a file from the object storage.
        
        Args:
            path (str): Path to the file
            
        Raises:
            FuseOSError: If an error occurs while deleting the file
        """
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
        """
        Create a directory.
        
        This method creates a directory by creating an empty object
        with a trailing slash in the key.
        
        Args:
            path (str): Path to the directory
            mode (int): Directory mode
            
        Raises:
            FuseOSError: If an error occurs while creating the directory
        """
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
        """
        Remove a directory.
        
        This method removes a directory if it is empty.
        
        Args:
            path (str): Path to the directory
            
        Raises:
            FuseOSError: If the directory is not empty or an error occurs
        """
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
        """
        Truncate file to specified length.
        
        This method changes the size of a file by either truncating it
        or extending it with null bytes.
        
        Args:
            path (str): Path to the file
            length (int): New length of the file
            fh (int, optional): File handle. Defaults to None.
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If an error occurs while truncating the file
        """
        logger.info(f"truncate: {path}, length={length}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # If buffer doesn't exist, initialize it with existing data
            if not self.write_buffer.has_buffer(key):
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                except:
                    logger.info(f"No existing data found for {key}, initializing empty buffer")
                    data = b""
                self.write_buffer.initialize_buffer(key, data)
            
            # Truncate the buffer
            self.write_buffer.truncate(key, length)
            
            # Invalidate read buffer
            self.read_buffer.remove(key)
            
            time_function("truncate", start_time)
        except Exception as e:
            logger.error(f"Error truncating {key}: {str(e)}")
            time_function("truncate", start_time)
            raise FuseOSError(errno.EIO)
        return 0

    def _flush_buffer(self, path):
        """
        Flush the in-memory buffer for a file to ACS storage.
        
        This method writes the contents of the write buffer to object storage.
        
        Args:
            path (str): Path to the file
            
        Raises:
            Exception: If an error occurs while flushing the buffer
        """
        logger.info(f"_flush_buffer: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        # Check if there's a buffer to flush
        data = self.write_buffer.read(key)
        if data is not None:
            try:
                client_start = time.time()
                self.client.put_object(self.bucket, key, data)
                logger.info(f"put_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                
                # Invalidate read buffer entry since file has been updated
                self.read_buffer.remove(key)
                
                time_function("_flush_buffer", start_time)
            except Exception as e:
                logger.error(f"Error flushing {key} to storage: {str(e)}")
                time_function("_flush_buffer", start_time)
                raise
        else:
            logger.debug(f"No buffer to flush for {key}")
            time_function("_flush_buffer", start_time)

    def release(self, path, fh):
        """
        Release the file handle and flush the write buffer to ACS storage.
        
        This method is called when a file is closed. It flushes the write buffer
        to object storage and removes the file from both buffers.
        
        Args:
            path (str): Path to the file
            fh (int): File handle
            
        Returns:
            int: 0 on success
        """
        logger.info(f"release: {path}")
        start_time = time.time()
        
        # Called after the last file descriptor is closed
        self._flush_buffer(path)
        key = self._get_path(path)
        
        # Remove from write buffer
        self.write_buffer.remove(key)
        
        # Remove from read buffer when file is no longer being accessed
        self.read_buffer.remove(key)
        logger.debug(f"Removed {key} from read buffer")
                
        time_function("release", start_time)
        return 0

    def link(self, target, name):
        """
        Create hard link by copying the object.
        
        This method creates a hard link by copying the object to a new key,
        since true hard links aren't supported in object storage.
        
        Args:
            target (str): Path to the target file
            name (str): Path to the new link
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If the target file does not exist or an error occurs
        """
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
        """
        File locking operation (implemented as a no-op).
        
        This method is a no-op since object storage doesn't support file locking.
        
        Args:
            path (str): Path to the file
            op (int): Lock operation
            fh (int): File handle
            
        Returns:
            int: 0 (always succeeds)
        """
        logger.info(f"flock: {path}, op={op}")
        start_time = time.time()
        
        # This is a no-op operation since object storage doesn't support file locking
        # Always return success regardless of the operation requested
        time_function("flock", start_time)
        return 0

def mount(bucket: str, mountpoint: str, foreground: bool = True):
    """
    Mount an ACS bucket at the specified mountpoint.
    
    This function mounts an ACS bucket as a local filesystem using FUSE.
    
    Args:
        bucket (str): Name of the bucket to mount
        mountpoint (str): Local path where the filesystem should be mounted
        foreground (bool, optional): Run in foreground. Defaults to True.
    """
    logger.info(f"Mounting bucket {bucket} at {mountpoint}")
    start_time = time.time()
    
    os.environ["GRPC_VERBOSITY"] = "ERROR"
    options = get_mount_options(foreground)

    # Set up signal handlers for graceful unmounting
    signal_handler = setup_signal_handlers(mountpoint, lambda mp: unmount(mp, ACSFuse))

    try:
        logger.info(f"Starting FUSE mount with options: {options}")
        mount_start = time.time()
        FUSE(ACSFuse(bucket), mountpoint, **options)
        logger.info(f"FUSE mount completed in {time.time() - mount_start:.4f} seconds")
        time_function("mount", start_time)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, unmounting...")
        print("Keyboard interrupt received, unmounting...")
        unmount(mountpoint, ACSFuse)
        time_function("mount", start_time)
    except Exception as e:
        logger.error(f"Error during mount: {e}")
        print(f"Error: {e}")
        unmount(mountpoint, ACSFuse)
        time_function("mount", start_time)

def main():
    """
    CLI entry point for mounting ACS buckets.
    
    This function is the entry point for the command-line interface.
    It parses command-line arguments and mounts the specified bucket.
    
    Usage:
        python -m acs_sdk.fuse <bucket> <mountpoint>
    """
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
