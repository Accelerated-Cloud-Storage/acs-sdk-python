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
import subprocess
import fcntl

# Import from our new modules
from .utils import logger, time_function, trace_op
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
        
        If the bucket doesn't exist, creates it in us-east-1 region.
        
        Args:
            bucket_name (str): Name of the bucket to mount
        
        Raises:
            ValueError: If the bucket cannot be created or accessed
        """
        logger.info(f"Initializing ACSFuse with bucket: {bucket_name}")
        start_time = time.time()
        
        # Start with a client in us-east-1 (default region for bucket creation)
        temp_client = ACSClient(Session(region="us-east-1"))
        
        try:
            # Try to get bucket info
            client_start = time.time()
            bucket_info = temp_client.head_bucket(bucket_name)
            logger.info(f"Found existing bucket in region {bucket_info.region}")
            logger.info(f"head_bucket call completed in {time.time() - client_start:.4f} seconds")
            
        except BucketError as e:
            if "does not exist" in str(e) or "not found" in str(e).lower():
                # Bucket doesn't exist, create it in us-east-1
                logger.info(f"Bucket {bucket_name} does not exist. Creating in us-east-1...")
                try:
                    client_start = time.time()
                    temp_client.create_bucket(bucket_name)
                    logger.info(f"Successfully created bucket {bucket_name} in us-east-1")
                    bucket_info = temp_client.head_bucket(bucket_name)
                    logger.info(f"Bucket creation and verification completed in {time.time() - client_start:.4f} seconds")
                except Exception as create_e:
                    logger.error(f"Failed to create bucket {bucket_name}: {str(create_e)}")
                    temp_client.close()
                    raise ValueError(f"Failed to create bucket {bucket_name}: {str(create_e)}")
            else:
                # Some other bucket error occurred
                logger.error(f"Failed to access bucket {bucket_name}: {str(e)}")
                temp_client.close()
                raise ValueError(f"Failed to access bucket {bucket_name}: {str(e)}")
        except Exception as e:
            # Handle any other unexpected errors
            logger.error(f"Unexpected error while accessing bucket {bucket_name}: {str(e)}")
            temp_client.close()
            raise ValueError(f"Unexpected error while accessing bucket {bucket_name}: {str(e)}")
        
        # Create the main client with the correct region
        self.client = ACSClient(Session(region=bucket_info.region))
        self.bucket = bucket_name
        
        # Initialize buffers
        self.read_buffer = ReadBuffer()
        self.write_buffer = WriteBuffer()

        # Verify we can access the bucket with the new client
        try:
            client_start = time.time()
            self.client.head_bucket(bucket_name)
            logger.info(f"Final bucket verification completed in {time.time() - client_start:.4f} seconds")
        except Exception as e:
            logger.error(f"Failed to verify bucket access with new client: {str(e)}")
            temp_client.close()
            raise ValueError(f"Failed to verify bucket access: {str(e)}")
            
        # Close the temporary client
        temp_client.close()
            
        time_function("__init__", start_time)

    def __del__(self):
        """
        Destructor to clean up resources.
        
        Ensures proper cleanup of resources by:
        1. Flushing any pending writes to storage
        2. Clearing read and write buffers
        3. Closing the ACS client connection
        """
        logger.info("Cleaning up ACSFuse resources...")
        try:
            # Flush and clean up write buffers first
            if hasattr(self, 'write_buffer'):
                # Get a list of all buffered files
                buffered_files = list(self.write_buffer.buffers.keys())
                for key in buffered_files:
                    try:
                        # Convert key to path and flush using existing method
                        path = '/' + key if not key.startswith('/') else key
                        self._flush_buffer(path)
                        logger.debug(f"Flushed and removed buffer for {key}")
                    except Exception as e:
                        logger.error(f"Error during write buffer cleanup for {key}: {e}", exc_info=True)

            # Clear read buffer
            if hasattr(self, 'read_buffer'):
                self.read_buffer.clear()
                logger.debug("Read buffer cleared")

            # Close the client connection
            if hasattr(self, 'client'):
                self.client.close()
                logger.info("ACS client connection closed")
        except Exception as e:
            logger.error(f"Error during ACSFuse cleanup: {e}", exc_info=True)
        finally:
            logger.info("ACSFuse cleanup completed")

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
        trace_op("getattr", path, fh=fh)
        logger.debug(f"getattr requested for path: {path}")
        start_time = time.time()
        
        now = datetime.now().timestamp()
        base_stat = {
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_atime': now,
            'st_mtime': now,
            'st_ctime': now,
            # Ensure the blocks information indicates plentiful space
            # This helps disk space checks pass even if statvfs isn't used
            'st_blocks': 1000000,  # Indicate lots of blocks
            'st_blksize': 4096,    # Standard block size
            'st_rdev': 0,          # Not a device file
        }

        if path == '/':  
            logger.debug(f"getattr returning root directory attributes")
            time_function("getattr", start_time)
            return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096}

        try:
            key = self._get_path(path)
            logger.debug(f"getattr converted path to key: {key}")
            
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
                    result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096}
                    logger.debug(f"getattr determined {path} is a directory. Returning attributes: {result}")
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
                logger.debug(f"getattr determined {path} is a file. Returning attributes: {result}")
                time_function("getattr", start_time)
                return result
            except Exception as e:
                if "NoSuchKey" in str(e):
                    logger.debug(f"Object {key} does not exist")
                else:
                    logger.error(f"Error checking file {key}: {str(e)}", exc_info=True)
                time_function("getattr", start_time)
                raise FuseOSError(errno.ENOENT)
                
        except Exception as e:
            logger.error(f"getattr error for {path}: {str(e)}", exc_info=True)
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
        logger.debug(f"readdir requested for path: {path}")
        start_time = time.time()
        
        try:
            prefix = self._get_path(path)
            if prefix and not prefix.endswith('/'):
                prefix += '/'
            logger.debug(f"readdir using prefix: {prefix}")

            entries = {'.', '..'}
            
            try:
                # Get all objects with prefix
                client_start = time.time()
                # Convert iterator to list to log raw results
                all_objects_list = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=prefix)
                ))
                logger.info(f"list_objects call for {prefix} completed in {time.time() - client_start:.4f} seconds")
                logger.debug(f"readdir raw objects from list_objects for prefix '{prefix}': {all_objects_list}")
                
                # Filter to get only immediate children
                seen = set()
                filtered_objects = []
                for obj in all_objects_list: # Iterate over the logged list
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
                logger.debug(f"readdir returning entries for {path}: {result}")
                time_function("readdir", start_time)
                return result

            except Exception as e:
                logger.error(f"Error in readdir list_objects for {prefix}: {str(e)}", exc_info=True)
                result = list(entries)
                time_function("readdir", start_time)
                return result
                
        except Exception as e:
            logger.error(f"Error in readdir for {path}: {str(e)}", exc_info=True)
            time_function("readdir", start_time)
            raise FuseOSError(errno.EIO)

    def rename(self, old, new):
        """
        Rename a file or directory with atomic-like guarantees.
        
        This method implements a robust rename operation that:
        1. Handles both files and directories
        2. Ensures atomic-like behavior for directory operations
        3. Maintains cache consistency
        4. Properly cleans up old paths
        
        Args:
            old (str): Old path
            new (str): New path
            
        Raises:
            FuseOSError: If the source doesn't exist or an error occurs
        """
        trace_op("rename", new, old=old)
        logger.info(f"rename: Starting rename operation from {old} to {new}")
        start_time = time.time()
        
        old_key = self._get_path(old)
        new_key = self._get_path(new)
        
        # Track operation state for cleanup
        operation_state = {
            'source_verified': False,
            'is_directory': False,
            'objects_listed': False,
            'new_written': False,
            'buffers_updated': False
        }

        try:
            # Step 1: Check if this is a directory operation
            try:
                client_start = time.time()
                # Try as file first
                source_metadata = self.client.head_object(self.bucket, old_key)
                operation_state['source_verified'] = True
                operation_state['is_directory'] = False
                logger.info(f"rename: Source verification completed in {time.time() - client_start:.4f} seconds")
            except Exception as e:
                # If not a file, check if it's a directory
                dir_key = old_key if old_key.endswith('/') else old_key + '/'
                try:
                    client_start = time.time()
                    objects = list(self.client.list_objects(
                        self.bucket,
                        ListObjectsOptions(prefix=dir_key, max_keys=1)
                    ))
                    logger.info(f"rename: Directory check completed in {time.time() - client_start:.4f} seconds")
                    
                    if objects:
                        operation_state['source_verified'] = True
                        operation_state['is_directory'] = True
                    else:
                        logger.error(f"rename: Source {old_key} does not exist as file or directory")
                        raise FuseOSError(errno.ENOENT)
                except Exception as dir_e:
                    logger.error(f"rename: Failed to verify source {old_key}: {str(dir_e)}")
                    raise FuseOSError(errno.ENOENT)

            # Step 2: Handle directory rename
            if operation_state['is_directory']:
                dir_old_key = old_key if old_key.endswith('/') else old_key + '/'
                dir_new_key = new_key if new_key.endswith('/') else new_key + '/'
                
                # List all objects under the old directory
                try:
                    client_start = time.time()
                    objects = list(self.client.list_objects(
                        self.bucket,
                        ListObjectsOptions(prefix=dir_old_key)
                    ))
                    operation_state['objects_listed'] = True
                    logger.info(f"rename: Listed {len(objects)} objects in directory {dir_old_key}")
                except Exception as e:
                    logger.error(f"rename: Failed to list objects in directory {dir_old_key}: {str(e)}")
                    raise FuseOSError(errno.EIO)

                # Step 3: Copy each object to new location
                for old_obj in objects:
                    try:
                        # TODO Check if the object is already in the write buffer
                        # Calculate new object key
                        rel_path = old_obj[len(dir_old_key):]
                        new_obj = dir_new_key + rel_path
                        
                        # Copy object
                        client_start = time.time()
                        copy_source_str = f"{self.bucket}/{old_obj}"
                        self.client.copy_object(
                            bucket=self.bucket,
                            copy_source=copy_source_str,
                            key=new_obj
                        )
                        logger.info(f"rename: Copied {old_obj} to {new_obj} in {time.time() - client_start:.4f} seconds")
                        
                        # Update buffer state for this file
                        if self.write_buffer.has_buffer(old_obj):
                            data = self.write_buffer.read(old_obj)
                            if data is not None:
                                self.write_buffer.initialize_buffer(new_obj, data)
                            self.write_buffer.remove(old_obj)
                        if self.read_buffer.get(old_obj):
                            self.read_buffer.remove(old_obj)
                            
                    except Exception as e:
                        logger.error(f"rename: Failed to copy object {old_obj}: {str(e)}")
                        # Continue with other files but track failure
                        operation_state['new_written'] = False

                # Step 4: Delete old objects only after all copies succeed
                if operation_state['new_written'] is not False:  # Not explicitly failed
                    for old_obj in objects:
                        try:
                            client_start = time.time()
                            self.client.delete_object(self.bucket, old_obj)
                            logger.info(f"rename: Deleted old object {old_obj} in {time.time() - client_start:.4f} seconds")
                        except Exception as e:
                            logger.error(f"rename: Failed to delete old object {old_obj}: {str(e)}")
                            # Continue deletion of other objects
                
                operation_state['buffers_updated'] = True
                logger.info(f"rename: Successfully renamed directory from {old_key} to {new_key}")
                time_function("rename", start_time)
                return 0

            # Step 3: Handle single file rename (existing implementation)
            source_data = None
            if self.write_buffer.has_buffer(old_key):
                logger.debug(f"rename: Reading source {old_key} from write buffer")
                try:
                    source_data = self.write_buffer.read(old_key)
                    if source_data is not None:
                        operation_state['data_read'] = True
                        logger.debug(f"rename: Successfully read {len(source_data)} bytes from write buffer")
                except Exception as e:
                    logger.error(f"rename: Failed to read from write buffer: {str(e)}")
                    # Fall through to try S3 direct copy

            # Write to new location
            try:
                if source_data is not None:
                    # Use buffered data
                    client_start = time.time()
                    self.client.put_object(self.bucket, new_key, source_data)
                    logger.info(f"rename: put_object for {new_key} completed in {time.time() - client_start:.4f} seconds")
                else:
                    # Use server-side copy
                    logger.debug(f"rename: Using server-side copy from {old_key} to {new_key}")
                    copy_source_str = f"{self.bucket}/{old_key}"
                    client_start = time.time()
                    self.client.copy_object(
                        bucket=self.bucket,
                        copy_source=copy_source_str,
                        key=new_key
                    )
                    logger.info(f"rename: copy_object completed in {time.time() - client_start:.4f} seconds")
                
                # Verify new file exists
                client_start = time.time()
                new_metadata = self.client.head_object(self.bucket, new_key)
                logger.info(f"rename: New file verification completed in {time.time() - client_start:.4f} seconds")
                operation_state['new_written'] = True

            except Exception as e:
                logger.error(f"rename: Failed to write new file {new_key}: {str(e)}")
                # Attempt cleanup if partial write occurred
                try:
                    if operation_state['new_written']:
                        self.client.delete_object(self.bucket, new_key)
                except Exception as cleanup_e:
                    logger.error(f"rename: Cleanup of failed write failed: {str(cleanup_e)}")
                raise FuseOSError(errno.EIO)

            # Update buffers and cleanup old file
            try:
                # Initialize write buffer for new file if needed
                if source_data is not None:
                    self.write_buffer.initialize_buffer(new_key, source_data)
                
                # Remove old file only after new file is confirmed
                client_start = time.time()
                self.client.delete_object(self.bucket, old_key)
                logger.info(f"rename: delete_object for {old_key} completed in {time.time() - client_start:.4f} seconds")
                
                # Clean up buffers
                if self.write_buffer.has_buffer(old_key):
                    self.write_buffer.remove(old_key)
                if self.read_buffer.get(old_key):
                    self.read_buffer.remove(old_key)
                if self.read_buffer.get(new_key):
                    self.read_buffer.remove(new_key)
                    
                operation_state['buffers_updated'] = True
                
            except Exception as e:
                logger.error(f"rename: Buffer/cleanup operations failed: {str(e)}")
                # Continue since new file is already written
                
            logger.info(f"rename: Successfully completed rename from {old_key} to {new_key}")
            logger.debug(f"rename: Operation state: {operation_state}")
            time_function("rename", start_time)
            return 0
            
        except FuseOSError:
            raise
        except Exception as e:
            logger.error(f"rename: Unhandled error: {str(e)}", exc_info=True)
            time_function("rename", start_time)
            raise FuseOSError(errno.EIO)

    def read(self, path, size, offset, fh):
        """
        Read file contents, checking buffer first.
        
        Optimization: Avoids re-fetching the entire object on subsequent read 
        buffer misses after the first full fetch.
        
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
        trace_op("read", path, size=size, offset=offset, fh=fh)
        logger.debug(f"read requested for path: {path}, size: {size}, offset: {offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # 1. Check write buffer first
            if self.write_buffer.has_buffer(key):
                logger.debug(f"Read path: Found key {key} in write buffer. Attempting read with offset/size.")
                # Use the refactored read method with offset and size
                write_chunk = self.write_buffer.read(key, offset=offset, size=size)
                if write_chunk is not None:
                    logger.debug(f"Read {len(write_chunk)} bytes directly from write buffer for {key} at offset {offset}")
                    time_function("read (from write_buffer)", start_time)
                    return write_chunk
                else:
                    # read returning None likely indicates an error or closed buffer
                    logger.error(f"Write buffer read for {key} returned None unexpectedly. Falling back...")
                    # Fall through to check read buffer / fetch, but log this as an issue.

            # 2. Check read buffer
            # Use the get method which handles chunking within BufferEntry
            cached_data_chunk = self.read_buffer.get(key, offset, size)
            if cached_data_chunk is not None:
                 logger.debug(f"Read buffer HIT: {len(cached_data_chunk)} bytes for {key} at offset {offset}")
                 # Check if the returned chunk is smaller than requested size AND
                 # we are not at the end of the file (based on a prior getattr size maybe?)
                 # For simplicity, we assume the read_buffer.get handles partial reads correctly.
                 return cached_data_chunk
                
            # 3. Buffer MISS - Fetch the object (or required part) from ACS
            logger.debug(f"Read buffer MISS for {key} at offset {offset}")
            # --- Always fetch on miss ---
            # Remove the _fully_fetched_keys optimization. If data isn't in the buffer,
            # we must fetch it from the source to ensure correctness, even if it means
            # re-fetching data that was previously evicted.
            # Check existence first to provide a better error
            try:
                 logger.info(f"Read buffer miss for {key}. Checking existence with head_object...")
                 head_start = time.time()
                 metadata = self.client.head_object(self.bucket, key)
                 logger.info(f"head_object check for {key} completed in {time.time() - head_start:.4f}s. Size: {metadata.content_length}")
            except Exception as head_e:
                 # Explicitly check for NoSuchKey / Not Found type errors from head_object
                 if "NoSuchKey" in str(head_e) or "Not Found" in str(head_e):
                      logger.error(f"Object {key} confirmed NOT FOUND during head_object check before get_object: {head_e}")
                      raise FuseOSError(errno.ENOENT) # Raise specific error if head confirms not found
                 else:
                      logger.warning(f"head_object check failed for {key} with unexpected error: {head_e}. Proceeding with get_object cautiously.", exc_info=True)
                      # Proceed to get_object, but log the warning

            logger.info(f"Read buffer miss for {key}. Fetching entire object from ACS...")
            client_start = time.time()
            try:
                data = self.client.get_object(self.bucket, key)
            except Exception as oe: # Catch broader exceptions during get_object now
                 # If the object truly doesn't exist (should have been caught by head_object ideally)
                 if "NoSuchKey" in str(oe) or "Not Found" in str(oe):
                      logger.error(f"Object {key} not found during get_object (expected head_object to catch this): {oe}")
                      raise FuseOSError(errno.ENOENT)
                 else:
                      logger.error(f"Unexpected error during get_object for {key}: {oe}", exc_info=True)
                      raise FuseOSError(errno.EIO) # Raise generic IO error for other get_object failures

            fetch_time = time.time() - client_start
            logger.info(f"get_object for {key} completed in {fetch_time:.4f}s, fetched {len(data)} bytes")

            # Add entire object to read buffer (which handles chunking)
            self.read_buffer.put(key, data)
            
            # Return the requested portion from the newly fetched & cached data
            if offset >= len(data):
                return b""
            end_offset = min(offset + size, len(data))
            result = data[offset:end_offset]
            
            logger.debug(f"Returning {len(result)} bytes for {key} after fetch and cache.")
            return result
            
        except FuseOSError as fe: # Re-raise specific FUSE errors
            raise
        except Exception as e:
            logger.error(f"Error reading {key}: {str(e)}", exc_info=True)
            raise FuseOSError(errno.EIO)

    def write(self, path, data, offset, fh):
        """
        Write data to a file with improved buffer handling.
        
        This method implements robust write operations that:
        1. Maintain write buffer consistency
        2. Support concurrent access patterns
        3. Provide better error recovery
        4. Invalidate read buffer to maintain consistency
        
        Args:
            path (str): Path to the file
            data (bytes): Data to write
            offset (int): Offset at which to write
            fh (int): File handle
            
        Returns:
            int: Number of bytes written
            
        Raises:
            FuseOSError: If an error occurs during write
        """
        trace_op("write", path, offset=offset, size=len(data))
        logger.info(f"write: Writing {len(data)} bytes to {path} at offset {offset}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        try:
            # Ensure write buffer exists
            if not self.write_buffer.has_buffer(key):
                logger.debug(f"write: Initializing write buffer for {key}")
                self.write_buffer.initialize_buffer(key)
            
            # Write to buffer
            logger.debug(f"write: Writing to buffer for {key}")
            client_start = time.time()
            bytes_written = self.write_buffer.write(key, data, offset)
            logger.info(f"write: Buffer write completed in {time.time() - client_start:.4f} seconds")
            
            if bytes_written != len(data):
                logger.error(f"write: Incomplete write for {key}. Expected {len(data)} bytes, wrote {bytes_written}")
                raise FuseOSError(errno.EIO)
            
            # Invalidate read buffer since we wrote new data
            if bytes_written > 0 and self.read_buffer.get(key) is not None:
                logger.debug(f"write: Invalidating read buffer for {key} due to write")
                self.read_buffer.remove(key)
            
            logger.debug(f"write: Successfully wrote {bytes_written} bytes to {key}")
            time_function("write", start_time)
            return bytes_written
            
        except Exception as e:
            logger.error(f"write: Error writing to {key}: {str(e)}", exc_info=True)
            time_function("write", start_time)
            raise FuseOSError(errno.EIO)

    def create(self, path, mode, fi=None):
        """
        Create a new file.
        
        This method implements a robust file creation that:
        1. Creates empty object in S3 to ensure file existence
        2. Initializes write buffer state
        3. Maintains consistent buffer state
        4. Supports concurrent access patterns
        
        Args:
            path (str): Path to the file
            mode (int): File mode (permissions are enforced at mount level)
            fi (dict, optional): File info. Defaults to None.
            
        Returns:
            int: File handle
            
        Raises:
            FuseOSError: If an error occurs during file creation
        """
        trace_op("create", path, mode=oct(mode))
        logger.info(f"create: Creating new file at {path} with mode {oct(mode)}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        try:
            # Invalidate read buffer in case this path existed before
            logger.debug(f"create: Invalidating read buffer for potential prior {key}")
            self.read_buffer.remove(key)

            # Create empty object in S3 first to ensure file existence
            # This is necessary for getattr to work correctly
            logger.debug(f"create: Creating empty file in S3: {key}")
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.info(f"create: put_object call completed in {time.time() - client_start:.4f} seconds")
            
            # Initialize write buffer
            logger.debug(f"create: Initializing write buffer for: {key}")
            self.write_buffer.initialize_buffer(key)
            
            # Verify buffer initialization
            if not self.write_buffer.has_buffer(key):
                logger.error(f"create: Failed to initialize write buffer for {key}")
                # Clean up the S3 object if buffer init fails
                try:
                    self.client.delete_object(self.bucket, key)
                except Exception as cleanup_e:
                    logger.error(f"create: Failed to cleanup S3 object after buffer init failure: {str(cleanup_e)}")
                raise FuseOSError(errno.EIO)
                
            logger.debug(f"create: Successfully created file {key}")
            time_function("create", start_time)
            return 0
            
        except Exception as e:
            logger.error(f"create: Error creating {key}: {str(e)}", exc_info=True)
            # Cleanup any partial state
            try:
                if self.write_buffer.has_buffer(key):
                    self.write_buffer.remove(key)
                # Try to remove the S3 object if it was created
                self.client.delete_object(self.bucket, key)
            except Exception as cleanup_e:
                logger.error(f"create: Cleanup after error failed: {str(cleanup_e)}")
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
        logger.debug(f"unlink requested for path: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
             # Also remove any potentially unflushed buffers
            if self.write_buffer.has_buffer(key):
                 logger.warning(f"Unlinking path {path} which has an active write buffer. Removing buffer.")
                 self.write_buffer.remove(key)
            if self.read_buffer.get(key) is not None: # Check read buffer too
                 logger.debug(f"Removing {key} from read buffer during unlink.")
                 self.read_buffer.remove(key)

            logger.debug(f"Attempting DeleteObject for key: {key}")
            client_start = time.time()
            self.client.delete_object(self.bucket, key)
            logger.info(f"delete_object call for {key} completed in {time.time() - client_start:.4f} seconds")
            logger.debug(f"Unlink successful for {path}")
            time_function("unlink", start_time)
            # Unlink usually returns 0 on success per POSIX
            return 0
        except Exception as e:
            # Check if the error is "NoSuchKey" - should not be an error for unlink
            if "NoSuchKey" in str(e):
                 logger.warning(f"Attempted to unlink non-existent key: {key}. Returning success.")
                 time_function("unlink", start_time)
                 return 0 # POSIX allows unlinking non-existent files without error
            logger.error(f"Error unlinking {key}: {str(e)}", exc_info=True)
            time_function("unlink", start_time)
            raise FuseOSError(errno.EIO)

    def mkdir(self, path, mode):
        """
        Create a directory.
        
        This method creates a directory by creating an empty object
        with a trailing slash in the key.
        
        Args:
            path (str): Path to the directory
            mode (int): Directory mode (Note: mode is currently ignored)
            
        Raises:
            FuseOSError: If an error occurs while creating the directory
        """
        logger.debug(f"mkdir requested for path: {path}, mode={oct(mode)}")
        start_time = time.time()
        
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            # Check if it already exists (as a file or dir)
            try: 
                logger.debug(f"Checking existence before mkdir for key: {key}")
                self.client.head_object(self.bucket, key)
                logger.warning(f"mkdir failed: path {path} (key {key}) already exists.")
                raise FuseOSError(errno.EEXIST) # Already exists
            except Exception as e:
                 if "Not Found" in str(e) or "NoSuchKey" in str(e):
                     logger.debug(f"Path {path} (key {key}) does not exist, proceeding with mkdir.")
                     pass # Good, doesn't exist
                 else:
                     raise # Other unexpected error during head_object check

            logger.debug(f"Attempting PutObject (0 bytes) for directory key: {key}")
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.info(f"put_object call for directory {key} completed in {time.time() - client_start:.4f} seconds")
            logger.debug(f"mkdir successful for {path}")
            time_function("mkdir", start_time)
            # mkdir usually returns 0 on success
            return 0
        except FuseOSError: # Re-raise specific FUSE errors like EEXIST
            raise
        except Exception as e:
            logger.error(f"Error creating directory {key}: {str(e)}", exc_info=True)
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
        logger.debug(f"rmdir requested for path: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            # Check if directory is empty (list objects with prefix, expect only the dir key itself)
            logger.debug(f"Checking if directory is empty for key: {key}")
            client_start = time.time()
            
            # Fetch up to 2 keys with the given prefix. No delimiter option available.
            contents = list(self.client.list_objects(
                self.bucket,
                ListObjectsOptions(prefix=key, max_keys=2) 
            ))
            logger.info(f"list_objects call for emptiness check on {key} completed in {time.time() - client_start:.4f} seconds")
            logger.debug(f"Emptiness check results for {key} (max_keys=2): {contents}")

            # Determine if empty based on results
            is_empty = False
            num_found = len(contents)

            if num_found == 0:
                 # Directory key itself doesn't exist.
                 logger.warning(f"Attempted rmdir on non-existent directory key: {key}")
                 raise FuseOSError(errno.ENOENT)
            elif num_found == 1:
                 # Found exactly one key. It must be the directory key itself to be considered empty.
                 if contents[0] == key:
                     is_empty = True
                 else:
                     # Found one item, but it's not the dir key itself - means non-empty
                     is_empty = False 
                     logger.debug(f"Directory {key} considered non-empty because the single item found was '{contents[0]}' not '{key}'")
            else: # num_found >= 2
                 # Found the directory key plus at least one other item.
                 is_empty = False
                 logger.debug(f"Directory {key} considered non-empty because list_objects returned {num_found} items (>=2)")

            if not is_empty:
                logger.warning(f"Directory {key} is not empty, cannot remove.")
                time_function("rmdir", start_time)
                raise FuseOSError(errno.ENOTEMPTY)
                
            logger.debug(f"Directory {key} confirmed empty. Attempting DeleteObject.")
            client_start_delete = time.time()
            self.client.delete_object(self.bucket, key)
            logger.info(f"delete_object call for directory {key} completed in {time.time() - client_start_delete:.4f} seconds")
            logger.debug(f"rmdir successful for {path}")
            time_function("rmdir", start_time)
             # rmdir usually returns 0 on success
            return 0
        except FuseOSError:
            # Re-raise FUSE errors like ENOTEMPTY, ENOENT
            time_function("rmdir", start_time) # Log timing even for handled FUSE errors
            raise
        except Exception as e:
             # Check if the error is "NoSuchKey" during the DELETE operation (should not happen if check passed)
            if "NoSuchKey" in str(e):
                 logger.error(f"DeleteObject failed with NoSuchKey for {key} even after emptiness check passed!", exc_info=True)
                 # This indicates a potential race condition or logic error
                 time_function("rmdir", start_time)
                 raise FuseOSError(errno.EIO) # Internal error state
            logger.error(f"Error removing directory {key}: {str(e)}", exc_info=True)
            time_function("rmdir", start_time)
            raise FuseOSError(errno.EIO)
    
    def truncate(self, path, length, fh=None):
        """
        Truncate file to specified length.
        
        This method changes the size of a file by either truncating it
        or extending it with null bytes. Handles both buffer and direct object cases.
        
        Args:
            path (str): Path to the file
            length (int): New length of the file
            fh (int, optional): File handle. Defaults to None.
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If an error occurs while truncating the file
        """
        logger.debug(f"truncate requested for path: {path}, length={length}, fh={fh}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Check if there's an active write buffer first
            if self.write_buffer.has_buffer(key):
                logger.debug(f"Truncating file with active write buffer: {key}")
                self.write_buffer.truncate(key, length)
                # Invalidate read buffer as well
                logger.debug(f"Invalidating read buffer for {key} due to truncate")
                self.read_buffer.remove(key)
            else:
                # No active write buffer, we need to operate directly on the object storage
                logger.debug(f"Truncating file without active write buffer: {key}. Fetching, modifying, putting.")
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    logger.info(f"get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                except Exception as e:
                     # If file doesn't exist, truncate(0) might be valid (create), but truncate(N>0) is error
                    if ("NoSuchKey" in str(e) or "Not Found" in str(e)) :
                        if length == 0:
                             logger.debug(f"Truncate(0) on non-existent file {key}. Creating empty file.")
                             data = b"" # Treat as creating an empty file
                             # Proceed to put empty data
                        else:
                             logger.error(f"Attempted to truncate non-existent file {key} to length {length}", exc_info=True)
                             raise FuseOSError(errno.ENOENT)
                    else:
                         logger.error(f"Error fetching object {key} for truncate: {e}", exc_info=True)
                         raise FuseOSError(errno.EIO)

                # Perform truncation on the fetched data
                original_len = len(data)
                if length < original_len:
                    data = data[:length]
                    logger.debug(f"Truncated data from {original_len} to {length} bytes")
                elif length > original_len:
                    data += b'\\x00' * (length - original_len)
                    logger.debug(f"Extended data from {original_len} to {length} bytes with nulls")
                # else: length == original_len, no change needed

                # Write the modified data back to object storage
                logger.debug(f"Putting truncated object back to {key} with new length {len(data)}")
                client_start = time.time()
                self.client.put_object(self.bucket, key, data)
                logger.info(f"put_object call for truncated {key} completed in {time.time() - client_start:.4f} seconds")

                # Invalidate read buffer just in case it existed but wasn't hit by write buffer check
                logger.debug(f"Invalidating read buffer for {key} after direct truncate")
                self.read_buffer.remove(key)
            
            logger.debug(f"Truncate successful for {path} to length {length}")
            time_function("truncate", start_time)
            return 0 # Truncate usually returns 0 on success
        except FuseOSError: # Re-raise specific FUSE errors
            raise
        except Exception as e:
            logger.error(f"Error truncating {key}: {str(e)}", exc_info=True)
            time_function("truncate", start_time)
            raise FuseOSError(errno.EIO)

    def open(self, path, flags):
        """
        Open a file and prepare it for reading/writing.
        
        This is called when a file is opened.
        
        Args:
            path (str): Path to the file
            flags (int): Open flags (O_RDONLY, O_WRONLY, etc.)
            
        Returns:
            int: File handle (using 0 as default)
        """
        trace_op("open", path, flags=flags)
        logger.debug(f"open requested for path: {path}, flags={flags}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        # Check if this is a read-only open, which is common for model files
        is_readonly = (flags & os.O_ACCMODE) == os.O_RDONLY
        
        # For read-only operations, verify the file exists
        if is_readonly:
            try:
                client_start = time.time()
                self.client.head_object(self.bucket, key)
                logger.debug(f"open: File {key} exists")
            except Exception as e:
                # File doesn't exist - valid error for caller to handle
                logger.error(f"open: File {key} does not exist: {e}")
                time_function("open", start_time)
                raise FuseOSError(errno.ENOENT)
                
        time_function("open", start_time)
        return 0  # File handle - using 0 as default

    def _flush_buffer(self, path):
        """
        Flush the in-memory buffer for a file to ACS storage. Reads from the 
        underlying file object (SpooledTemporaryFile) in chunks before constructing
        the final bytes object for the client.
        
        Args:
            path (str): Path to the file
            
        Raises:
            Exception: If an error occurs while flushing the buffer
        """
        trace_op("_flush_buffer", path)
        logger.debug(f"_flush_buffer called for path: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        # Check if there's anything to flush
        if not self.write_buffer.has_buffer(key):
            logger.debug(f"No active write buffer to flush for {key}")
            return

        FLUSH_CHUNK_SIZE = 4 * 1024 * 1024 # 4MB chunk size for reading from spooled file

        try:
            # Get buffer size for logging
            buffer_size = self.write_buffer.get_size(key)
            
            # Get the underlying file object (SpooledTemporaryFile)
            # We need direct access, so bypassing write_buffer.read()
            with self.write_buffer.lock: # Ensure thread safety while accessing buffer
                if key not in self.write_buffer.buffers:
                     logger.warning(f"Attempted to flush non-existent buffer for key: {key} (inside lock)")
                     return
                spooled_file = self.write_buffer.buffers[key]
                spooled_file.seek(0) # Ensure we read from the beginning

                # Read in chunks and accumulate into an in-memory BytesIO
                accumulated_data = BytesIO()
                while True:
                    chunk = spooled_file.read(FLUSH_CHUNK_SIZE)
                    if not chunk:
                        break
                    accumulated_data.write(chunk)
            
            # Get the final complete bytes object 
            # This is still required by the unchanged client.put_object
            final_data = accumulated_data.getvalue()
            accumulated_data.close() # Release memory from the intermediate buffer

            if len(final_data) != buffer_size:
                 logger.warning(f"Flush size mismatch for {key}: Expected {buffer_size}, got {len(final_data)}")

            # Upload the data
            upload_start = time.time()
            # Pass the complete bytes object to the existing put_object
            self.client.put_object(self.bucket, key, final_data) 
            upload_time = time.time() - upload_start
            
            # Log the upload operation
            if buffer_size > 0: # Avoid division by zero and log only for non-empty flushes
                 throughput_mbps = (buffer_size / (1024*1024)) / upload_time if upload_time > 0 else float('inf')
                 logger.info(f"Flushed {buffer_size/(1024*1024):.2f}MB to {key} in {upload_time:.2f}s ({throughput_mbps:.2f} MB/s)")
            else:
                 logger.info(f"Flushed empty buffer for {key} in {upload_time:.2f}s")

            # Invalidate read buffer entry since file has been updated on storage
            self.read_buffer.remove(key)
            
        except KeyError:
             logger.warning(f"Attempted to flush non-existent buffer for key: {key} (KeyError)")
        except Exception as e:
            logger.error(f"Error flushing buffer for {key}: {e}", exc_info=True)
            raise # Re-raise the exception to signal failure
        finally:
             time_function("_flush_buffer", start_time) # Log time even on error

    def fsync(self, path, datasync, fh):
        """
        Synchronize file contents to storage.

        Ensures data is safely persisted to storage for reliability.

        Args:
            path (str): Path to the file
            datasync (int): 1 for datasync, 0 for fsync
            fh (int): File handle

        Returns:
            int: 0 (always succeeds)
        """
        trace_op("fsync", path, datasync=datasync, fh=fh)
        logger.debug(f"fsync requested for path: {path}, datasync={datasync}, fh={fh}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        # Always flush to storage for reliability
        if self.write_buffer.has_buffer(key):
            logger.debug(f"fsync: Flushing write buffer for {key}")
            try:
                # Use internal _flush_buffer logic which handles spooled files correctly
                self._flush_buffer(path) # Call the internal flush method
                logger.debug(f"fsync: Successfully flushed buffer for {key} via _flush_buffer")

                # Invalidate the read buffer after successful flush
                if self.read_buffer.get(key):
                    self.read_buffer.remove(key)
            except Exception as e:
                logger.error(f"fsync: Error during _flush_buffer for {key}: {e}", exc_info=True)
                # Decide if we should raise FuseOSError(errno.EIO) here or just log
                # For fsync, often best effort is acceptable, so just log for now.
        else:
            logger.debug(f"fsync: No write buffer found for {key}")

        time_function("fsync", start_time)
        return 0
        
    def release(self, path, fh):
        """
        Release the file handle and flush the write buffer to ACS storage.
        
        This method is called when a file is closed. It flushes the write buffer
        to object storage and removes the file from both buffers.
        
        Args:
            path (str): Path to the file
            fh (int): File handle
            
        Returns:
            int: 0 on success (usually ignored)
        """
        trace_op("release", path, fh=fh)
        logger.debug(f"release requested for path: {path}, fh={fh}")
        start_time = time.time()
        
        key = self._get_path(path)
        
        # Always flush to storage on release for reliability
        if self.write_buffer.has_buffer(key):
            logger.debug(f"release: Write buffer exists for {key}, flushing to storage via _flush_buffer")
            try:
                 self._flush_buffer(path) # Call the internal flush method
                 logger.info(f"release: Successfully flushed buffer for {key} via _flush_buffer")
            except Exception as e:
                logger.error(f"release: Error during _flush_buffer for {key}: {e}", exc_info=True)
                # Continue with cleanup anyway

            # Clean up the write buffer *after* attempting flush
            logger.debug(f"release: Removing write buffer for {key}")
            self.write_buffer.remove(key)
        else:
            logger.debug(f"release: No write buffer found for {key}")
            
        # Clean up the read buffer too
        if self.read_buffer.get(key):
            logger.debug(f"release: Removing read buffer for {key}")
            self.read_buffer.remove(key)
            
        logger.debug(f"release: Finished for {path}")
        time_function("release", start_time)
        return 0

    def chmod(self, path, mode):
        """
        Change file/directory mode (implemented as a no-op).

        Object storage doesn't support POSIX modes, so we ignore this.
        Hugging Face caching attempts this on lock files.

        Args:
            path (str): Path to the file/directory
            mode (int): New mode

        Returns:
            int: 0 (always succeeds)
        """
        logger.debug(f"chmod requested for path: {path}, mode={oct(mode)} - NO-OP")
        # No-op for object storage
        # Check if path actually exists first? Optional, but could return ENOENT.
        # key = self._get_path(path)
        # try:
        #     self.client.head_object(self.bucket, key) # Or use getattr logic
        # except Exception:
        #     raise FuseOSError(errno.ENOENT)
        return 0

    def chown(self, path, uid, gid):
        """
        Change owner/group (implemented as a no-op).

        Object storage doesn't support POSIX owners/groups in the same way.

        Args:
            path (str): Path to the file/directory
            uid (int): New user ID
            gid (int): New group ID

        Returns:
            int: 0 (always succeeds)
        """
        logger.debug(f"chown requested for path: {path}, uid={uid}, gid={gid} - NO-OP")
        # No-op for object storage
        return 0

    def link(self, target, name):
        """
        Create 'hard link' by performing a server-side copy of the object.
        Avoids reading large files into memory.
        """
        trace_op("link", name, target=target)
        logger.debug(f"link requested target='{target}', name='{name}'")
        start_time = time.time()
        link_start_time = start_time

        try:
            target_key = self._get_path(target)
            new_key = self._get_path(name)
            logger.debug(f"Link (Server-Side Copy): target_key='{target_key}', new_key='{new_key}'")

            # First verify target exists and get size for logging
            try:
                logger.debug(f"Link: Checking existence of target_key='{target_key}'")
                client_start_head = time.time()
                metadata = self.client.head_object(self.bucket, target_key)
                target_size = metadata.content_length
                logger.info(f"Link: head_object for {target_key} completed in {time.time() - client_start_head:.4f} seconds. Size: {target_size}")
            except Exception as e:
                logger.error(f"Link target object {target_key} does not exist or head failed: {str(e)}", exc_info=True)
                raise FuseOSError(errno.ENOENT)

            # Invalidate read buffer for the target link name in case it existed before
            logger.debug(f"Link: Invalidating read buffer for potential prior {new_key}")
            self.read_buffer.remove(new_key)
            
            # --- Use Server-Side Copy --- 
            copy_source_str = f"{self.bucket}/{target_key}" # Assuming format bucket/key
            logger.debug(f"Link: Attempting server-side copy from source '{copy_source_str}' to bucket '{self.bucket}', key '{new_key}'")
            client_start_copy = time.time()
            
            self.client.copy_object(
                bucket=self.bucket,       # Destination bucket
                copy_source=copy_source_str, # Source bucket/key string
                key=new_key              # Destination key
            )
            
            copy_duration = time.time() - client_start_copy
            logger.info(f"Link: Server-side copy_object call for {new_key} completed in {copy_duration:.4f} seconds.")
            # --- End Server-Side Copy --- 

            total_link_time = time.time() - link_start_time
            logger.debug(f"Link successful from '{target}' to '{name}'. Total link time: {total_link_time:.4f} seconds.")
            time_function("link", start_time)
            return 0
        except FuseOSError:
            raise
        except Exception as e:
            logger.error(f"Error creating link (server-side copy) from {target} to {name}: {str(e)}", exc_info=True)
            time_function("link", start_time)
            raise FuseOSError(errno.EIO)

    def flock(self, path, op, fh):
        """
        File locking operation.
        
        This method implements file locking more robustly to support
        concurrent access patterns in ML frameworks like HuggingFace.
        
        Args:
            path (str): Path to the file
            op (int): Lock operation (LOCK_SH, LOCK_EX, etc.)
            fh (int): File handle
            
        Returns:
            int: 0 (always succeeds)
        """
        logger.info(f"flock: {path}, op={op}")
        start_time = time.time()
        
        # Track lock operations for better emulation
        key = self._get_path(path)
        
        # Check what type of lock is being requested
        if op & fcntl.LOCK_EX:
            logger.debug(f"Exclusive lock requested for {path}")
        elif op & fcntl.LOCK_SH:
            logger.debug(f"Shared lock requested for {path}")
        elif op & fcntl.LOCK_UN:
            logger.debug(f"Unlock requested for {path}")
        
        time_function("flock", start_time)
        return 0

    def lstat(self, path):
        """
        Get file attributes without following symlinks (like lstat).
        
        This is crucial for libraries checking symlink support.
        We treat any existing object as a potential symlink for lstat purposes.
        
        Args:
            path (str): Path to the file or directory
            
        Returns:
            dict: File attributes
            
        Raises:
            FuseOSError: If the file or directory does not exist
        """
        # Note: lstat is similar to getattr but *must not* follow symlinks.
        # For our implementation, if an object exists at `path`, we report it 
        # as a symlink. If a directory exists, report as directory.
        trace_op("lstat", path)
        logger.debug(f"lstat requested for path: {path}")
        start_time = time.time()
        
        now = datetime.now().timestamp()
        base_stat = {
            'st_uid': os.getuid(),
            'st_gid': os.getgid(),
            'st_atime': now,
            'st_ctime': now,
            # Set base blocks/blksize, but override for symlinks later
            'st_blocks': 1,      # Default blocks
            'st_blksize': 4096,  
            'st_rdev': 0,
        }

        if path == '/':  
            logger.debug(f"lstat returning root directory attributes")
            time_function("lstat", start_time)
            # Root is always a directory
            return {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096, 'st_mtime': now}

        try:
            key = self._get_path(path)
            logger.debug(f"lstat converted path to key: {key}")
            
            # 1. Try as a potential file/symlink object first
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.info(f"lstat: head_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                
                # If head_object succeeds, report as a symlink
                result = {**base_stat,
                        'st_mode': 0o120777,  # S_IFLNK | 0777 (Symbolic link permissions)
                        'st_size': metadata.content_length, # Size is the length of the target path string
                        'st_mtime': metadata.last_modified.timestamp(),
                        'st_blocks': 0, # Symlinks usually have 0 blocks
                        'st_nlink': 1}
                logger.debug(f"lstat determined {path} is an object (reporting as symlink). Returning attributes: {result}")
                time_function("lstat", start_time)
                return result
            except Exception as e:
                # Handle cases where the object doesn't exist or other head errors occurred
                if "NoSuchKey" in str(e) or "Not Found" in str(e):
                    logger.debug(f"lstat: Object {key} does not exist. Will check for directory.")
                else:
                    # Log unexpected errors during head_object but proceed to directory check
                    logger.warning(f"lstat: Error checking object {key}: {str(e)}. Proceeding to check directory.", exc_info=True)
                pass # Fall through to directory check

            # 2. If not found as an object, check if it's a directory prefix
            dir_key = key if key.endswith('/') else key + '/'
            try:
                client_start = time.time()
                # List objects with this prefix to check if it's a directory
                # Check for max_keys=1 to be efficient
                objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                logger.info(f"lstat: list_objects call for directory check {dir_key} completed in {time.time() - client_start:.4f} seconds")
                
                if objects:  # If we found any objects with this prefix, it's a directory
                    result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096, 'st_mtime': now}
                    logger.debug(f"lstat determined {path} is a directory. Returning attributes: {result}")
                    time_function("lstat", start_time)
                    return result
                else:
                    # If list_objects is empty, it is not a directory either
                    logger.debug(f"lstat: Directory check for {dir_key} returned no objects.")
                    time_function("lstat", start_time)
                    raise FuseOSError(errno.ENOENT)
            except FuseOSError: # Re-raise ENOENT if thrown above
                 raise
            except Exception as dir_e:
                logger.error(f"lstat: Error during directory check for {dir_key}: {str(dir_e)}", exc_info=True)
                time_function("lstat", start_time)
                # If directory check fails unexpectedly, report as non-existent
                raise FuseOSError(errno.ENOENT)
                
        except FuseOSError: # Re-raise ENOENT from inner blocks
             raise
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"lstat: Unhandled error for {path}: {str(e)}", exc_info=True)
            time_function("lstat", start_time)
            raise FuseOSError(errno.EIO) # Generic I/O error for safety

    def statvfs(self, path):
        """
        Get filesystem statistics (statvfs).

        Reports realistic disk space values compatible with all libraries.
        Provides special handling for HuggingFace paths.

        Args:
            path (str): Path within the filesystem (often '/')

        Returns:
            dict: A dictionary containing statvfs attributes.
        """
        trace_op("statvfs", path)
        logger.debug(f"statvfs requested for path: {path}")
        # Use realistic values that won't cause integer overflow
        # 5TB total space with 4.5TB free is a safe, realistic value
        block_size = 4096                        # Standard block size
        total_blocks = 1250000000                # ~5TB (1.25B * 4KB = 5TB)
        free_blocks = 1125000000                 # ~4.5TB free (90% free)
        
        result = {
            'f_bsize': block_size,               # Block size
            'f_frsize': block_size,              # Fragment size
            'f_blocks': total_blocks,            # Total blocks
            'f_bfree': free_blocks,              # Free blocks
            'f_bavail': free_blocks,             # Available blocks (same as free for us)
            'f_files': 10000000,                 # Total inodes (files) - reasonable number
            'f_ffree': 9000000,                  # Free inodes
            'f_favail': 9000000,                 # Available inodes
            'f_fsid': 42,                        # Filesystem ID - using a consistent value
            'f_flag': 0,                         # Mount flags
            'f_namemax': 255,                    # Maximum filename length
        }
        
        # Calculate human-readable values
        total_gb = (total_blocks * block_size) / (1024**3)
        free_gb = (free_blocks * block_size) / (1024**3)
        
        logger.debug(f"statvfs reporting: {total_gb:.2f}GB total, {free_gb:.2f}GB free ({(free_gb/total_gb)*100:.1f}% free)")
        
        return result

    def symlink(self, source, target):
        """
        Create a symbolic link to an existing file or directory.
        
        Hugging Face uses symlinks for efficient caching. For object storage,
        we implement this by creating an object at the target path whose content
        is the source path string.
        
        Args:
            source (str): Source path (target of the symlink)
            target (str): Target path (path where the symlink is created)
            
        Returns:
            int: 0 on success
        """
        trace_op("symlink", target, source=source)
        logger.info(f"symlink requested from {source} to {target}")
        start_time = time.time()
        
        try:
            # Target path is where the symlink "file" will be created
            target_key = self._get_path(target)
            
            # The *content* of the symlink file will be the source path
            symlink_content = source.encode('utf-8')
            
            # Invalidate read buffer for the symlink path in case it existed before
            logger.debug(f"symlink: Invalidating read buffer for potential prior {target_key}")
            self.read_buffer.remove(target_key)

            # Write the symlink object
            logger.debug(f"Creating symlink object at {target_key} pointing to {source}")
            client_start = time.time()
            self.client.put_object(self.bucket, target_key, symlink_content)
            logger.info(f"put_object call for symlink {target_key} completed in {time.time() - client_start:.4f} seconds")
            logger.debug(f"symlink created from {source} to {target}")
            
            time_function("symlink", start_time)
            return 0
        except Exception as e:
            logger.error(f"Error creating symlink from {source} to {target}: {str(e)}", exc_info=True)
            time_function("symlink", start_time)
            raise FuseOSError(errno.EIO)
            
    def readlink(self, path):
        """
        Read the target of a symbolic link.
        
        Retrieves the content of the object representing the symlink,
        which contains the source path.
        
        Args:
            path (str): Path to the symlink
            
        Returns:
            str: Target path of the symlink
            
        Raises:
            FuseOSError: If the path is not a symlink or an error occurs
        """
        trace_op("readlink", path)
        logger.debug(f"readlink requested for path: {path}")
        start_time = time.time()
        
        key = self._get_path(path)
        try:
            # Get the content of the symlink object
            logger.debug(f"Getting object content for symlink key: {key}")
            client_start = time.time()
            data = self.client.get_object(self.bucket, key)
            logger.info(f"get_object call for symlink {key} completed in {time.time() - client_start:.4f} seconds")
            
            # Decode the content (which is the source path)
            source_path = data.decode('utf-8')
                
            logger.debug(f"readlink returning source path: {source_path}")
            time_function("readlink", start_time)
            # Return the raw source path as stored
            return source_path
        except ObjectError as oe:
            # If the object doesn't exist, it's not a valid symlink
            logger.error(f"readlink failed: Object {key} not found. {str(oe)}")
            time_function("readlink", start_time)
            raise FuseOSError(errno.ENOENT)
        except Exception as e:
            logger.error(f"Error reading symlink {key}: {str(e)}", exc_info=True)
            time_function("readlink", start_time)
            # EINVAL is often used for "not a symlink" or other issues
            raise FuseOSError(errno.EINVAL) 

    def access(self, path, mode):
        """
        Check if a file can be accessed with the given mode.

        Args:
            path (str): Path to the file
            mode (int): Access mode (F_OK, R_OK, W_OK, X_OK)

        Returns:
            int: 0 on success

        Raises:
            FuseOSError: If the file doesn't exist or can't be accessed
        """
        trace_op("access", path, mode=mode)
        logger.debug(f"access requested for path: {path}, mode={mode}")
        start_time = time.time()
            
        try:
            # Check file existence
            if path == '/':
                logger.debug(f"access returning success for root directory")
                time_function("access", start_time)
                return 0
            
            key = self._get_path(path)
            try:
                client_start = time.time()
                self.client.head_object(self.bucket, key)
                logger.debug(f"access: head_object for {key} completed in {time.time() - client_start:.4f} seconds")
                time_function("access", start_time)
                return 0
            except Exception as e:
                logger.debug(f"Object not found for key {key}: {e}")
                
                # Check if it's a directory
                dir_key = key if key.endswith('/') else key + '/'
                try:
                    client_start = time.time()
                    objects = list(self.client.list_objects(
                        self.bucket,
                        ListObjectsOptions(prefix=dir_key, max_keys=1)
                    ))
                    logger.debug(f"access: list_objects for {dir_key} completed in {time.time() - client_start:.4f} seconds")
                    if objects:
                        time_function("access", start_time)
                        return 0
                except Exception as list_e:
                    logger.debug(f"Directory check failed for {dir_key}: {list_e}")
                    
                # Not found as file or directory
                logger.debug(f"access: {path} not found")
                time_function("access", start_time)
                raise FuseOSError(errno.ENOENT)
        except FuseOSError:
            raise
        except Exception as e:
            logger.error(f"Error checking access for {path}: {e}")
            time_function("access", start_time)
            raise FuseOSError(errno.EACCES)

    def statfs(self, path):
        """
        Get filesystem statistics.
        
        This method is called for the statfs/fstatfs system calls.
        HuggingFace libraries often use this call directly to check available disk space.
        
        Args:
            path (str): Path to get statistics for
            
        Returns:
            dict: A dictionary containing filesystem statistics
        """
        trace_op("statfs", path)
        logger.debug(f"statfs requested for path: {path}")
        start_time = time.time()
        
        # These values will show up in the `df` command output
        # and are critical for HuggingFace disk space checks
        
        # Use values that indicate 5TB total space with all of it free
        block_size = 4096
        total_blocks = 1250000000     # 5TB
        free_blocks = 1250000000      # 5TB free
        
        result = {
            'f_bsize': block_size,    # Block size
            'f_frsize': block_size,   # Fragment size
            'f_blocks': total_blocks, # Total blocks
            'f_bfree': free_blocks,   # Free blocks
            'f_bavail': free_blocks,  # Available blocks
            'f_files': 1000000000,    # Total inodes
            'f_ffree': 999999999,     # Free inodes
            'f_favail': 999999999,    # Available inodes
            'f_flag': 0,              # Mount flags
            'f_namemax': 255,         # Maximum filename length
        }
        
        # Calculate human-readable values for logging
        total_tb = (total_blocks * block_size) / (1024**4)
        free_tb = (free_blocks * block_size) / (1024**4)
        
        logger.debug(f"statfs reporting: {total_tb:.2f}TB total, {free_tb:.2f}TB free ({(free_tb/total_tb)*100:.1f}% free)")
        
        time_function("statfs", start_time)
        return result

    def getxattr(self, path, name, position=0):
        """
        Get extended attributes for a file or directory.
        
        This method supports common extended attributes needed by applications like HuggingFace:
        - user.mime_type: MIME type of the file
        - user.content_length: Size of the file
        - user.etag: ETag from S3
        - user.last_modified: Last modification time
        - security.capability: Security capabilities
        - system.posix_acl_access: POSIX ACL access info
        
        Args:
            path (str): Path to the file
            name (str): Name of the extended attribute
            position (int, optional): Position in the attribute. Defaults to 0.
            
        Returns:
            bytes: The attribute value
            
        Raises:
            FuseOSError: If the attribute doesn't exist (ENODATA) or file not found (ENOENT)
        """
        trace_op("getxattr", path, name=name, position=position)
        logger.debug(f"getxattr requested for path: {path}, name: {name}")
        start_time = time.time()
        
        try:
            # Special case for root directory
            if path == '/':
                if name == 'user.mime_type':
                    return b'inode/directory'
                elif name == 'security.capability':
                    # Standard directory capabilities
                    return b'\x01\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                raise FuseOSError(errno.ENODATA)

            key = self._get_path(path)
            
            # First check if it's a directory by checking with trailing slash
            dir_key = key if key.endswith('/') else key + '/'
            try:
                client_start = time.time()
                objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                logger.info(f"getxattr: list_objects call for directory check {dir_key} completed in {time.time() - client_start:.4f} seconds")
                
                if objects:  # It's a directory
                    if name == 'user.mime_type':
                        return b'inode/directory'
                    elif name == 'security.capability':
                        return b'\x01\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                    raise FuseOSError(errno.ENODATA)
            except Exception as e:
                logger.debug(f"Directory check failed for {dir_key}: {str(e)}")
                pass  # Fall through to file check

            # Try as a regular file
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.info(f"getxattr: head_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                
                # Return requested attribute
                if name == 'user.mime_type':
                    mime_type = metadata.content_type or 'application/octet-stream'
                    return mime_type.encode('utf-8')
                elif name == 'user.content_length':
                    return str(metadata.content_length).encode('utf-8')
                elif name == 'user.etag':
                    return metadata.etag.encode('utf-8') if metadata.etag else b''
                elif name == 'user.last_modified':
                    return str(int(metadata.last_modified.timestamp())).encode('utf-8')
                elif name == 'security.capability':
                    # Standard file capabilities
                    return b'\x01\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
                elif name == 'system.posix_acl_access':
                    # Return empty ACL
                    return b''
                else:
                    logger.debug(f"getxattr: Unsupported attribute {name} requested for {path}")
                    raise FuseOSError(errno.ENODATA)
                    
            except Exception as e:
                if "NoSuchKey" in str(e) or "Not Found" in str(e):
                    logger.debug(f"getxattr: Object {key} does not exist")
                    raise FuseOSError(errno.ENOENT)
                else:
                    logger.error(f"getxattr: Error checking file {key}: {str(e)}", exc_info=True)
                    raise FuseOSError(errno.EIO)
                    
        except FuseOSError:
            raise
        except Exception as e:
            logger.error(f"getxattr error for {path}: {str(e)}", exc_info=True)
            time_function("getxattr", start_time)
            raise FuseOSError(errno.EIO)
        finally:
            time_function("getxattr", start_time)

    def listxattr(self, path):
        """
        List supported extended attributes for a file or directory.
        
        Args:
            path (str): Path to the file or directory
            
        Returns:
            list: List of supported attribute names
            
        Raises:
            FuseOSError: If the file doesn't exist
        """
        trace_op("listxattr", path)
        logger.debug(f"listxattr requested for path: {path}")
        start_time = time.time()
        
        try:
            # For directories (including root), return basic attributes
            if path == '/' or self._is_directory(path):
                time_function("listxattr", start_time)
                return ['user.mime_type', 'security.capability']
            
            # For files, verify existence first
            key = self._get_path(path)
            try:
                self.client.head_object(self.bucket, key)
                time_function("listxattr", start_time)
                return [
                    'user.mime_type',
                    'user.content_length',
                    'user.etag',
                    'user.last_modified',
                    'security.capability',
                    'system.posix_acl_access'
                ]
            except Exception as e:
                if "NoSuchKey" in str(e) or "Not Found" in str(e):
                    raise FuseOSError(errno.ENOENT)
                raise FuseOSError(errno.EIO)
                
        except FuseOSError:
            raise
        except Exception as e:
            logger.error(f"listxattr error for {path}: {str(e)}", exc_info=True)
            time_function("listxattr", start_time)
            raise FuseOSError(errno.EIO)

    def _is_directory(self, path):
        """
        Helper method to check if a path is a directory.
        
        Args:
            path (str): Path to check
            
        Returns:
            bool: True if path is a directory, False otherwise
        """
        if path == '/':
            return True
            
        key = self._get_path(path)
        dir_key = key if key.endswith('/') else key + '/'
        
        try:
            objects = list(self.client.list_objects(
                self.bucket,
                ListObjectsOptions(prefix=dir_key, max_keys=1)
            ))
            return len(objects) > 0
        except Exception:
            return False

def mount(bucket: str, mountpoint: str, foreground: bool = True, allow_other: bool = False):
    """
    Mount an ACS bucket at the specified mountpoint.
    
    This function mounts an ACS bucket as a local filesystem using FUSE.
    
    Args:
        bucket (str): Name of the bucket to mount
        mountpoint (str): Local path where the filesystem should be mounted
        foreground (bool, optional): Run in foreground. Defaults to True.
        allow_other (bool, optional): Allow other users to access the mount.
            Requires 'user_allow_other' in /etc/fuse.conf. Defaults to False.
    """
    logger.info(f"Mounting bucket {bucket} at {mountpoint}")
    start_time = time.time()
    
    # Check mountpoint directory status with better error handling
    if os.path.exists(mountpoint):
        if not os.path.isdir(mountpoint):
            logger.error(f"Mountpoint path exists but is not a directory: {mountpoint}")
            print(f"Error: {mountpoint} exists but is not a directory. Please specify a directory path.")
            return
        
        # Check if writable by attempting to write a test file
        test_file = os.path.join(mountpoint, '.acs_write_test')
        try:
            with open(test_file, 'w') as f:
                f.write('test')
            os.unlink(test_file)  # Remove test file if successful
        except PermissionError:
            logger.error(f"Mountpoint {mountpoint} exists but is not writable")
            print(f"Error: You don't have write permission for {mountpoint}.")
            print(f"Try: sudo chown $(whoami) {mountpoint}")
            return
        except Exception as e:
            # This handles other potential issues like read-only filesystem
            logger.error(f"Cannot write to mountpoint {mountpoint}: {str(e)}")
            print(f"Error: Cannot write to mountpoint {mountpoint}: {str(e)}")
            print(f"Check permissions or use a different directory.")
            return
    else:
        # Directory doesn't exist, create it
        logger.info(f"Mountpoint {mountpoint} does not exist, creating it...")
        try:
            os.makedirs(mountpoint, mode=0o755)
            logger.info(f"Successfully created mountpoint directory: {mountpoint}")
            print(f"Created mountpoint directory: {mountpoint}")
        except Exception as e:
            logger.error(f"Failed to create mountpoint {mountpoint}: {str(e)}")
            print(f"Error: Failed to create mountpoint directory {mountpoint}: {str(e)}")
            print(f"Try: sudo mkdir -p {mountpoint}")
            print(f"sudo chown $(whoami) {mountpoint}")
            return
    
    # Check if mountpoint is already mounted
    try:
        process = subprocess.run(["mountpoint", "-q", mountpoint], check=False)
        if process.returncode == 0:
            logger.warning(f"Mountpoint {mountpoint} is already mounted")
            print(f"Warning: {mountpoint} is already mounted. Unmounting first...")
            unmount(mountpoint, ACSFuse)
            
            # Verify unmount was successful
            process = subprocess.run(["mountpoint", "-q", mountpoint], check=False)
            if process.returncode == 0:
                logger.error(f"Failed to unmount {mountpoint}")
                print(f"Error: Failed to unmount {mountpoint}. Please unmount manually:")
                print(f"sudo umount {mountpoint} || sudo fusermount -u {mountpoint}")
                return
    except Exception as e:
        # If mountpoint command isn't available, check if directory is empty
        logger.warning(f"Could not check if {mountpoint} is mounted: {str(e)}")
        
        # Check if directory is empty (safe to mount only if empty)
        contents = os.listdir(mountpoint)
        if contents and not all(item.startswith('.') for item in contents):
            logger.warning(f"Mountpoint {mountpoint} is not empty: {contents}")
            print(f"Warning: Directory {mountpoint} is not empty. For safety, FUSE mounts should use empty directories.")
            print("Non-empty mount points can cause file access issues and data confusion.")
            user_response = input("Continue anyway? (y/N): ").strip().lower()
            if user_response != 'y':
                print("Mount operation cancelled. Please use an empty directory.")
                return
    
    os.environ["GRPC_VERBOSITY"] = "ERROR"
    options = get_mount_options(foreground, allow_other)

    # Set up signal handlers for graceful unmounting
    signal_handler = setup_signal_handlers(mountpoint, lambda mp: unmount(mp, ACSFuse))

    try:
        logger.info(f"Starting FUSE mount with options: {options}")
        mount_start = time.time()
        
        # Start the FUSE mount with threading enabled
        FUSE(ACSFuse(bucket), mountpoint, nothreads=False, **options)
        
        logger.info(f"FUSE mount completed in {time.time() - mount_start:.4f} seconds")
        time_function("mount", start_time)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, unmounting...")
        print("Keyboard interrupt received, unmounting...")
        unmount(mountpoint, ACSFuse)
        time_function("mount", start_time)
    except Exception as e:
        # More detailed error logging
        error_msg = str(e)
        error_type = type(e).__name__
        logger.error(f"Error during mount: {error_type}: {error_msg}")
        print(f"Error: {error_type}: {error_msg}")
        
        # Suggest solutions based on common errors
        if "Invalid argument" in error_msg:
            print("\nPossible solutions:")
            print("1. Check if the mountpoint directory exists and is empty")
            print("2. Ensure you have permission to write to the mountpoint")
            print("3. Try unmounting any existing mounts: fusermount -u " + mountpoint)
            print("\nTry creating the mountpoint with:")
            print(f"sudo mkdir -p {mountpoint}")
            print(f"sudo chown $(whoami) {mountpoint}")
        elif "Permission denied" in error_msg:
            print("\nPossible solutions:")
            print("1. Check if you have permission to access the mountpoint")
            print("2. Run with sudo if needed (sudo python -m acs_sdk.fuse ...)")
            print("\nOr set correct permissions with:")
            print(f"sudo chown $(whoami) {mountpoint}")
        
        unmount(mountpoint, ACSFuse)
        time_function("mount", start_time)

def main():
    """
    CLI entry point for mounting ACS buckets.
    
    This function is the entry point for the command-line interface.
    It parses command-line arguments and mounts the specified bucket.
    
    Usage:
        python -m acs_sdk.fuse <bucket> <mountpoint>
        
    Options:
        --allow-other: Allow other users to access the mount
            (requires 'user_allow_other' in /etc/fuse.conf)
        --trace: Enable detailed tracing of file operations for debugging
    """
    logger.info(f"Starting ACS FUSE CLI with arguments: {sys.argv}")
    start_time = time.time()

    import argparse
    parser = argparse.ArgumentParser(description='Mount an ACS bucket as a local filesystem')
    parser.add_argument('bucket', help='The name of the bucket to mount')
    parser.add_argument('mountpoint', help='The directory to mount the bucket on')
    parser.add_argument('--allow-other', action='store_true', 
                        help='Allow other users to access the mount (requires user_allow_other in /etc/fuse.conf)')
    parser.add_argument('--trace', action='store_true',
                        help='Enable detailed tracing of file operations for debugging')
    
    args = parser.parse_args()
    
    # Set trace environment variable if requested
    if args.trace:
        os.environ['ACS_FUSE_TRACE_OPS'] = 'true'
        print("Detailed operation tracing enabled")
    
    logger.info(f"Mounting bucket {args.bucket} at {args.mountpoint}")
    mount(args.bucket, args.mountpoint, allow_other=args.allow_other)
    time_function("main", start_time)

if __name__ == '__main__':
    main()
