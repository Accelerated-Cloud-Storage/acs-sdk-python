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
import logging
import os
import sys 
import time 
from datetime import datetime
from acs_sdk.client.client import ACSClient
from acs_sdk.client.client import Session
from acs_sdk.client.types import ListObjectsOptions, HeadObjectOutput
from acs_sdk.client.exceptions import ObjectError
from io import BytesIO
from threading import Lock
import subprocess

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
        start_time = time.time()
        logger.info(f"ACSFuse.__init__: Initializing for bucket: {bucket_name}")
        try:
            # Get bucket region and create session with it
            temp_client = ACSClient(Session())
            
            client_start = time.time()
            bucket_info = temp_client.head_bucket(bucket_name)
            logger.debug(f"Initial head_bucket call completed in {time.time() - client_start:.4f} seconds")
            
            self.client = ACSClient(Session(region=bucket_info.region)) # Create client with bucket region
            self.bucket = bucket_name # Each mount is tied to one bucket
            
            # Initialize buffers
            self.read_buffer = ReadBuffer()
            self.write_buffer = WriteBuffer()

            # Verify bucket exists
            try:
                client_start = time.time()
                self.client.head_bucket(bucket_name)
                logger.debug(f"Verification head_bucket call completed in {time.time() - client_start:.4f} seconds")
            except Exception as e:
                logger.error(f"Failed to access bucket {bucket_name}: {str(e)}")
                raise ValueError(f"Failed to access bucket {bucket_name}: {str(e)}")
            
            logger.info(f"ACSFuse.__init__: Initialization successful for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"ACSFuse.__init__: Initialization failed: {str(e)}")
            raise # Re-raise the exception after logging
        finally:
            time_function("ACSFuse.__init__", start_time)

    def _get_path(self, path):
        """
        Convert FUSE path to ACS key.
        
        Args:
            path (str): FUSE path
            
        Returns:
            str: ACS object key
        """
        # This is a simple helper, timing might be excessive, but keeping for consistency
        start_time = time.time()
        logger.debug(f"ACSFuse._get_path: Converting path: {path}")
        result = path.lstrip('/')
        time_function("ACSFuse._get_path", start_time)
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
        start_time = time.time()
        logger.info(f"ACSFuse.getattr: Request for path: {path}, fh={fh}")
        try:
            now = datetime.now().timestamp()
            base_stat = {
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
                'st_atime': now,
                'st_mtime': now,
                'st_ctime': now,
                'st_blocks': 1,
                'st_blksize': 4096,
                'st_rdev': 0,
                'st_nlink': 1 # Default links
             }

            if path == "/":
                result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096} # S_IFDIR | 0755
                logger.info(f"ACSFuse.getattr: Returning attributes for root directory: {result}")
                return result

            key = self._get_path(path)

            # 1. Try head_object first to see if it's a file-like object
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.debug(f"getattr: head_object for {key} succeeded in {time.time() - client_start:.4f} seconds")
                # It exists as an object, report as a regular file
                result = {
                    **base_stat,
                    'st_mode': 0o100644,  # S_IFREG | 0644
                    'st_size': metadata.content_length,
                    'st_mtime': metadata.last_modified.timestamp(),
                    'st_nlink': 1
                }
                logger.info(f"ACSFuse.getattr: Determined {path} is a file. Returning attributes: {result}")
                return result
            except ObjectError as oe:
                if "not exist" in str(oe).lower():
                    logger.debug(f"getattr: Object {key} not found. Checking if it's a directory.")
                    # Proceed to directory check
                else:
                    logger.error(f"getattr: Error checking object {key}: {str(oe)}")
                    raise FuseOSError(errno.EIO) from oe
            except Exception as head_e:
                 logger.error(f"getattr: Unexpected error during head_object for {key}: {str(head_e)}")
                 raise FuseOSError(errno.EIO) from head_e

            # 2. If head_object failed with NotFound, check if it's a directory prefix
            dir_key = key if key.endswith("/") else key + "/"
            try:
                client_start = time.time()
                objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                logger.debug(f"getattr: list_objects for directory check {dir_key} completed in {time.time() - client_start:.4f} seconds")

                if objects:
                    # Found objects with this prefix, treat as directory
                    result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096} # S_IFDIR | 0755
                    logger.info(f"ACSFuse.getattr: Determined {path} is a directory. Returning attributes: {result}")
                    return result
                else:
                    # If list_objects is empty, it is not a directory either
                    logger.warning(f"ACSFuse.getattr: Path {path} not found as object or directory.")
                    raise FuseOSError(errno.ENOENT)
            except FuseOSError: # Re-raise ENOENT if thrown above
                 raise
            except Exception as dir_e:
                logger.error(f"ACSFuse.getattr: Error during directory check for {dir_key}: {str(dir_e)}")
                # If directory check fails unexpectedly, report as non-existent
                raise FuseOSError(errno.ENOENT) from dir_e

        except FuseOSError as fuse_e: # Re-raise ENOENT from inner blocks
            logger.warning(f"ACSFuse.getattr: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"ACSFuse.getattr: Unhandled error for {path}: {str(e)}", exc_info=True)
            raise FuseOSError(errno.EIO) from e # Generic I/O error for safety
        finally:
            time_function("ACSFuse.getattr", start_time)

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
        start_time = time.time()
        logger.info(f"ACSFuse.readdir: Request for path: {path}, fh={fh}")
        try:
            prefix = self._get_path(path)
            if prefix and not prefix.endswith("/") :
                prefix += "/"

            entries = {".", ".."}
            
            try:
                # Get all objects with prefix
                client_start = time.time()
                objects_iterator = self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=prefix)
                )
                logger.debug(f"readdir: list_objects call for {prefix} initiated in {time.time() - client_start:.4f} seconds")

                # Filter to get only immediate children
                seen = set()
                # Iterate through the generator safely
                for obj_key in objects_iterator:
                    if not obj_key.startswith(prefix):
                        continue

                    rel_path = obj_key[len(prefix):]
                    if not rel_path: # Skip the directory entry itself if present
                        continue

                    # Get first segment of remaining path
                    parts = rel_path.split("/", 1) # Split only once
                    entry_name = parts[0]
                    if len(parts) > 1 and parts[1]: # It's implicitly a directory
                       entry_name += "/"
                    
                    if entry_name:
                      seen.add(entry_name)

                logger.debug(f"readdir: list_objects iteration for {prefix} completed.")

                # Prepare entries
                for key in seen:
                    # Remove trailing slash for directory entries before adding
                    # FUSE expects directory names without trailing slashes in readdir results
                    if key.endswith("/") :
                        key = key[:-1]
                    if key: # Ensure no empty strings are added
                        entries.add(key)

                result = list(entries)
                logger.info(f"ACSFuse.readdir: Returning {len(result)} entries for {path}: {result}")
                return result

            except Exception as e:
                # Log the error from the list_objects call or processing
                logger.error(f"ACSFuse.readdir: Error listing objects for prefix '{prefix}': {str(e)}")
                # Still return basic entries to avoid breaking directory listing completely
                result = list(entries)
                return result
                
        except Exception as e:
            logger.error(f"ACSFuse.readdir: Unhandled error for path {path}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
             time_function("ACSFuse.readdir", start_time)

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
        start_time = time.time()
        logger.info(f"ACSFuse.rename: Request from '{old}' to '{new}'")
        old_key = self._get_path(old)
        new_key = self._get_path(new)

        try:
            # Check if source exists (using head_object is efficient)
            try:
                client_start = time.time()
                self.client.head_object(self.bucket, old_key)
                logger.debug(f"rename: head_object check for source {old_key} completed in {time.time() - client_start:.4f} seconds")
            except ObjectError as oe:
                 if "not exist" in str(oe).lower():
                    logger.error(f"ACSFuse.rename: Source path '{old}' (key: {old_key}) does not exist.")
                    raise FuseOSError(errno.ENOENT) from oe
                 else:
                    logger.error(f"ACSFuse.rename: Error checking source object {old_key}: {str(oe)}")
                    raise FuseOSError(errno.EIO) from oe
            except Exception as e:
                 logger.error(f"ACSFuse.rename: Unexpected error checking source object {old_key}: {str(e)}")
                 raise FuseOSError(errno.EIO) from e

            # If source check passes, proceed with copy and delete within the main try block
            # Perform the copy operation
            copy_source = f"{self.bucket}/{old_key}" # Format expected by copy_object
            logger.debug(f"rename: Attempting to copy from '{copy_source}' to key '{new_key}' in bucket '{self.bucket}'")
            client_start = time.time()
            self.client.copy_object(self.bucket, copy_source, new_key)
            logger.debug(f"rename: copy_object call completed in {time.time() - client_start:.4f} seconds")

            # Delete the original object
            logger.debug(f"rename: Attempting to delete original object '{old_key}'")
            client_start = time.time()
            self.client.delete_object(self.bucket, old_key)
            logger.debug(f"rename: delete_object call for {old_key} completed in {time.time() - client_start:.4f} seconds")

            logger.info(f"ACSFuse.rename: Successfully renamed '{old}' to '{new}'")

        except FuseOSError as fuse_e:
            logger.warning(f"ACSFuse.rename: Raising FuseOSError: {fuse_e.errno}")
            raise
        except Exception as e:
            logger.error(f"ACSFuse.rename: Error during rename operation from '{old}' to '{new}': {str(e)}")
            # Attempt to clean up potentially partially copied object if rename failed mid-way
            try:
                self.client.delete_object(self.bucket, new_key)
                logger.warning(f"ACSFuse.rename: Cleaned up partially created object '{new_key}' after error.")
            except Exception as cleanup_e:
                logger.warning(f"ACSFuse.rename: Error during cleanup of '{new_key}': {str(cleanup_e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.rename", start_time)

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
        start_time = time.time()
        # Add specific logging for config.json
        is_config_json = path.endswith("config.json")
        log_prefix = f"ACSFuse.read(config.json={is_config_json})" if is_config_json else "ACSFuse.read:"
        logger.info(f"{log_prefix} Request for path: {path}, size={size}, offset={offset}, fh={fh}")

        key = self._get_path(path)
        try:
            # Fast path: Check in-memory buffer first
            buffer_entry = self.read_buffer.get(key)
            data = None # Initialize data

            if buffer_entry is None:
                # Buffer miss - fetch from object storage
                logger.debug(f"{log_prefix} Buffer miss for {key}, fetching from object storage")
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    fetch_duration = time.time() - client_start
                    logger.debug(f"{log_prefix} get_object call for {key} completed in {fetch_duration:.4f} seconds. Fetched {len(data)} bytes.")
                    if is_config_json:
                         logger.debug(f"{log_prefix} Fetched config.json content (first 100 bytes): {data[:100]}")

                    # Store in buffer
                    self.read_buffer.put(key, data)
                    logger.debug(f"{log_prefix} Stored fetched data for {key} in read buffer")
                except ObjectError as oe:
                    logger.error(f"{log_prefix} Error fetching {key} from object storage: {str(oe)}")
                    raise FuseOSError(errno.EIO) from oe # Or ENOENT if it's a "not found" error
                except Exception as e:
                     logger.error(f"{log_prefix} Unexpected error fetching {key}: {str(e)}")
                     raise FuseOSError(errno.EIO) from e
            else:
                logger.debug(f"{log_prefix} Buffer hit for {key}. Buffer size: {len(buffer_entry)}")
                if is_config_json:
                     logger.debug(f"{log_prefix} Using buffered config.json content (first 100 bytes): {buffer_entry[:100]}")
                data = buffer_entry

            # Ensure data is available before proceeding
            if data is None:
                 logger.error(f"{log_prefix} Data is unexpectedly None for key {key} after fetch/buffer check.")
                 raise FuseOSError(errno.EIO)


            # Return requested portion from buffer/fetched data
            file_size = len(data)
            if offset >= file_size:
                logger.info(f"{log_prefix} Offset {offset} is beyond file size {file_size} for {path}. Returning empty bytes.")
                return b""

            end_offset = min(offset + size, file_size)
            result = data[offset:end_offset]
            bytes_read = len(result)
            logger.info(f"{log_prefix} Returning {bytes_read} bytes for {path} from offset {offset}")
            return result

        except FuseOSError as fuse_e:
             logger.warning(f"{log_prefix} Raising FuseOSError for {path}: {fuse_e.errno}")
             raise
        except Exception as e:
            logger.error(f"{log_prefix} Unexpected error reading {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function(f"ACSFuse.read ({'config.json' if is_config_json else 'other'})", start_time)

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
        start_time = time.time()
        bytes_to_write = len(data)
        logger.info(f"ACSFuse.write: Request for path: {path}, size={bytes_to_write}, offset={offset}, fh={fh}")
        key = self._get_path(path)
        try:
            # Initialize buffer if it doesn't exist
            if not self.write_buffer.has_buffer(key):
                logger.info(f"write: Buffer for {key} does not exist. Initializing...")
                # Try to fetch existing data to initialize buffer correctly
                try:
                    client_start = time.time()
                    current_data = self.client.get_object(self.bucket, key)
                    logger.debug(f"write: get_object call for {key} completed in {time.time() - client_start:.4f} seconds")
                    logger.info(f"write: Initializing buffer for {key} with {len(current_data)} existing bytes.")
                except ObjectError as oe:
                     if "not exist" in str(oe).lower():
                         logger.info(f"write: No existing object found for {key}, initializing empty buffer.")
                         current_data = b""
                     else:
                         logger.error(f"write: Error fetching initial data for {key}: {str(oe)}")
                         raise FuseOSError(errno.EIO) from oe
                except Exception as e:
                     logger.error(f"write: Unexpected error fetching initial data for {key}: {str(e)}")
                     raise FuseOSError(errno.EIO) from e

                self.write_buffer.initialize_buffer(key, current_data)

            # Write data to buffer
            bytes_written = self.write_buffer.write(key, data, offset)
            logger.debug(f"write: Wrote {bytes_written} bytes to buffer for {key} at offset {offset}")

            # Invalidate read buffer entry since file has changed
            logger.debug(f"write: Invalidating read buffer for {key}")
            self.read_buffer.remove(key)

            if bytes_written != bytes_to_write:
                 logger.warning(f"write: Expected to write {bytes_to_write} bytes but wrote {bytes_written} for {key}")
                 # FUSE expects the number of bytes requested to be returned if successful

            logger.info(f"ACSFuse.write: Successfully buffered {bytes_written} bytes for {path}")
            return bytes_written # Return number of bytes written

        except FuseOSError as fuse_e:
            logger.warning(f"ACSFuse.write: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            logger.error(f"ACSFuse.write: Unexpected error writing to buffer for {path}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.write", start_time)

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
            int: File handle (typically 0 for FUSE operations that don't need a real OS handle)
            
        Raises:
            FuseOSError: If an error occurs while creating the file
        """
        start_time = time.time()
        logger.info(f"ACSFuse.create: Request for path: {path}, mode={oct(mode)}, fi={fi}")
        key = self._get_path(path)
        try:
            # Create empty object in Object Storage first to ensure the key exists
            logger.debug(f"create: Putting empty object for key {key}")
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.debug(f"create: put_object call for empty {key} completed in {time.time() - client_start:.4f} seconds")

            # Initialize buffer for subsequent writes
            logger.debug(f"create: Initializing empty write buffer for {key}")
            self.write_buffer.initialize_buffer(key)

            logger.info(f"ACSFuse.create: Successfully created file {path}")
            # FUSE create expects a file handle (or 0)
            # We don't manage OS file handles, so return 0
            return 0
        except Exception as e:
            logger.error(f"ACSFuse.create: Error creating {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
             time_function("ACSFuse.create", start_time)

    def unlink(self, path):
        """
        Delete a file if it exists.
        
        This method deletes a file from the object storage.
        
        Args:
            path (str): Path to the file
            
        Raises:
            FuseOSError: If an error occurs while deleting the file
        """
        start_time = time.time()
        logger.info(f"ACSFuse.unlink: Request for path: {path}")
        key = self._get_path(path)
        try:
            logger.debug(f"unlink: Attempting to delete object {key}")
            client_start = time.time()
            self.client.delete_object(self.bucket, key)
            logger.debug(f"unlink: delete_object call for {key} completed in {time.time() - client_start:.4f} seconds")

            # Also remove from buffers if present
            self.read_buffer.remove(key)
            self.write_buffer.remove(key)
            logger.debug(f"unlink: Removed {key} from read/write buffers")

            logger.info(f"ACSFuse.unlink: Successfully deleted file {path}")
        except ObjectError as oe:
             if "not exist" in str(oe).lower():
                 logger.warning(f"ACSFuse.unlink: Object {key} does not exist, treating as success (idempotent).")
                 # Unlink should succeed if the file doesn't exist (ENOENT is often mapped to success here)
                 pass # Succeed quietly
             else:
                 logger.error(f"ACSFuse.unlink: Error deleting {key}: {str(oe)}")
                 raise FuseOSError(errno.EIO) from oe
        except Exception as e:
            logger.error(f"ACSFuse.unlink: Unexpected error unlinking {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.unlink", start_time)

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
        start_time = time.time()
        logger.info(f"ACSFuse.mkdir: Request for path: {path}, mode={oct(mode)}")
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            # Check if it already exists (as a file or directory)
            try:
                self.client.head_object(self.bucket, key)
                logger.warning(f"mkdir: Directory object {key} already exists.")
                raise FuseOSError(errno.EEXIST)
            except ObjectError as oe:
                if "not exist" not in str(oe).lower():
                    # Error other than "not found" during check
                    logger.error(f"mkdir: Error checking existence of {key}: {str(oe)}")
                    raise FuseOSError(errno.EIO) from oe
                # If it doesn't exist, proceed to create
            except Exception as e:
                  logger.error(f"mkdir: Unexpected error checking existence of {key}: {str(e)}")
                  raise FuseOSError(errno.EIO) from e

            # Create the directory marker object
            logger.debug(f"mkdir: Putting empty object for directory marker {key}")
            client_start = time.time()
            self.client.put_object(self.bucket, key, b"")
            logger.debug(f"mkdir: put_object call for directory {key} completed in {time.time() - client_start:.4f} seconds")

            logger.info(f"ACSFuse.mkdir: Successfully created directory {path}")
        except FuseOSError as fuse_e:
             logger.warning(f"ACSFuse.mkdir: Raising FuseOSError for {path}: {fuse_e.errno}")
             raise
        except Exception as e:
            logger.error(f"ACSFuse.mkdir: Error creating directory {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.mkdir", start_time)

    def rmdir(self, path):
        """
        Remove a directory.
        
        This method removes a directory if it is empty.
        
        Args:
            path (str): Path to the directory
            
        Raises:
            FuseOSError: If the directory is not empty or an error occurs
        """
        start_time = time.time()
        logger.info(f"ACSFuse.rmdir: Request for path: {path}")
        key = self._get_path(path)
        if not key.endswith('/'):
            key += '/'
            
        try:
            # Check if directory is empty by listing objects with its prefix
            logger.debug(f"rmdir: Checking if directory {key} is empty")
            client_start = time.time()
            # We need to list with the prefix. If we find more than just the directory marker
            # itself, it's not empty. List with max_keys=2 for efficiency.
            contents = list(self.client.list_objects(
                self.bucket,
                ListObjectsOptions(prefix=key, max_keys=2)
            ))
            logger.debug(f"rmdir: list_objects call for {key} completed in {time.time() - client_start:.4f} seconds. Found {len(contents)} items.")

            # Check contents:
            # - If 0 items: Directory marker doesn't even exist (treat as ENOENT)
            # - If 1 item and it's exactly the key: Directory is empty
            # - If > 1 item OR 1 item that is NOT the key: Directory is not empty
            if not contents:
                 logger.warning(f"rmdir: Directory marker {key} not found.")
                 raise FuseOSError(errno.ENOENT)
            elif len(contents) > 1 or contents[0] != key:
                logger.warning(f"rmdir: Directory {key} is not empty (found: {contents}), cannot remove")
                raise FuseOSError(errno.ENOTEMPTY)

            # Directory is empty, delete the marker object
            logger.debug(f"rmdir: Directory {key} is empty. Deleting marker object.")
            client_start = time.time()
            self.client.delete_object(self.bucket, key)
            logger.debug(f"rmdir: delete_object call for directory {key} completed in {time.time() - client_start:.4f} seconds")

            logger.info(f"ACSFuse.rmdir: Successfully removed directory {path}")
        except FuseOSError as fuse_e:
            # Re-raise specific FUSE errors like ENOENT or ENOTEMPTY
            logger.warning(f"ACSFuse.rmdir: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            logger.error(f"ACSFuse.rmdir: Error removing directory {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.rmdir", start_time)

    def truncate(self, path, length, fh=None):
        """
        Truncate file to specified length.
        
        This method changes the size of a file by either truncating it
        or extending it with null bytes. It operates on the write buffer.
        
        Args:
            path (str): Path to the file
            length (int): New length of the file
            fh (int, optional): File handle. Defaults to None.
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If an error occurs while truncating the file
        """
        start_time = time.time()
        logger.info(f"ACSFuse.truncate: Request for path: {path}, length={length}, fh={fh}")
        key = self._get_path(path)
        try:
            # Ensure the write buffer is initialized for this key
            if not self.write_buffer.has_buffer(key):
                logger.debug(f"truncate: Write buffer for {key} not found. Initializing...")
                try:
                    client_start = time.time()
                    data = self.client.get_object(self.bucket, key)
                    logger.debug(f"truncate: get_object call for {key} completed in {time.time() - client_start:.4f} seconds. Size: {len(data)}")
                except ObjectError as oe:
                     if "not exist" in str(oe).lower():
                         logger.info(f"truncate: Object {key} doesn't exist. Initializing empty buffer.")
                         data = b""
                     else:
                         logger.error(f"truncate: Error fetching data for {key}: {str(oe)}")
                         raise FuseOSError(errno.EIO) from oe
                except Exception as e:
                     logger.error(f"truncate: Unexpected error fetching data for {key}: {str(e)}")
                     raise FuseOSError(errno.EIO) from e
                self.write_buffer.initialize_buffer(key, data)
            else:
                 logger.debug(f"truncate: Write buffer already exists for {key}.")


            # Truncate the buffer
            logger.debug(f"truncate: Truncating buffer for {key} to length {length}")
            self.write_buffer.truncate(key, length)

            # Invalidate read buffer as content might have changed
            logger.debug(f"truncate: Invalidating read buffer for {key}")
            self.read_buffer.remove(key)

            logger.info(f"ACSFuse.truncate: Successfully truncated {path} to length {length} in buffer")
            return 0 # Success
        except FuseOSError as fuse_e:
             logger.warning(f"ACSFuse.truncate: Raising FuseOSError for {path}: {fuse_e.errno}")
             raise
        except Exception as e:
            logger.error(f"ACSFuse.truncate: Error truncating {key}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
             time_function("ACSFuse.truncate", start_time)
    
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
        start_time = time.time()
        logger.info(f"ACSFuse.release: Request for path: {path}, fh={fh}")
        
        try:
            # Called after the last file descriptor is closed
            logger.debug(f"release: Flushing write buffer for {path}")
            flush_start = time.time()
            self._flush_buffer(path)
            logger.debug(f"release: Buffer flush completed in {time.time() - flush_start:.4f} seconds")
            
            key = self._get_path(path)
            
            # Remove from write buffer
            logger.debug(f"release: Removing {key} from write buffer")
            wb_start = time.time()
            self.write_buffer.remove(key)
            logger.debug(f"release: Write buffer removal completed in {time.time() - wb_start:.4f} seconds")
            
            # Remove from read buffer when file is no longer being accessed
            logger.debug(f"release: Removing {key} from read buffer")
            rb_start = time.time()
            self.read_buffer.remove(key)
            logger.debug(f"release: Read buffer removal completed in {time.time() - rb_start:.4f} seconds")
            
            logger.info(f"ACSFuse.release: Successfully released {path}")
            return 0
            
        except Exception as e:
            logger.error(f"ACSFuse.release: Error releasing {path}: {str(e)}")
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.release", start_time)

    def link(self, target, name):
        """
        Create hard link by copying the object.
        
        This method creates a hard link by copying the object to a new key,
        since true hard links aren't supported in object storage.
        
        Args:
            target (str): Path to the target file (source of the link)
            name (str): Path to the new link (destination)
            
        Returns:
            int: 0 on success
            
        Raises:
            FuseOSError: If the target file does not exist or an error occurs
        """
        start_time = time.time()
        logger.info(f"ACSFuse.link: Request to link target='{target}' to name='{name}'")
        target_key = self._get_path(target)
        new_key = self._get_path(name)

        try:
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
            
            time_function("ACSFuse.link", start_time)
            return 0
        except Exception as e:
            logger.error(f"ACSFuse.link: Error creating link from {target} to {name}: {str(e)}")
            time_function("ACSFuse.link", start_time)
            raise FuseOSError(errno.EIO) from e

    def lstat(self, path):
        start_time = time.time()
        logger.info(f"ACSFuse.lstat: Request for path: {path}")
        try:
            now = datetime.now().timestamp()
            base_stat = {
                'st_uid': os.getuid(),
                'st_gid': os.getgid(),
                'st_atime': now,
                'st_ctime': now,
                'st_mtime': now, # Default mtime, overwrite below
                'st_blocks': 1,
                'st_blksize': 4096,
                'st_rdev': 0,
                'st_nlink': 1
            }

            if path == "/":
                result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096} # S_IFDIR | 0755
                logger.info(f"ACSFuse.lstat: Returning root directory attributes: {result}")
                return result

            key = self._get_path(path)
            logger.debug(f"lstat: Converted path to key: {key}")

            # 1. Try head_object first to check for a file-like object
            try:
                client_start = time.time()
                metadata = self.client.head_object(self.bucket, key)
                logger.debug(f"lstat: head_object for {key} succeeded in {time.time() - client_start:.4f} seconds")

                # Simplified logic: Report as a regular file. readlink will handle if it's actually a symlink.
                result = {
                    **base_stat,
                    'st_mode': 0o100644,  # S_IFREG | 0644
                    'st_size': metadata.content_length,
                    'st_mtime': metadata.last_modified.timestamp(),
                    'st_nlink': 1,
                    'st_blocks': (metadata.content_length + base_stat['st_blksize'] - 1) // base_stat['st_blksize']
                }
                logger.info(f"ACSFuse.lstat: Determined {path} is an object (reporting as regular file). Returning attributes: {result}")
                return result

            except ObjectError as oe:
                if "not exist" in str(oe).lower():
                    logger.debug(f"lstat: Object {key} not found via head_object. Checking if it's a directory.")
                    # Proceed to directory check below
                else:
                    # Log unexpected errors during head_object but proceed to directory check
                    logger.warning(f"lstat: Error checking object {key}: {str(oe)}. Proceeding to check directory.")
                    # Fall through to directory check, maybe list_objects will succeed?
            except Exception as head_e:
                 logger.warning(f"lstat: Unexpected error checking object {key}: {str(head_e)}. Proceeding to check directory.", exc_info=True)
                 # Fall through

            # 2. If not found as an object or head failed, check if it's a directory prefix
            dir_key = key if key.endswith("/") else key + "/"
            try:
                client_start = time.time()
                objects = list(self.client.list_objects(
                    self.bucket,
                    ListObjectsOptions(prefix=dir_key, max_keys=1)
                ))
                logger.debug(f"lstat: list_objects for directory check {dir_key} completed in {time.time() - client_start:.4f} seconds")

                if objects:  # If we found any objects with this prefix, it's a directory
                    result = {**base_stat, 'st_mode': 0o40755, 'st_nlink': 2, 'st_size': 4096} # S_IFDIR | 0755
                    logger.info(f"ACSFuse.lstat: Determined {path} is a directory. Returning attributes: {result}")
                    return result
                else:
                    # If list_objects is empty, it is not a directory either
                    logger.warning(f"ACSFuse.lstat: Path {path} not found as object or directory.")
                    raise FuseOSError(errno.ENOENT)
            except FuseOSError: # Re-raise ENOENT if thrown above
                 raise
            except Exception as dir_e:
                logger.error(f"ACSFuse.lstat: Error during directory check for {dir_key}: {str(dir_e)}")
                # If directory check fails unexpectedly, report as non-existent
                raise FuseOSError(errno.ENOENT) from dir_e

        except FuseOSError as fuse_e: # Re-raise ENOENT from inner blocks
            logger.warning(f"ACSFuse.lstat: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error(f"ACSFuse.lstat: Unhandled error for {path}: {str(e)}", exc_info=True)
            raise FuseOSError(errno.EIO) from e # Generic I/O error for safety
        finally:
            time_function("ACSFuse.lstat", start_time)

    def statvfs(self, path):
        """
        Get filesystem statistics (similar to statvfs but older interface).

        Called for statfs/fstatfs system calls. Critical for some libraries checking disk space.

        Args:
            path (str): Path to get statistics for (often '/')

        Returns:
            dict: A dictionary containing filesystem statistics (keys match C struct).
                  Keys like 'f_bsize', 'f_blocks', 'f_bfree', etc.
        """
        start_time = time.time()
        logger.info(f"ACSFuse.statvfs: Request for path: {path}")
        # This method often uses the same underlying logic as statvfs.
        # We can delegate to statvfs or replicate the logic. Replicating for clarity:
        try:
            # Use realistic values that won't cause integer overflow
            # 5PB total space with 4.5PB free seems reasonable for cloud storage
            block_size = 4096                        # Standard block size
            total_blocks = 5 * (1024**5) // block_size # ~5 Petabytes
            free_blocks = int(total_blocks * 0.9)      # ~4.5 Petabytes free (90% free)

            result = {
                'f_bsize': block_size,               # Filesystem block size
                'f_frsize': block_size,              # Fundamental filesystem block size
                'f_blocks': total_blocks,            # Total number of blocks in filesystem
                'f_bfree': free_blocks,              # Total number of free blocks
                'f_bavail': free_blocks,             # Free blocks available to unprivileged user
                'f_files': 10**9,                    # Total number of file nodes in filesystem (large number)
                'f_ffree': int(10**9 * 0.9),         # Total number of free file nodes
                'f_favail': int(10**9 * 0.9),        # Free file nodes available to non-superuser
                'f_fsid': 42,                        # Filesystem ID - using a consistent dummy value
                'f_flag': 0,                         # Mount flags (e.g., ST_NOSUID) - 0 for standard
                'f_namemax': 255,                    # Maximum filename length
            }

            # Calculate human-readable values for logging
            total_pb = (total_blocks * block_size) / (1024**5)
            free_pb = (free_blocks * block_size) / (1024**5)

            logger.info(f"ACSFuse.statvfs: Reporting: {total_pb:.2f}PB total, {free_pb:.2f}PB free ({(free_pb/total_pb)*100:.1f}% free)")

            return result
        except Exception as e:
             logger.error(f"ACSFuse.statvfs: Error calculating statvfs for {path}: {str(e)}")
             # statvfs shouldn't really fail, but return dummy EIO if it does
             # Returning a dictionary of zeros might be safer?
             # Let's return EIO for now.
             # Returning 0s: return {k: 0 for k in ['f_bsize', 'f_frsize', 'f_blocks', ...]}
             raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.statvfs", start_time)

    def symlink(self, target, source): # Note: FUSE arguments are target, source (link_path, target_path)
        """
        Create a symbolic link. (target -> source)

        FUSE passes arguments as `symlink(target, source)`, where `target` is the path
        of the link itself, and `source` is the path the link should point to.

        We implement this by creating an object at the `target` path whose content
        is the `source` path string.

        Args:
            target (str): Path where the symlink file is created (link name)
            source (str): Path the symlink should point to (content of the link)

        Returns:
            int: 0 on success
        """
        start_time = time.time()
        logger.info(f"ACSFuse.symlink: Request to create link at '{target}' pointing to '{source}'")
        try:
            # Target path is where the symlink "file" will be created
            target_key = self._get_path(target)

            # The *content* of the symlink file will be the source path string
            symlink_content = source.encode("utf-8")

            # Write the symlink object
            logger.debug(f"symlink: Creating symlink object at {target_key} with content '{source}'")
            client_start = time.time()
            self.client.put_object(self.bucket, target_key, symlink_content)
            logger.info(f"put_object call for symlink {target_key} completed in {time.time() - client_start:.4f} seconds")
            logger.info(f"ACSFuse.symlink: Successfully created symlink '{target}' -> '{source}'")

            return 0
        except Exception as e:
            logger.error(f"ACSFuse.symlink: Error creating symlink from '{source}' to '{target}': {str(e)}", exc_info=True)
            raise FuseOSError(errno.EIO) from e
        finally:
            time_function("ACSFuse.symlink", start_time)

    def readlink(self, path):
        """
        Read the target of a symbolic link.
        
        Retrieves the content of the object representing the symlink,
        which contains the source path string.
        
        Args:
            path (str): Path to the symlink
            
        Returns:
            str: Target path of the symlink
            
        Raises:
            FuseOSError: If the path is not a symlink or an error occurs
        """
        start_time = time.time()
        logger.info(f"ACSFuse.readlink: Request for path: {path}")
        key = self._get_path(path)
        try:
            # Attempt to get the object content. If it looks like a path, return it.
            # If it doesn't exist, or content is invalid, raise appropriate error.
            logger.debug(f"readlink: Getting object content for potential symlink key: {key}")
            client_start = time.time()
            data = self.client.get_object(self.bucket, key)
            logger.debug(f"readlink: get_object call for {key} completed in {time.time() - client_start:.4f} seconds")

            # Try decoding the content as a path
            try:
                source_path = data.decode("utf-8")
                # Basic check: does it contain null bytes? If so, likely not a path.
                if "\x00" in source_path:
                     logger.warning(f"readlink: Content of {key} contains null bytes, not a valid symlink target.")
                     raise FuseOSError(errno.EINVAL)

                logger.info(f"ACSFuse.readlink: Returning target path 	{source_path}	 for link 	{path}	")
                return source_path
            except UnicodeDecodeError:
                 logger.warning(f"readlink: Content of {key} is not valid UTF-8, cannot be a symlink target.")
                 raise FuseOSError(errno.EINVAL)

        except ObjectError as oe:
            # Handle cases where the object itself doesn't exist
            logger.error(f"ACSFuse.readlink: Object {key} not found or error reading content: {str(oe)}")
            if "not exist" in str(oe).lower():
                 raise FuseOSError(errno.ENOENT) from oe
            else:
                 # Other errors during get_object (e.g., permission denied)
                 raise FuseOSError(errno.EIO) from oe # Generic I/O error might be suitable
        except FuseOSError as fuse_e:
            # Re-raise EINVAL or ENOENT from checks above
            logger.warning(f"ACSFuse.readlink: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            logger.error(f"ACSFuse.readlink: Unexpected error reading symlink {key}: {str(e)}", exc_info=True)
            raise FuseOSError(errno.EINVAL) from e # Default to invalid argument
        finally:
            time_function("ACSFuse.readlink", start_time)

    def access(self, path, mode):
        """
        Check if a file can be accessed with the given mode.
        NOTE: We only check for existence (F_OK). Permissions are simplified.

        Args:
            path (str): Path to the file or directory
            mode (int): Access mode (os.F_OK, os.R_OK, os.W_OK, os.X_OK)

        Returns:
            int: 0 on success

        Raises:
            FuseOSError: If the file doesn't exist (ENOENT) or other access check fails (EACCES).
        """
        start_time = time.time()
        logger.info(f"ACSFuse.access: Request for path: {path}, mode={mode}")
        try:
            # F_OK (0) checks existence. This is the primary check we can reliably do.
            # We can use getattr for this, as it checks both files and directories.
            self.getattr(path) # Will raise ENOENT if not found
            logger.info(f"ACSFuse.access: Path {path} found. Granting access for mode {mode}.")
            # If getattr succeeds, the path exists. We simplify permissions check:
            # Allow R_OK and W_OK if it exists. X_OK might need checking if it's a directory.
            # For now, return 0 (success) if F_OK check (implicit in getattr) passes.
            return 0 # Success

        except FuseOSError as fuse_e:
            # Propagate ENOENT from getattr
            logger.warning(f"ACSFuse.access: Raising FuseOSError for {path}: {fuse_e.errno}")
            raise
        except Exception as e:
            logger.error(f"ACSFuse.access: Unexpected error checking access for {path}: {str(e)}")
            raise FuseOSError(errno.EACCES) from e # Default to access denied on unexpected errors
        finally:
            time_function("ACSFuse.access", start_time)

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
     mount_func_start_time = time.time() # Timer for the whole mount function
     logger.info(f"mount: Starting mount process for bucket '{bucket}' at '{mountpoint}' (foreground={foreground}, allow_other={allow_other})")

     # Check mountpoint directory status with better error handling
     if os.path.exists(mountpoint):
         if not os.path.isdir(mountpoint):
             logger.error(f"Mountpoint path exists but is not a directory: {mountpoint}")
             print(f"Error: {mountpoint} exists but is not a directory. Please specify a directory path.")
             time_function("mount", mount_func_start_time)
             return

         # Check if writable by attempting to write a test file
         test_file = os.path.join(mountpoint, '.acs_write_test')
         try:
             with open(test_file, 'w') as f:
                 f.write('test')
             os.unlink(test_file)  # Remove test file if successful
             logger.debug(f"Mountpoint {mountpoint} is writable.")
         except PermissionError:
             logger.error(f"Mountpoint {mountpoint} exists but is not writable")
             print(f"Error: You don't have write permission for {mountpoint}.")
             print(f"Try: sudo chown $(whoami) {mountpoint}")
             time_function("mount", mount_func_start_time)
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
             time_function("mount", mount_func_start_time)
             return

     # Check if mountpoint is already mounted
     try:
         process = subprocess.run(["mountpoint", "-q", mountpoint], check=False)
         if process.returncode == 0:
             logger.warning(f"Mountpoint {mountpoint} is already mounted. Unmounting first...")
             print(f"Warning: {mountpoint} is already mounted. Unmounting first...")
             unmount_start = time.time()
             unmount(mountpoint, ACSFuse) # Pass the class for potential buffer clearing
             logger.info(f"Previous unmount operation took {time.time() - unmount_start:.4f} seconds.")

             # Verify unmount was successful
             process = subprocess.run(["mountpoint", "-q", mountpoint], check=False)
             if process.returncode == 0:
                 logger.error(f"Failed to unmount {mountpoint} automatically.")
                 print(f"Error: Failed to unmount {mountpoint}. Please unmount manually:")
                 print(f"  sudo umount {mountpoint} || sudo fusermount -u {mountpoint}")
                 time_function("mount", mount_func_start_time)
                 return
             else:
                  logger.info(f"Successfully unmounted previous instance at {mountpoint}.")
     except FileNotFoundError:
         logger.warning(f"'mountpoint' command not found. Cannot reliably check if already mounted. Proceeding with caution.")
     except Exception as e:
         # Handle other errors during mountpoint check/unmount
         logger.warning(f"Could not check/unmount {mountpoint}: {str(e)}. Proceeding with caution.")

     # Check directory emptiness *after* attempting unmount
     try:
         contents = os.listdir(mountpoint)
         # Allow hidden files (like .DS_Store)
         non_hidden_contents = [item for item in contents if not item.startswith('.')]
         if non_hidden_contents:
             logger.warning(f"Mountpoint {mountpoint} is not empty after unmount attempt: {non_hidden_contents}")
             print(f"Warning: Directory {mountpoint} is not empty. FUSE mounts work best on empty directories.")
             print("Mounting on a non-empty directory can hide existing files and cause confusion.")
             try:
                  user_response = input("Continue anyway? (y/N): ").strip().lower()
                  if user_response != 'y':
                      print("Mount operation cancelled. Please use an empty directory or clear the existing one.")
                      time_function("mount", mount_func_start_time)
                      return
                  else:
                       logger.info("User chose to continue mounting on non-empty directory.")
             except EOFError: # Handle non-interactive environments
                  logger.warning("Non-interactive environment detected. Continuing mount on non-empty directory.")
                  print("Warning: Non-interactive environment. Continuing mount on non-empty directory.")
     except Exception as e:
          logger.error(f"Could not check contents of mountpoint {mountpoint}: {str(e)}")
          print(f"Error: Could not check contents of mountpoint {mountpoint}: {str(e)}")
          time_function("mount", mount_func_start_time)
          return

     os.environ["GRPC_VERBOSITY"] = "ERROR" # Keep gRPC logs quiet unless needed
     # Get mount options from mount_utils
     options = get_mount_options(foreground=foreground)
     if allow_other:
         options['allow_other'] = True
         logger.info("Enabled 'allow_other' mount option.")
         print("Note: 'allow_other' requires 'user_allow_other' in /etc/fuse.conf")


     # Set up signal handlers for graceful unmounting
     logger.debug("Setting up signal handlers (SIGINT, SIGTERM)...")
     setup_signal_handlers(mountpoint, lambda mp: unmount(mp, ACSFuse))

     fuse_instance = None
     try:
         logger.info(f"Initializing ACSFuse for bucket '{bucket}'...")
         init_start = time.time()
         fuse_instance = ACSFuse(bucket) # Initialize FUSE operations class
         logger.info(f"ACSFuse initialization completed in {time.time() - init_start:.4f} seconds")

         logger.info(f"Starting FUSE mount at {mountpoint} with options: {options}")
         print(f"Mounting ACS bucket '{bucket}' at '{mountpoint}'...")
         fuse_start_time = time.time()

         # Start the FUSE mount with threading enabled (nothreads=False is default and recommended)
         # Pass the instantiated fuse_instance
         FUSE(fuse_instance, mountpoint, nothreads=False, **options)

         # This line is reached only when FUSE exits (e.g., unmounted or foreground process terminated)
         logger.info(f"FUSE process for {mountpoint} finished after {time.time() - fuse_start_time:.4f} seconds.")
         print(f"Filesystem at {mountpoint} has been unmounted.")

     except (KeyboardInterrupt, SystemExit):
        logger.info("Received KeyboardInterrupt or SystemExit. Initiating unmount...")
        print("\nInterrupt received, unmounting...")
        if mountpoint: # Ensure mountpoint is defined
            unmount(mountpoint, ACSFuse)
     except ValueError as ve: # Catch initialization errors from ACSFuse.__init__
         logger.error(f"Mount initialization failed: {str(ve)}")
         print(f"Error: Mount initialization failed: {str(ve)}")
         # No need to unmount if initialization failed before FUSE started
     except RuntimeError as rt_err: # Catch FUSE-specific runtime errors
         logger.error(f"FUSE runtime error: {str(rt_err)}")
         print(f"Error: FUSE runtime error: {str(rt_err)}")
         # Attempt unmount if FUSE might have partially started
         if mountpoint:
             logger.info("Attempting cleanup unmount after error...")
             unmount(mountpoint, ACSFuse)
     except Exception as e:
         # Catch-all for other unexpected errors during mount setup or run
         error_msg = str(e)
         error_type = type(e).__name__
         logger.error(f"Unhandled error during mount: {error_type}: {error_msg}", exc_info=True) # Log traceback
         print(f"\nAn unexpected error occurred: {error_type}: {error_msg}")

         # Provide context-specific suggestions
         if "Transport endpoint is not connected" in error_msg or "Broken pipe" in error_msg:
              print("\nThis might indicate the mount process terminated unexpectedly or the connection was lost.")
              print("Check system logs for more details.")
         elif "Invalid argument" in error_msg:
             print("\nThis often means an issue with the mountpoint or FUSE options.")
             print(" - Ensure the mountpoint directory exists and is empty.")
             print(" - Check FUSE installation and permissions.")
             print(" - Try unmounting manually: fusermount -u " + mountpoint)
         elif "Permission denied" in error_msg:
             print("\nPermission denied.")
             print(" - Check permissions for the mountpoint directory.")
             print(f" - Try: sudo chown $(whoami) {mountpoint}")
             print(" - If using 'allow_other', ensure 'user_allow_other' is set in /etc/fuse.conf.")
             print(" - Consider running the mount command with sudo if necessary.")

         # Attempt cleanup unmount regardless of error type
         if mountpoint:
             logger.info("Attempting cleanup unmount after error...")
             unmount(mountpoint, ACSFuse)
     finally:
          # Final log message for the mount function itself
          logger.info(f"mount: Function finished for {mountpoint}.")
          time_function("mount", mount_func_start_time)


def main():
    """
    CLI entry point for mounting ACS buckets.

    Parses command-line arguments and calls the mount function.

    Usage:
        python -m acs_sdk.fuse <bucket> <mountpoint> [--allow-other] [--log-level LEVEL] [--trace]

    Options:
        --allow-other: Allow other users to access the mount
            (requires 'user_allow_other' in /etc/fuse.conf)
        --log-level: Set logging level (DEBUG, INFO, WARNING, ERROR). Defaults to INFO.
        --trace: DEPRECATED (use --log-level DEBUG instead. Enable detailed tracing.
    """
    main_start_time = time.time()
    # Basic logging config for CLI setup phase (can be overridden by args)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info(f"main: Starting ACS FUSE CLI with raw arguments: {sys.argv}")

    import argparse
    parser = argparse.ArgumentParser(
        description='Mount an ACS bucket as a local filesystem using FUSE.',
        epilog='Example: python -m acs_sdk.fuse my-bucket /mnt/my-bucket'
    )
    parser.add_argument('bucket', help='The name of the ACS bucket to mount')
    parser.add_argument('mountpoint', help='The local directory path to mount the bucket onto')
    parser.add_argument('--allow-other', action='store_true',
                        help='Allow other users to access the mount (requires user_allow_other in /etc/fuse.conf)')
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Set the logging level (default: INFO)')
    parser.add_argument('--trace', action='store_true',
                        help='DEPRECATED: Use --log-level DEBUG instead. Enable detailed operation tracing.')

    # Handle edge case of no arguments or just -h/--help
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    # Configure logging level based on arguments
    log_level = args.log_level.upper()
    if args.trace:
        print("Warning: --trace is deprecated. Using --log-level DEBUG instead.")
        log_level = 'DEBUG'

    # Reconfigure the root logger level
    # Find the root logger and set its level. Also configure format.
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=getattr(logging, log_level), format=log_format, force=True) # Use force=True to override initial basicConfig

    logger.info(f"main: Logging level set to {log_level}")
    logger.debug(f"main: Parsed arguments: {args}")


    # Prepare mount arguments
    bucket = args.bucket
    mountpoint = os.path.abspath(args.mountpoint) # Use absolute path for clarity
    allow_other = args.allow_other

    logger.info(f"main: Preparing to mount bucket '{bucket}' at '{mountpoint}' (allow_other={allow_other})")

    try:
        mount(bucket, mountpoint, foreground=True, allow_other=allow_other) # Keep foreground=True for typical CLI usage
    except Exception as e:
         # Catch errors that might propagate from mount() if not handled internally
         logger.critical(f"main: Unhandled exception during mount execution: {e}", exc_info=True)
         print(f"\nA critical error occurred during the mount process: {e}")
         print("Please check the logs for more details.")
         sys.exit(1) # Exit with error status
    finally:
        logger.info(f"main: ACS FUSE CLI process finished.")
        time_function("main", main_start_time)
        sys.exit(0) # Ensure clean exit


if __name__ == '__main__':
    main()
