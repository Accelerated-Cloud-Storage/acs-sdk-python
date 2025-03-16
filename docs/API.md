# Table of Contents

* [client](#client)
  * [ACSClient](#client.ACSClient)
  * [HeadBucketOutput](#client.HeadBucketOutput)
  * [HeadObjectOutput](#client.HeadObjectOutput)
  * [ListObjectsOptions](#client.ListObjectsOptions)
  * [Session](#client.Session)
  * [ACSError](#client.ACSError)
  * [AuthenticationError](#client.AuthenticationError)
  * [BucketError](#client.BucketError)
  * [ObjectError](#client.ObjectError)
  * [ConfigurationError](#client.ConfigurationError)
* [client.client](#client.client)
  * [ACSClient](#client.client.ACSClient)
    * [close](#client.client.ACSClient.close)
    * [create\_bucket](#client.client.ACSClient.create_bucket)
    * [delete\_bucket](#client.client.ACSClient.delete_bucket)
    * [list\_buckets](#client.client.ACSClient.list_buckets)
    * [put\_object](#client.client.ACSClient.put_object)
    * [get\_object](#client.client.ACSClient.get_object)
    * [delete\_object](#client.client.ACSClient.delete_object)
    * [delete\_objects](#client.client.ACSClient.delete_objects)
    * [head\_object](#client.client.ACSClient.head_object)
    * [list\_objects](#client.client.ACSClient.list_objects)
    * [copy\_object](#client.client.ACSClient.copy_object)
    * [head\_bucket](#client.client.ACSClient.head_bucket)
    * [rotate\_key](#client.client.ACSClient.rotate_key)
    * [share\_bucket](#client.client.ACSClient.share_bucket)
* [client.exceptions](#client.exceptions)
  * [ACSError](#client.exceptions.ACSError)
  * [AuthenticationError](#client.exceptions.AuthenticationError)
  * [BucketError](#client.exceptions.BucketError)
  * [ObjectError](#client.exceptions.ObjectError)
  * [ConfigurationError](#client.exceptions.ConfigurationError)
* [client.retry](#client.retry)
  * [ACSError](#client.retry.ACSError)
  * [BucketError](#client.retry.BucketError)
  * [ObjectError](#client.retry.ObjectError)
  * [RETRYABLE\_STATUS\_CODES](#client.retry.RETRYABLE_STATUS_CODES)
* [client.types](#client.types)
  * [Session](#client.types.Session)
    * [region](#client.types.Session.region)
  * [HeadBucketOutput](#client.types.HeadBucketOutput)
    * [region](#client.types.HeadBucketOutput.region)
  * [HeadObjectOutput](#client.types.HeadObjectOutput)
    * [content\_type](#client.types.HeadObjectOutput.content_type)
    * [content\_encoding](#client.types.HeadObjectOutput.content_encoding)
    * [content\_language](#client.types.HeadObjectOutput.content_language)
    * [content\_length](#client.types.HeadObjectOutput.content_length)
    * [last\_modified](#client.types.HeadObjectOutput.last_modified)
    * [etag](#client.types.HeadObjectOutput.etag)
    * [user\_metadata](#client.types.HeadObjectOutput.user_metadata)
    * [server\_side\_encryption](#client.types.HeadObjectOutput.server_side_encryption)
    * [version\_id](#client.types.HeadObjectOutput.version_id)
  * [ListObjectsOptions](#client.types.ListObjectsOptions)
    * [prefix](#client.types.ListObjectsOptions.prefix)
    * [start\_after](#client.types.ListObjectsOptions.start_after)
    * [max\_keys](#client.types.ListObjectsOptions.max_keys)
* [fuse](#fuse)
  * [mount](#fuse.mount)
  * [main](#fuse.main)
  * [ACSFuse](#fuse.ACSFuse)
  * [unmount](#fuse.unmount)
  * [ReadBuffer](#fuse.ReadBuffer)
  * [BufferEntry](#fuse.BufferEntry)
  * [WriteBuffer](#fuse.WriteBuffer)
  * [calculate\_ttl](#fuse.calculate_ttl)
* [fuse.fuse\_mount](#fuse.fuse_mount)
  * [FUSE](#fuse.fuse_mount.FUSE)
  * [FuseOSError](#fuse.fuse_mount.FuseOSError)
  * [Operations](#fuse.fuse_mount.Operations)
  * [errno](#fuse.fuse_mount.errno)
  * [sys](#fuse.fuse_mount.sys)
  * [ACSClient](#fuse.fuse_mount.ACSClient)
  * [Session](#fuse.fuse_mount.Session)
  * [ListObjectsOptions](#fuse.fuse_mount.ListObjectsOptions)
  * [BytesIO](#fuse.fuse_mount.BytesIO)
  * [Lock](#fuse.fuse_mount.Lock)
  * [logger](#fuse.fuse_mount.logger)
  * [time\_function](#fuse.fuse_mount.time_function)
  * [ReadBuffer](#fuse.fuse_mount.ReadBuffer)
  * [BufferEntry](#fuse.fuse_mount.BufferEntry)
  * [WriteBuffer](#fuse.fuse_mount.WriteBuffer)
  * [calculate\_ttl](#fuse.fuse_mount.calculate_ttl)
  * [unmount](#fuse.fuse_mount.unmount)
  * [setup\_signal\_handlers](#fuse.fuse_mount.setup_signal_handlers)
  * [get\_mount\_options](#fuse.fuse_mount.get_mount_options)
  * [ACSFuse](#fuse.fuse_mount.ACSFuse)
    * [getattr](#fuse.fuse_mount.ACSFuse.getattr)
    * [readdir](#fuse.fuse_mount.ACSFuse.readdir)
    * [rename](#fuse.fuse_mount.ACSFuse.rename)
    * [read](#fuse.fuse_mount.ACSFuse.read)
    * [write](#fuse.fuse_mount.ACSFuse.write)
    * [create](#fuse.fuse_mount.ACSFuse.create)
    * [unlink](#fuse.fuse_mount.ACSFuse.unlink)
    * [mkdir](#fuse.fuse_mount.ACSFuse.mkdir)
    * [rmdir](#fuse.fuse_mount.ACSFuse.rmdir)
    * [truncate](#fuse.fuse_mount.ACSFuse.truncate)
    * [release](#fuse.fuse_mount.ACSFuse.release)
    * [link](#fuse.fuse_mount.ACSFuse.link)
    * [flock](#fuse.fuse_mount.ACSFuse.flock)
  * [mount](#fuse.fuse_mount.mount)
  * [main](#fuse.fuse_mount.main)
* [fuse.utils](#fuse.utils)
  * [logging](#fuse.utils.logging)
  * [logger](#fuse.utils.logger)
  * [time\_function](#fuse.utils.time_function)
* [fuse.mount\_utils](#fuse.mount_utils)
  * [sys](#fuse.mount_utils.sys)
  * [signal](#fuse.mount_utils.signal)
  * [subprocess](#fuse.mount_utils.subprocess)
  * [FUSE](#fuse.mount_utils.FUSE)
  * [logger](#fuse.mount_utils.logger)
  * [time\_function](#fuse.mount_utils.time_function)
  * [unmount](#fuse.mount_utils.unmount)
  * [setup\_signal\_handlers](#fuse.mount_utils.setup_signal_handlers)
  * [get\_mount\_options](#fuse.mount_utils.get_mount_options)
* [fuse.buffer](#fuse.buffer)
  * [math](#fuse.buffer.math)
  * [threading](#fuse.buffer.threading)
  * [RLock](#fuse.buffer.RLock)
  * [Lock](#fuse.buffer.Lock)
  * [BytesIO](#fuse.buffer.BytesIO)
  * [logger](#fuse.buffer.logger)
  * [MIN\_TTL](#fuse.buffer.MIN_TTL)
  * [MAX\_TTL](#fuse.buffer.MAX_TTL)
  * [MIN\_SIZE](#fuse.buffer.MIN_SIZE)
  * [MAX\_SIZE](#fuse.buffer.MAX_SIZE)
  * [calculate\_ttl](#fuse.buffer.calculate_ttl)
  * [BufferEntry](#fuse.buffer.BufferEntry)
  * [ReadBuffer](#fuse.buffer.ReadBuffer)
    * [get](#fuse.buffer.ReadBuffer.get)
    * [put](#fuse.buffer.ReadBuffer.put)
    * [remove](#fuse.buffer.ReadBuffer.remove)
    * [clear](#fuse.buffer.ReadBuffer.clear)
  * [WriteBuffer](#fuse.buffer.WriteBuffer)
    * [initialize\_buffer](#fuse.buffer.WriteBuffer.initialize_buffer)
    * [write](#fuse.buffer.WriteBuffer.write)
    * [read](#fuse.buffer.WriteBuffer.read)
    * [truncate](#fuse.buffer.WriteBuffer.truncate)
    * [remove](#fuse.buffer.WriteBuffer.remove)
    * [has\_buffer](#fuse.buffer.WriteBuffer.has_buffer)
* [fuse.\_\_main\_\_](#fuse.__main__)
  * [main](#fuse.__main__.main)

<a id="client"></a>

# Module client

<a id="client.ACSClient"></a>

## ACSClient

<a id="client.HeadBucketOutput"></a>

## HeadBucketOutput

<a id="client.HeadObjectOutput"></a>

## HeadObjectOutput

<a id="client.ListObjectsOptions"></a>

## ListObjectsOptions

<a id="client.Session"></a>

## Session

<a id="client.ACSError"></a>

## ACSError

<a id="client.AuthenticationError"></a>

## AuthenticationError

<a id="client.BucketError"></a>

## BucketError

<a id="client.ObjectError"></a>

## ObjectError

<a id="client.ConfigurationError"></a>

## ConfigurationError

<a id="client.client"></a>

# Module client.client

ACS Client Module.

This module implements the client for Accelerated Cloud Storage (ACS) service using gRPC.
It provides functionality for interacting with ACS buckets and objects, including
creating, listing, and deleting buckets, as well as uploading, downloading, and
managing objects within buckets.

Classes:
    ACSClient: Main client class for interacting with the ACS service.

<a id="client.client.*"></a>

## \*

<a id="client.client.*"></a>

## \*

<a id="client.client.ACSClient"></a>

## ACSClient Objects

```python
class ACSClient()
```

Client for the Accelerated Cloud Storage (ACS) service.

This class provides methods to interact with the ACS service, including creating,
deleting, and listing buckets and objects, as well as uploading and downloading data.
It handles authentication, connection management, and error handling.

**Attributes**:

- `session` _Session_ - Configuration for the client session.
- `channel` _grpc.Channel_ - The gRPC channel for communication.
- `client` _pb_grpc.ObjectStorageCacheStub_ - The gRPC client stub.
- `SERVER_ADDRESS` _str_ - The server address for the ACS service.
- `BASE_CHUNK_SIZE` _int_ - Base chunk size for streaming operations (64KB).
- `COMPRESSION_THRESHOLD` _int_ - Threshold for compression (100MB).

<a id="client.client.ACSClient.SERVER_ADDRESS"></a>

## ACSClient.SERVER\_ADDRESS

<a id="client.client.ACSClient.BASE_CHUNK_SIZE"></a>

## ACSClient.BASE\_CHUNK\_SIZE

64KB base chunk size for streaming

<a id="client.client.ACSClient.COMPRESSION_THRESHOLD"></a>

## ACSClient.COMPRESSION\_THRESHOLD

100MB threshold for compression

<a id="client.client.ACSClient.close"></a>

### ACSClient.close

```python
def close()
```

Close the client.

Closes the gRPC channel and releases resources.

<a id="client.client.ACSClient.create_bucket"></a>

### ACSClient.create\_bucket

```python
@retry()
def create_bucket(bucket: str) -> None
```

Create a new bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
  

**Raises**:

- `BucketError` - If bucket creation fails.

<a id="client.client.ACSClient.delete_bucket"></a>

### ACSClient.delete\_bucket

```python
@retry()
def delete_bucket(bucket: str) -> None
```

Delete a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
  

**Raises**:

- `BucketError` - If bucket deletion fails.

<a id="client.client.ACSClient.list_buckets"></a>

### ACSClient.list\_buckets

```python
@retry()
def list_buckets() -> List[pb.Bucket]
```

List all buckets.

**Returns**:

- `List[pb.Bucket]` - A list of buckets.
  

**Raises**:

- `ACSError` - If listing buckets fails.

<a id="client.client.ACSClient.put_object"></a>

### ACSClient.put\_object

```python
@retry()
def put_object(bucket: str, key: str, data: bytes) -> None
```

Upload data to a bucket with optional compression.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `key` _str_ - The object key.
- `data` _bytes_ - The data to upload.
  

**Raises**:

- `ObjectError` - If object upload fails.

<a id="client.client.ACSClient.get_object"></a>

### ACSClient.get\_object

```python
@retry()
def get_object(bucket: str,
               key: str,
               byte_range: Optional[str] = None) -> bytes
```

Download an object from a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `key` _str_ - The object key.
- `byte_range` _Optional[str]_ - Byte range to download (e.g. "0-1023").
  Defaults to None.
  

**Returns**:

- `bytes` - The downloaded object data.
  

**Raises**:

- `ObjectError` - If retrieval fails or range format is invalid.

<a id="client.client.ACSClient.delete_object"></a>

### ACSClient.delete\_object

```python
@retry()
def delete_object(bucket: str, key: str) -> None
```

Delete a single object from a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `key` _str_ - The object key.
  

**Raises**:

- `ObjectError` - If deletion fails.

<a id="client.client.ACSClient.delete_objects"></a>

### ACSClient.delete\_objects

```python
@retry()
def delete_objects(bucket: str, keys: List[str]) -> None
```

Delete multiple objects from a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `keys` _List[str]_ - A list of object keys to delete.
  

**Raises**:

- `ObjectError` - If deletion fails.

<a id="client.client.ACSClient.head_object"></a>

### ACSClient.head\_object

```python
@retry()
def head_object(bucket: str, key: str) -> HeadObjectOutput
```

Retrieve metadata for an object without downloading it.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `key` _str_ - The object key.
  

**Returns**:

- `HeadObjectOutput` - The metadata of the object.
  

**Raises**:

- `BucketError` - If the bucket does not exist or is not accessible.
- `ObjectError` - If the object metadata retrieval fails.

<a id="client.client.ACSClient.list_objects"></a>

### ACSClient.list\_objects

```python
@retry()
def list_objects(
        bucket: str,
        options: Optional[ListObjectsOptions] = None) -> Iterator[str]
```

List objects in a bucket with optional filtering.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `options` _Optional[ListObjectsOptions]_ - Filtering options.
  Defaults to None.
  

**Yields**:

- `Iterator[str]` - Object keys.
  

**Raises**:

- `BucketError` - If listing fails.

<a id="client.client.ACSClient.copy_object"></a>

### ACSClient.copy\_object

```python
@retry()
def copy_object(bucket: str, copy_source: str, key: str) -> None
```

Copy an object within or between buckets.

**Arguments**:

- `bucket` _str_ - The destination bucket name.
- `copy_source` _str_ - The source bucket name and object key.
- `key` _str_ - The destination object key.
  

**Raises**:

- `ObjectError` - If the copy operation fails.

<a id="client.client.ACSClient.head_bucket"></a>

### ACSClient.head\_bucket

```python
@retry()
def head_bucket(bucket: str) -> HeadBucketOutput
```

Retrieve metadata for a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
  

**Returns**:

- `HeadBucketOutput` - Bucket metadata including region.
  

**Raises**:

- `BucketError` - If the operation fails.

<a id="client.client.ACSClient.rotate_key"></a>

### ACSClient.rotate\_key

```python
@retry()
def rotate_key(force: bool = False) -> None
```

Rotate access keys.

**Arguments**:

- `force` _bool_ - Whether to force key rotation even if not needed.
  Defaults to False.
  

**Raises**:

- `ConfigurationError` - If key rotation fails.

<a id="client.client.ACSClient.share_bucket"></a>

### ACSClient.share\_bucket

```python
@retry()
def share_bucket(bucket: str) -> None
```

Share a bucket with the ACS service.

**Arguments**:

- `bucket` _str_ - The bucket name.
  

**Raises**:

- `BucketError` - If sharing fails.

<a id="client.exceptions"></a>

# Module client.exceptions

Exceptions Module.

This module defines the exception hierarchy for the ACS client, providing
specific exception types for different categories of errors that may occur
during ACS operations.

Classes:
    ACSError: Base exception for all ACS client errors.
    AuthenticationError: Exception for authentication failures.
    BucketError: Exception for bucket operation failures.
    ObjectError: Exception for object operation failures.
    ConfigurationError: Exception for configuration or credential errors.

<a id="client.exceptions.ACSError"></a>

## ACSError Objects

```python
class ACSError(Exception)
```

Base exception for ACS client errors.

This is the parent class for all ACS-specific exceptions, providing
a common structure for error codes and messages.

**Arguments**:

- `message` _str_ - Error message.
- `code` _str, optional_ - Error code. Defaults to "ERR_UNKNOWN".
  

**Attributes**:

- `code` _str_ - Error code for categorizing the error.
- `message` _str_ - Descriptive error message.

<a id="client.exceptions.AuthenticationError"></a>

## AuthenticationError Objects

```python
class AuthenticationError(ACSError)
```

Exception raised when authentication fails.

This exception is raised when there are issues with authentication,
such as invalid credentials or expired tokens.

**Arguments**:

- `message` _str_ - Error message.
  

**Attributes**:

- `code` _str_ - Error code, set to "ERR_AUTH".
- `message` _str_ - Descriptive error message.

<a id="client.exceptions.BucketError"></a>

## BucketError Objects

```python
class BucketError(ACSError)
```

Exception raised for bucket operation failures.

This exception is raised when operations on buckets fail, such as
creating, deleting, or accessing buckets.

**Arguments**:

- `message` _str_ - Error message.
- `operation` _str, optional_ - The bucket operation that failed.
  Defaults to None.
  

**Attributes**:

- `code` _str_ - Error code, formatted as "ERR_BUCKET" or "ERR_BUCKET_{OPERATION}".
- `message` _str_ - Descriptive error message.

<a id="client.exceptions.ObjectError"></a>

## ObjectError Objects

```python
class ObjectError(ACSError)
```

Exception raised for object operation failures.

This exception is raised when operations on objects fail, such as
uploading, downloading, or deleting objects.

**Arguments**:

- `message` _str_ - Error message.
- `operation` _str, optional_ - The object operation that failed.
  Defaults to None.
  

**Attributes**:

- `code` _str_ - Error code, formatted as "ERR_OBJECT" or "ERR_OBJECT_{OPERATION}".
- `message` _str_ - Descriptive error message.

<a id="client.exceptions.ConfigurationError"></a>

## ConfigurationError Objects

```python
class ConfigurationError(ACSError)
```

Exception raised for configuration or credential errors.

This exception is raised when there are issues with the client configuration
or credentials, such as missing or invalid configuration files.

**Arguments**:

- `message` _str_ - Error message.
  

**Attributes**:

- `code` _str_ - Error code, set to "ERR_CONFIG".
- `message` _str_ - Descriptive error message.

<a id="client.retry"></a>

# Module client.retry

Retry Module.

This module provides a retry decorator with exponential backoff for ACS client operations.
It handles transient errors and network issues by automatically retrying failed operations
with increasing delays between attempts.

Functions:
    retry: Decorator for retrying functions with exponential backoff.
    _convert_grpc_error: Helper function to convert gRPC errors to ACS-specific exceptions.

<a id="client.retry.ACSError"></a>

## ACSError

<a id="client.retry.BucketError"></a>

## BucketError

<a id="client.retry.ObjectError"></a>

## ObjectError

<a id="client.retry.RETRYABLE_STATUS_CODES"></a>

## client.retry.RETRYABLE\_STATUS\_CODES

<a id="client.types"></a>

# Module client.types

Types Module.

This module contains type definitions for ACS client operations, including
dataclasses for session configuration, bucket metadata, object metadata,
and options for listing objects.

Classes:
    Session: Configuration for an ACS client session.
    HeadBucketOutput: Metadata for a bucket.
    HeadObjectOutput: Metadata for an object.
    ListObjectsOptions: Options for listing objects in a bucket.

<a id="client.types.Session"></a>

## Session Objects

```python
@dataclass
class Session()
```

Configuration for an ACS client session.

This class holds configuration parameters for an ACS client session,
such as the AWS region to use.

**Attributes**:

- `region` _str_ - The AWS region to use for this session.
  Defaults to "us-east-1".

<a id="client.types.Session.region"></a>

## Session.region

<a id="client.types.HeadBucketOutput"></a>

## HeadBucketOutput Objects

```python
@dataclass
class HeadBucketOutput()
```

Metadata for a bucket.

This class represents the metadata returned by a head_bucket operation,
containing information about the bucket.

**Attributes**:

- `region` _str_ - The region where the bucket is located.

<a id="client.types.HeadBucketOutput.region"></a>

## HeadBucketOutput.region

<a id="client.types.HeadObjectOutput"></a>

## HeadObjectOutput Objects

```python
@dataclass
class HeadObjectOutput()
```

Metadata for an object.

This class represents the metadata returned by a head_object operation,
containing information about the object without downloading its contents.

**Attributes**:

- `content_type` _str_ - MIME type of the object.
- `content_encoding` _Optional[str]_ - Content encoding of the object.
- `content_language` _Optional[str]_ - Content language of the object.
- `content_length` _int_ - Size of the object in bytes.
- `last_modified` _datetime_ - Last modification time.
- `etag` _str_ - Entity tag of the object.
- `user_metadata` _Dict[str, str]_ - Custom metadata key-value pairs.
- `server_side_encryption` _Optional[str]_ - Server encryption method.
- `version_id` _Optional[str]_ - Version identifier.

<a id="client.types.HeadObjectOutput.content_type"></a>

## HeadObjectOutput.content\_type

<a id="client.types.HeadObjectOutput.content_encoding"></a>

## HeadObjectOutput.content\_encoding

<a id="client.types.HeadObjectOutput.content_language"></a>

## HeadObjectOutput.content\_language

<a id="client.types.HeadObjectOutput.content_length"></a>

## HeadObjectOutput.content\_length

<a id="client.types.HeadObjectOutput.last_modified"></a>

## HeadObjectOutput.last\_modified

<a id="client.types.HeadObjectOutput.etag"></a>

## HeadObjectOutput.etag

<a id="client.types.HeadObjectOutput.user_metadata"></a>

## HeadObjectOutput.user\_metadata

<a id="client.types.HeadObjectOutput.server_side_encryption"></a>

## HeadObjectOutput.server\_side\_encryption

<a id="client.types.HeadObjectOutput.version_id"></a>

## HeadObjectOutput.version\_id

<a id="client.types.ListObjectsOptions"></a>

## ListObjectsOptions Objects

```python
@dataclass
class ListObjectsOptions()
```

Options for listing objects in a bucket.

This class provides filtering and pagination options for the list_objects
operation, allowing for more targeted object listing.

**Attributes**:

- `prefix` _Optional[str]_ - Limits results to keys that begin with the specified prefix.
  Defaults to None.
- `start_after` _Optional[str]_ - Specifies the key to start after when listing objects.
  Defaults to None.
- `max_keys` _Optional[int]_ - Limits the number of keys returned.
  Defaults to None.

<a id="client.types.ListObjectsOptions.prefix"></a>

## ListObjectsOptions.prefix

<a id="client.types.ListObjectsOptions.start_after"></a>

## ListObjectsOptions.start\_after

<a id="client.types.ListObjectsOptions.max_keys"></a>

## ListObjectsOptions.max\_keys

<a id="fuse"></a>

# Module fuse

ACS FUSE Filesystem.

This package provides functionality to mount ACS buckets as local filesystems using FUSE.
It allows for transparent access to ACS objects as if they were local files.

Main components:
    - mount: Function to mount an ACS bucket
    - unmount: Function to unmount a previously mounted filesystem
    - ACSFuse: FUSE implementation for ACS
    - ReadBuffer: Buffer for reading files
    - WriteBuffer: Buffer for writing files

<a id="fuse.mount"></a>

## mount

<a id="fuse.main"></a>

## main

<a id="fuse.ACSFuse"></a>

## ACSFuse

<a id="fuse.unmount"></a>

## unmount

<a id="fuse.ReadBuffer"></a>

## ReadBuffer

<a id="fuse.BufferEntry"></a>

## BufferEntry

<a id="fuse.WriteBuffer"></a>

## WriteBuffer

<a id="fuse.calculate_ttl"></a>

## calculate\_ttl

<a id="fuse.fuse_mount"></a>

# Module fuse.fuse\_mount

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

<a id="fuse.fuse_mount.FUSE"></a>

## FUSE

<a id="fuse.fuse_mount.FuseOSError"></a>

## FuseOSError

<a id="fuse.fuse_mount.Operations"></a>

## Operations

<a id="fuse.fuse_mount.errno"></a>

## errno

<a id="fuse.fuse_mount.sys"></a>

## sys

<a id="fuse.fuse_mount.ACSClient"></a>

## ACSClient

<a id="fuse.fuse_mount.Session"></a>

## Session

<a id="fuse.fuse_mount.ListObjectsOptions"></a>

## ListObjectsOptions

<a id="fuse.fuse_mount.BytesIO"></a>

## BytesIO

<a id="fuse.fuse_mount.Lock"></a>

## Lock

<a id="fuse.fuse_mount.logger"></a>

## logger

<a id="fuse.fuse_mount.time_function"></a>

## time\_function

<a id="fuse.fuse_mount.ReadBuffer"></a>

## ReadBuffer

<a id="fuse.fuse_mount.BufferEntry"></a>

## BufferEntry

<a id="fuse.fuse_mount.WriteBuffer"></a>

## WriteBuffer

<a id="fuse.fuse_mount.calculate_ttl"></a>

## calculate\_ttl

<a id="fuse.fuse_mount.unmount"></a>

## unmount

<a id="fuse.fuse_mount.setup_signal_handlers"></a>

## setup\_signal\_handlers

<a id="fuse.fuse_mount.get_mount_options"></a>

## get\_mount\_options

<a id="fuse.fuse_mount.ACSFuse"></a>

## ACSFuse Objects

```python
class ACSFuse(Operations)
```

FUSE implementation for Accelerated Cloud Storage.

This class implements the FUSE operations interface to provide
filesystem access to ACS buckets. It handles file operations by
translating them to ACS API calls and manages buffers for efficient
read and write operations.

**Attributes**:

- `client` _ACSClient_ - Client for ACS API calls
- `bucket` _str_ - Name of the bucket being mounted
- `read_buffer` _ReadBuffer_ - Buffer for read operations
- `write_buffer` _WriteBuffer_ - Buffer for write operations

<a id="fuse.fuse_mount.ACSFuse.getattr"></a>

### ACSFuse.getattr

```python
def getattr(path, fh=None)
```

Get file attributes.

This method returns the attributes of a file or directory,
such as size, permissions, and modification time.

**Arguments**:

- `path` _str_ - Path to the file or directory
- `fh` _int, optional_ - File handle. Defaults to None.
  

**Returns**:

- `dict` - File attributes
  

**Raises**:

- `FuseOSError` - If the file or directory does not exist

<a id="fuse.fuse_mount.ACSFuse.readdir"></a>

### ACSFuse.readdir

```python
def readdir(path, fh)
```

List directory contents.

This method returns the contents of a directory, including
files and subdirectories.

**Arguments**:

- `path` _str_ - Path to the directory
- `fh` _int_ - File handle
  

**Returns**:

- `list` - List of directory entries
  

**Raises**:

- `FuseOSError` - If an error occurs while listing the directory

<a id="fuse.fuse_mount.ACSFuse.rename"></a>

### ACSFuse.rename

```python
def rename(old, new)
```

Rename a file or directory.

This method renames a file or directory by copying the object
to a new key and deleting the old one.

**Arguments**:

- `old` _str_ - Old path
- `new` _str_ - New path
  

**Raises**:

- `FuseOSError` - If the source file does not exist or an error occurs

<a id="fuse.fuse_mount.ACSFuse.read"></a>

### ACSFuse.read

```python
def read(path, size, offset, fh)
```

Read file contents, checking buffer first.

This method reads data from a file, first checking the read buffer
and falling back to object storage if necessary.

**Arguments**:

- `path` _str_ - Path to the file
- `size` _int_ - Number of bytes to read
- `offset` _int_ - Offset in the file to start reading from
- `fh` _int_ - File handle
  

**Returns**:

- `bytes` - The requested data
  

**Raises**:

- `FuseOSError` - If an error occurs while reading the file

<a id="fuse.fuse_mount.ACSFuse.write"></a>

### ACSFuse.write

```python
def write(path, data, offset, fh)
```

Write data to an in-memory buffer, to be flushed on close.

This method writes data to a file by storing it in a write buffer,
which will be flushed to object storage when the file is closed.

**Arguments**:

- `path` _str_ - Path to the file
- `data` _bytes_ - Data to write
- `offset` _int_ - Offset in the file to start writing at
- `fh` _int_ - File handle
  

**Returns**:

- `int` - Number of bytes written
  

**Raises**:

- `FuseOSError` - If an error occurs while writing the file

<a id="fuse.fuse_mount.ACSFuse.create"></a>

### ACSFuse.create

```python
def create(path, mode, fi=None)
```

Create a new file.

This method creates a new empty file in the object storage
and initializes a write buffer for it.

**Arguments**:

- `path` _str_ - Path to the file
- `mode` _int_ - File mode
- `fi` _dict, optional_ - File info. Defaults to None.
  

**Returns**:

- `int` - 0 on success
  

**Raises**:

- `FuseOSError` - If an error occurs while creating the file

<a id="fuse.fuse_mount.ACSFuse.unlink"></a>

### ACSFuse.unlink

```python
def unlink(path)
```

Delete a file if it exists.

This method deletes a file from the object storage.

**Arguments**:

- `path` _str_ - Path to the file
  

**Raises**:

- `FuseOSError` - If an error occurs while deleting the file

<a id="fuse.fuse_mount.ACSFuse.mkdir"></a>

### ACSFuse.mkdir

```python
def mkdir(path, mode)
```

Create a directory.

This method creates a directory by creating an empty object
with a trailing slash in the key.

**Arguments**:

- `path` _str_ - Path to the directory
- `mode` _int_ - Directory mode
  

**Raises**:

- `FuseOSError` - If an error occurs while creating the directory

<a id="fuse.fuse_mount.ACSFuse.rmdir"></a>

### ACSFuse.rmdir

```python
def rmdir(path)
```

Remove a directory.

This method removes a directory if it is empty.

**Arguments**:

- `path` _str_ - Path to the directory
  

**Raises**:

- `FuseOSError` - If the directory is not empty or an error occurs

<a id="fuse.fuse_mount.ACSFuse.truncate"></a>

### ACSFuse.truncate

```python
def truncate(path, length, fh=None)
```

Truncate file to specified length.

This method changes the size of a file by either truncating it
or extending it with null bytes.

**Arguments**:

- `path` _str_ - Path to the file
- `length` _int_ - New length of the file
- `fh` _int, optional_ - File handle. Defaults to None.
  

**Returns**:

- `int` - 0 on success
  

**Raises**:

- `FuseOSError` - If an error occurs while truncating the file

<a id="fuse.fuse_mount.ACSFuse.release"></a>

### ACSFuse.release

```python
def release(path, fh)
```

Release the file handle and flush the write buffer to ACS storage.

This method is called when a file is closed. It flushes the write buffer
to object storage and removes the file from both buffers.

**Arguments**:

- `path` _str_ - Path to the file
- `fh` _int_ - File handle
  

**Returns**:

- `int` - 0 on success

<a id="fuse.fuse_mount.ACSFuse.link"></a>

### ACSFuse.link

```python
def link(target, name)
```

Create hard link by copying the object.

This method creates a hard link by copying the object to a new key,
since true hard links aren't supported in object storage.

**Arguments**:

- `target` _str_ - Path to the target file
- `name` _str_ - Path to the new link
  

**Returns**:

- `int` - 0 on success
  

**Raises**:

- `FuseOSError` - If the target file does not exist or an error occurs

<a id="fuse.fuse_mount.ACSFuse.flock"></a>

### ACSFuse.flock

```python
def flock(path, op, fh)
```

File locking operation (implemented as a no-op).

This method is a no-op since object storage doesn't support file locking.

**Arguments**:

- `path` _str_ - Path to the file
- `op` _int_ - Lock operation
- `fh` _int_ - File handle
  

**Returns**:

- `int` - 0 (always succeeds)

<a id="fuse.fuse_mount.mount"></a>

## mount

```python
def mount(bucket: str, mountpoint: str, foreground: bool = True)
```

Mount an ACS bucket at the specified mountpoint.

This function mounts an ACS bucket as a local filesystem using FUSE.

**Arguments**:

- `bucket` _str_ - Name of the bucket to mount
- `mountpoint` _str_ - Local path where the filesystem should be mounted
- `foreground` _bool, optional_ - Run in foreground. Defaults to True.

<a id="fuse.fuse_mount.main"></a>

## main

```python
def main()
```

CLI entry point for mounting ACS buckets.

This function is the entry point for the command-line interface.
It parses command-line arguments and mounts the specified bucket.

Usage:
    python -m acs_sdk.fuse <bucket> <mountpoint>

<a id="fuse.utils"></a>

# Module fuse.utils

Utility functions for ACS FUSE filesystem.

This module provides logging configuration and utility functions
for the ACS FUSE filesystem implementation.

<a id="fuse.utils.logging"></a>

## logging

<a id="fuse.utils.logger"></a>

## fuse.utils.logger

<a id="fuse.utils.time_function"></a>

## time\_function

```python
def time_function(func_name, start_time)
```

Helper function for timing operations.

Calculates and logs the elapsed time for a function call.

**Arguments**:

- `func_name` _str_ - Name of the function being timed
- `start_time` _float_ - Start time from time.time()
  

**Returns**:

- `float` - Elapsed time in seconds

<a id="fuse.mount_utils"></a>

# Module fuse.mount\_utils

Mount utilities for ACS FUSE filesystem.

This module provides functions for mounting and unmounting ACS buckets
as local filesystems using FUSE.

<a id="fuse.mount_utils.sys"></a>

## sys

<a id="fuse.mount_utils.signal"></a>

## signal

<a id="fuse.mount_utils.subprocess"></a>

## subprocess

<a id="fuse.mount_utils.FUSE"></a>

## FUSE

<a id="fuse.mount_utils.logger"></a>

## logger

<a id="fuse.mount_utils.time_function"></a>

## time\_function

<a id="fuse.mount_utils.unmount"></a>

## unmount

```python
def unmount(mountpoint, fuse_ops_class=None)
```

Unmount the filesystem using fusermount (Linux).

This function safely unmounts a FUSE filesystem, ensuring that
all buffers are cleared before unmounting.

**Arguments**:

- `mountpoint` _str_ - Path where the filesystem is mounted
- `fuse_ops_class` _class, optional_ - The FUSE operations class to look for in active operations

<a id="fuse.mount_utils.setup_signal_handlers"></a>

## setup\_signal\_handlers

```python
def setup_signal_handlers(mountpoint, unmount_func)
```

Set up signal handlers for graceful unmounting.

This function sets up handlers for SIGINT and SIGTERM to ensure
that the filesystem is properly unmounted when the process is terminated.

**Arguments**:

- `mountpoint` _str_ - Path where the filesystem is mounted
- `unmount_func` _callable_ - Function to call for unmounting
  

**Returns**:

- `callable` - The signal handler function

<a id="fuse.mount_utils.get_mount_options"></a>

## get\_mount\_options

```python
def get_mount_options(foreground=True)
```

Get standard mount options for FUSE.

This function returns a dictionary of options to use when mounting
a FUSE filesystem.

**Arguments**:

- `foreground` _bool, optional_ - Run in foreground. Defaults to True.
  

**Returns**:

- `dict` - Dictionary of mount options

<a id="fuse.buffer"></a>

# Module fuse.buffer

Buffer management for ACS FUSE filesystem.

This module provides buffer implementations for reading and writing files in the ACS FUSE filesystem.
It includes classes for managing read and write operations with appropriate caching strategies.

<a id="fuse.buffer.math"></a>

## math

<a id="fuse.buffer.threading"></a>

## threading

<a id="fuse.buffer.RLock"></a>

## RLock

<a id="fuse.buffer.Lock"></a>

## Lock

<a id="fuse.buffer.BytesIO"></a>

## BytesIO

<a id="fuse.buffer.logger"></a>

## logger

<a id="fuse.buffer.MIN_TTL"></a>

## fuse.buffer.MIN\_TTL

1 second for small files

<a id="fuse.buffer.MAX_TTL"></a>

## fuse.buffer.MAX\_TTL

10 minutes maximum TTL

<a id="fuse.buffer.MIN_SIZE"></a>

## fuse.buffer.MIN\_SIZE

1KB

<a id="fuse.buffer.MAX_SIZE"></a>

## fuse.buffer.MAX\_SIZE

5TB

<a id="fuse.buffer.calculate_ttl"></a>

## calculate\_ttl

```python
def calculate_ttl(size: int) -> int
```

Calculate TTL based on file size using logarithmic scaling.

**Arguments**:

- `size` _int_ - Size of the file in bytes
  

**Returns**:

- `int` - TTL in seconds, between MIN_TTL and MAX_TTL

<a id="fuse.buffer.BufferEntry"></a>

## BufferEntry Objects

```python
class BufferEntry()
```

Represents a buffered file with access time tracking.

This class stores file data in memory along with metadata about
when it was last accessed and how long it should be kept in the buffer.

**Attributes**:

- `data` _bytes_ - The file content
- `last_access` _float_ - Timestamp of the last access
- `timer` _threading.Timer_ - Timer for automatic removal
- `ttl` _int_ - Time-to-live in seconds

<a id="fuse.buffer.ReadBuffer"></a>

## ReadBuffer Objects

```python
class ReadBuffer()
```

Fast dictionary-based buffer for file contents with size-based TTL expiration.

This class provides a memory cache for file contents to avoid repeated
requests to the object storage. Entries are automatically expired based
on their size and access patterns.

**Attributes**:

- `buffer` _dict_ - Dictionary mapping keys to BufferEntry objects
- `lock` _threading.RLock_ - Lock for thread-safe operations

<a id="fuse.buffer.ReadBuffer.get"></a>

### ReadBuffer.get

```python
def get(key: str) -> bytes
```

Get data from buffer and update access time.

**Arguments**:

- `key` _str_ - The key identifying the file
  

**Returns**:

- `bytes` - The file content or None if not in buffer

<a id="fuse.buffer.ReadBuffer.put"></a>

### ReadBuffer.put

```python
def put(key: str, data: bytes) -> None
```

Add data to buffer with size-based TTL.

**Arguments**:

- `key` _str_ - The key identifying the file
- `data` _bytes_ - The file content to buffer

<a id="fuse.buffer.ReadBuffer.remove"></a>

### ReadBuffer.remove

```python
def remove(key: str) -> None
```

Remove an entry from buffer.

**Arguments**:

- `key` _str_ - The key identifying the file to remove

<a id="fuse.buffer.ReadBuffer.clear"></a>

### ReadBuffer.clear

```python
def clear() -> None
```

Clear all entries from buffer.

<a id="fuse.buffer.WriteBuffer"></a>

## WriteBuffer Objects

```python
class WriteBuffer()
```

Manages write buffers for files being modified.

This class provides in-memory buffers for files that are being written to,
allowing changes to be accumulated before being flushed to object storage.

**Attributes**:

- `buffers` _dict_ - Dictionary mapping keys to BytesIO objects
- `lock` _threading.Lock_ - Lock for thread-safe operations

<a id="fuse.buffer.WriteBuffer.initialize_buffer"></a>

### WriteBuffer.initialize\_buffer

```python
def initialize_buffer(key: str, data: bytes = b"") -> None
```

Initialize a buffer for a file with existing data or empty.

**Arguments**:

- `key` _str_ - The key identifying the file
- `data` _bytes, optional_ - Initial data for the buffer. Defaults to empty.

<a id="fuse.buffer.WriteBuffer.write"></a>

### WriteBuffer.write

```python
def write(key: str, data: bytes, offset: int) -> int
```

Write data to a buffer at the specified offset.

**Arguments**:

- `key` _str_ - The key identifying the file
- `data` _bytes_ - The data to write
- `offset` _int_ - The offset at which to write the data
  

**Returns**:

- `int` - The number of bytes written

<a id="fuse.buffer.WriteBuffer.read"></a>

### WriteBuffer.read

```python
def read(key: str) -> bytes
```

Read the entire contents of a buffer.

**Arguments**:

- `key` _str_ - The key identifying the file
  

**Returns**:

- `bytes` - The buffer contents or None if the buffer doesn't exist

<a id="fuse.buffer.WriteBuffer.truncate"></a>

### WriteBuffer.truncate

```python
def truncate(key: str, length: int) -> None
```

Truncate a buffer to the specified length.

**Arguments**:

- `key` _str_ - The key identifying the file
- `length` _int_ - The length to truncate to

<a id="fuse.buffer.WriteBuffer.remove"></a>

### WriteBuffer.remove

```python
def remove(key: str) -> None
```

Remove a buffer.

**Arguments**:

- `key` _str_ - The key identifying the file to remove

<a id="fuse.buffer.WriteBuffer.has_buffer"></a>

### WriteBuffer.has\_buffer

```python
def has_buffer(key: str) -> bool
```

Check if a buffer exists for the specified key.

**Arguments**:

- `key` _str_ - The key to check
  

**Returns**:

- `bool` - True if a buffer exists, False otherwise

<a id="fuse.__main__"></a>

# Module fuse.\_\_main\_\_

ACS FUSE Main Entry Point.

This module serves as the entry point when the acs_sdk.fuse package is executed
directly using the -m flag (e.g., python -m acs_sdk.fuse).

It provides a command-line interface for mounting ACS buckets as local filesystems.

Setup:
    # Install the ACS SDK package
    pip install acs-sdk

    # Install FUSE on your system
    # On Ubuntu/Debian:
    sudo apt-get install fuse

    # On CentOS/RHEL:
    sudo yum install fuse

    # On macOS (using Homebrew):
    brew install macfuse

    # Configure ACS credentials
    # Create ~/.acs/credentials.yaml with:
    # default:
    #   access_key_id: your_access_key_id
    #   secret_access_key: your_secret_access_key

    # Create a mount point with permissions
    mkdir -p /mnt/acs-bucket
    chown ec2-user:ec2-user /mnt/acs-bucket

Usage:
    # Mount a bucket
    python -m acs_sdk.fuse <bucket> <mountpoint>

    # Example
    python -m acs_sdk.fuse my-bucket /mnt/acs-bucket

    # Unmount when done
    # On Linux
    fusermount -u /mnt/acs-bucket
    # On macOS
    umount /mnt/acs-bucket

Troubleshooting:
    # Create mount point with sudo
    sudo mkdir -p /mnt/acs-bucket
    sudo chown ec2-user:ec2-user /mnt/acs-bucket

    # Check if FUSE is properly installed
    which fusermount  # Linux
    which mount_macfuse  # macOS

    # Check mount permissions 
    ls -ld /mnt/acs-bucket

<a id="fuse.__main__.main"></a>

## main

