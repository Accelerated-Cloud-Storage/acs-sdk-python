# Table of Contents

* [exceptions](#exceptions)
  * [ACSError](#exceptions.ACSError)
  * [AuthenticationError](#exceptions.AuthenticationError)
  * [BucketError](#exceptions.BucketError)
  * [ObjectError](#exceptions.ObjectError)
  * [ConfigurationError](#exceptions.ConfigurationError)
* [types](#types)
  * [Session](#types.Session)
    * [region](#types.Session.region)
  * [HeadBucketOutput](#types.HeadBucketOutput)
    * [region](#types.HeadBucketOutput.region)
  * [HeadObjectOutput](#types.HeadObjectOutput)
    * [content\_type](#types.HeadObjectOutput.content_type)
    * [content\_encoding](#types.HeadObjectOutput.content_encoding)
    * [content\_language](#types.HeadObjectOutput.content_language)
    * [content\_length](#types.HeadObjectOutput.content_length)
    * [last\_modified](#types.HeadObjectOutput.last_modified)
    * [etag](#types.HeadObjectOutput.etag)
    * [user\_metadata](#types.HeadObjectOutput.user_metadata)
    * [server\_side\_encryption](#types.HeadObjectOutput.server_side_encryption)
    * [version\_id](#types.HeadObjectOutput.version_id)
  * [ListObjectsOptions](#types.ListObjectsOptions)
    * [prefix](#types.ListObjectsOptions.prefix)
    * [start\_after](#types.ListObjectsOptions.start_after)
    * [max\_keys](#types.ListObjectsOptions.max_keys)
* [client](#client)
  * [ACSClient](#client.ACSClient)
    * [close](#client.ACSClient.close)
    * [create\_bucket](#client.ACSClient.create_bucket)
    * [delete\_bucket](#client.ACSClient.delete_bucket)
    * [list\_buckets](#client.ACSClient.list_buckets)
    * [put\_object](#client.ACSClient.put_object)
    * [get\_object](#client.ACSClient.get_object)
    * [delete\_object](#client.ACSClient.delete_object)
    * [delete\_objects](#client.ACSClient.delete_objects)
    * [head\_object](#client.ACSClient.head_object)
    * [list\_objects](#client.ACSClient.list_objects)
    * [copy\_object](#client.ACSClient.copy_object)
    * [head\_bucket](#client.ACSClient.head_bucket)
    * [rotate\_key](#client.ACSClient.rotate_key)
    * [share\_bucket](#client.ACSClient.share_bucket)
* [fuse](#FUSE)
<a id="exceptions"></a>

# Module exceptions

Module defining exceptions for the ACS client.

<a id="exceptions.ACSError"></a>

## ACSError Objects

```python
class ACSError(Exception)
```

Base exception for ACS client errors.

**Arguments**:

- `message` _str_ - Error message.
- `code` _str, optional_ - Error code. Defaults to "ERR_UNKNOWN".

<a id="exceptions.AuthenticationError"></a>

## AuthenticationError Objects

```python
class AuthenticationError(ACSError)
```

Exception raised when authentication fails.

**Arguments**:

- `message` _str_ - Error message.

<a id="exceptions.BucketError"></a>

## BucketError Objects

```python
class BucketError(ACSError)
```

Exception raised for bucket operation failures.

**Arguments**:

- `message` _str_ - Error message.
- `operation` _str, optional_ - The bucket operation that failed.

<a id="exceptions.ObjectError"></a>

## ObjectError Objects

```python
class ObjectError(ACSError)
```

Exception raised for object operation failures.

**Arguments**:

- `message` _str_ - Error message.
- `operation` _str, optional_ - The object operation that failed.

<a id="exceptions.ConfigurationError"></a>

## ConfigurationError Objects

```python
class ConfigurationError(ACSError)
```

Exception raised for configuration or credential errors.

**Arguments**:

- `message` _str_ - Error message.

<a id="types"></a>

# Module types

Module containing type definitions for ACS client operations.

<a id="types.Session"></a>

## Session Objects

```python
@dataclass
class Session()
```

Configuration for an ACS client session.

**Attributes**:

- `region` _str_ - The AWS region to use for this session. Defaults to "us-east-1".

<a id="types.HeadBucketOutput"></a>

## HeadBucketOutput Objects

```python
@dataclass
class HeadBucketOutput()
```

Metadata for a bucket.

**Attributes**:

- `region` _str_ - The region where the bucket is located.

<a id="types.HeadBucketOutput.region"></a>

## HeadBucketOutput.region

<a id="types.HeadObjectOutput"></a>

## HeadObjectOutput Objects

```python
@dataclass
class HeadObjectOutput()
```

Metadata for an object.

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

<a id="types.HeadObjectOutput.content_type"></a>

## HeadObjectOutput.content\_type

<a id="types.HeadObjectOutput.content_encoding"></a>

## HeadObjectOutput.content\_encoding

<a id="types.HeadObjectOutput.content_language"></a>

## HeadObjectOutput.content\_language

<a id="types.HeadObjectOutput.content_length"></a>

## HeadObjectOutput.content\_length

<a id="types.HeadObjectOutput.last_modified"></a>

## HeadObjectOutput.last\_modified

<a id="types.HeadObjectOutput.etag"></a>

## HeadObjectOutput.etag

<a id="types.HeadObjectOutput.user_metadata"></a>

## HeadObjectOutput.user\_metadata

<a id="types.HeadObjectOutput.server_side_encryption"></a>

## HeadObjectOutput.server\_side\_encryption

<a id="types.HeadObjectOutput.version_id"></a>

## HeadObjectOutput.version\_id

<a id="types.ListObjectsOptions"></a>

## ListObjectsOptions Objects

```python
@dataclass
class ListObjectsOptions()
```

Options for listing objects in a bucket.

**Attributes**:

- `prefix` _Optional[str]_ - Limits results to keys that begin with the specified prefix.
- `start_after` _Optional[str]_ - Specifies the key to start after when listing objects.
- `max_keys` _Optional[int]_ - Limits the number of keys returned.

<a id="types.ListObjectsOptions.prefix"></a>

## ListObjectsOptions.prefix

<a id="types.ListObjectsOptions.start_after"></a>

## ListObjectsOptions.start\_after

<a id="types.ListObjectsOptions.max_keys"></a>

## ListObjectsOptions.max\_keys

<a id="client"></a>

# Module client

Module implementing the ACS client for Accelerated Cloud Storage using gRPC.

<a id="client.*"></a>

## \*

<a id="client.*"></a>

## \*

<a id="client.ACSClient"></a>

## ACSClient Objects

```python
class ACSClient(session: Optional[Session] = None)
```

ACSClient is a client for the Accelerated Cloud Storage (ACS) service. It provides methods to interact with the ACS service, including creating, deleting, and listing buckets and objects, as well as uploading and downloading data.

**Arguments**:

- `session` _Optional[Session], optional_ - Configuration for the client session. Defaults to None.

<a id="client.ACSClient.SERVER_ADDRESS"></a>

## ACSClient.SERVER\_ADDRESS

<a id="client.ACSClient.BASE_CHUNK_SIZE"></a>

## ACSClient.BASE\_CHUNK\_SIZE

64KB chunks for streaming

<a id="client.ACSClient.COMPRESSION_THRESHOLD"></a>

## ACSClient.COMPRESSION\_THRESHOLD

100MB threshold for compression

<a id="client.ACSClient.close"></a>

### ACSClient.close

```python
def close()
```

Close the client.

<a id="client.ACSClient.create_bucket"></a>

### ACSClient.create\_bucket

```python
def create_bucket(bucket: str) -> None
```

Create a new bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
  

**Raises**:

- `BucketError` - If bucket creation fails.

<a id="client.ACSClient.delete_bucket"></a>

### ACSClient.delete\_bucket

```python
@retry()
def delete_bucket(bucket: str) -> None
```

Delete a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.

<a id="client.ACSClient.list_buckets"></a>

### ACSClient.list\_buckets

```python
@retry()
def list_buckets() -> List[pb.Bucket]
```

List all buckets.

**Returns**:

- `List[pb.Bucket]` - A list of buckets.

<a id="client.ACSClient.put_object"></a>

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

<a id="client.ACSClient.get_object"></a>

### ACSClient.get\_object

```python
def get_object(bucket: str, key: str, byte_range: Optional[str] = None) -> bytes
```

Download an object from a bucket.

**Arguments**:

- `bucket` _str_ - The bucket name.
- `key` _str_ - The object key.
- `byte_range` _Optional[str], optional_ - Byte range to download (e.g. "0-1023"). Defaults to None.
  

**Returns**:

- `bytes` - The downloaded object data.
  

**Raises**:

- `ObjectError` - If retrieval fails.

<a id="client.ACSClient.delete_object"></a>

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

<a id="client.ACSClient.delete_objects"></a>

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

<a id="client.ACSClient.head_object"></a>

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

- `