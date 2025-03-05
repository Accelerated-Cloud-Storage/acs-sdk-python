# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Types Module.

This module contains type definitions for ACS client operations, including
dataclasses for session configuration, bucket metadata, object metadata,
and options for listing objects.

Classes:
    Session: Configuration for an ACS client session.
    HeadBucketOutput: Metadata for a bucket.
    HeadObjectOutput: Metadata for an object.
    ListObjectsOptions: Options for listing objects in a bucket.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional, List, Iterator

@dataclass
class Session:
    """
    Configuration for an ACS client session.
    
    This class holds configuration parameters for an ACS client session,
    such as the AWS region to use.
    
    Attributes:
        region (str): The AWS region to use for this session.
            Defaults to "us-east-1".
    """
    region: str = "us-east-1"

@dataclass
class HeadBucketOutput:
    """
    Metadata for a bucket.
    
    This class represents the metadata returned by a head_bucket operation,
    containing information about the bucket.

    Attributes:
        region (str): The region where the bucket is located.
    """
    region: str

@dataclass
class HeadObjectOutput:
    """
    Metadata for an object.
    
    This class represents the metadata returned by a head_object operation,
    containing information about the object without downloading its contents.

    Attributes:
        content_type (str): MIME type of the object.
        content_encoding (Optional[str]): Content encoding of the object.
        content_language (Optional[str]): Content language of the object.
        content_length (int): Size of the object in bytes.
        last_modified (datetime): Last modification time.
        etag (str): Entity tag of the object.
        user_metadata (Dict[str, str]): Custom metadata key-value pairs.
        server_side_encryption (Optional[str]): Server encryption method.
        version_id (Optional[str]): Version identifier.
    """
    content_type: str
    content_encoding: Optional[str]
    content_language: Optional[str]
    content_length: int
    last_modified: datetime
    etag: str
    user_metadata: Dict[str, str]
    server_side_encryption: Optional[str]
    version_id: Optional[str]

@dataclass
class ListObjectsOptions:
    """
    Options for listing objects in a bucket.
    
    This class provides filtering and pagination options for the list_objects
    operation, allowing for more targeted object listing.

    Attributes:
        prefix (Optional[str]): Limits results to keys that begin with the specified prefix.
            Defaults to None.
        start_after (Optional[str]): Specifies the key to start after when listing objects.
            Defaults to None.
        max_keys (Optional[int]): Limits the number of keys returned.
            Defaults to None.
    """
    prefix: Optional[str] = None
    start_after: Optional[str] = None
    max_keys: Optional[int] = None
