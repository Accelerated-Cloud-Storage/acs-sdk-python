# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""Module containing type definitions for ACS client operations."""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

@dataclass
class HeadBucketOutput:
    """Metadata for a bucket.

    Attributes:
        region (str): The region where the bucket is located.
    """
    region: str

@dataclass
class HeadObjectOutput:
    """Metadata for an object.

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
    """Options for listing objects in a bucket.

    Attributes:
        prefix (Optional[str]): Limits results to keys that begin with the specified prefix.
        start_after (Optional[str]): Specifies the key to start after when listing objects.
        max_keys (Optional[int]): Limits the number of keys returned.
    """
    prefix: Optional[str] = None
    start_after: Optional[str] = None
    max_keys: Optional[int] = None
