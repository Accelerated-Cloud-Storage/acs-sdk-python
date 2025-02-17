from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

@dataclass
class HeadBucketOutput:
    """Metadata for a bucket."""
    region: str

@dataclass
class HeadObjectOutput:
    """Metadata for an object."""
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
    """Options for listing objects."""
    prefix: Optional[str] = None
    start_after: Optional[str] = None
    max_keys: Optional[int] = None
