# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
ACS FUSE Filesystem.

This package provides functionality to mount ACS buckets as local filesystems using FUSE.
It allows for transparent access to ACS objects as if they were local files.

Main components:
    - mount: Function to mount an ACS bucket
    - unmount: Function to unmount a previously mounted filesystem
    - ACSFuse: FUSE implementation for ACS
    - ReadBuffer: Buffer for reading files
    - WriteBuffer: Buffer for writing files
"""

from .fuse_mount import mount, main, ACSFuse
from .mount_utils import unmount
from .buffer import ReadBuffer, BufferEntry, WriteBuffer, calculate_ttl

__all__ = ['mount', 'unmount', 'main', 'ACSFuse', 'ReadBuffer', 'WriteBuffer', 'BufferEntry', 'calculate_ttl'] 