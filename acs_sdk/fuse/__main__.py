# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
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
    
    # Create a mount point
    mkdir -p /mnt/acs-bucket

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
    # Enable debug logging
    export ACS_LOG_LEVEL=DEBUG
    python -m acs_sdk.fuse <bucket> <mountpoint>
    
    # Run with sudo if permission issues occur
    sudo python -m acs_sdk.fuse <bucket> <mountpoint>
    
    # Check if FUSE is properly installed
    which fusermount  # Linux
    which mount_macfuse  # macOS
"""

from .fuse_mount import main

if __name__ == '__main__':
    main() 