# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved
'''
This example demonstrates how to use the ACS FUSE filesystem to read and write files to an ACS bucket.

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

'''
import sys
import os

def main():
    if len(sys.argv) != 3:
        print("Usage: python fuse_operations.py <bucket> <mountpoint>")
        sys.exit(1)
    
    bucket = sys.argv[1]
    mountpoint = sys.argv[2]
    example_file = os.path.join(mountpoint, "example.txt")
    
    # Write to a file
    try:
        with open(example_file, 'w') as f:
            f.write("Hello FUSE")
        print(f"File created and written: {example_file}")
    except Exception as e:
        print(f"Write operation failed: {e}")
    
    # Read from the file
    try:
        with open(example_file, 'r') as f:
            content = f.read()
        print(f"Content read from file: {content}")
    except Exception as e:
        print(f"Read operation failed: {e}")
    
    # Delete the file
    try:
        os.remove(example_file)
        print(f"File removed: {example_file}")
    except Exception as e:
        print(f"Delete operation failed: {e}")

if __name__ == '__main__':
    main()