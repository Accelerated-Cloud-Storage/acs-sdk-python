# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved
'''
This example demonstrates how to use the ACS FUSE filesystem to read and write files to an ACS bucket.

To run this example, you need to have the ACS FUSE filesystem mounted on your system.

# Create a mount point
mkdir -p /mnt/acs-bucket

# Mount the bucket
python -m acs_sdk.fuse my-bucket /mnt/acs-bucket

# Now you can work with the files as if they were local
ls /mnt/acs-bucket
cat /mnt/acs-bucket/example.txt

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