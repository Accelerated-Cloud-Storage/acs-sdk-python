# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
# Make sure to run the python -m acs_sdk.fuse.fuse_mount <bucket> <mountpoint> command before running the script.
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