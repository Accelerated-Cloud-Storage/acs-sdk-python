#!/usr/bin/env python3
# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""Example demonstrating the use of Session with ACSClient."""

from acs_sdk import ACSClient, Session

def main():
    # Create a session with a specific region
    session = Session(region="us-west-2")
    
    # Initialize the client with the session
    client = ACSClient(session=session)
    
    # Use the client
    try:
        # List buckets
        buckets = client.list_buckets()
        print(f"Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"  - {bucket.name}")
        
        # Create a new bucket
        bucket_name = "example-bucket"
        print(f"\nCreating bucket: {bucket_name}")
        client.create_bucket(bucket_name)
        
        # Upload an object
        key = "hello.txt"
        data = b"Hello, Accelerated Cloud Storage!"
        print(f"\nUploading object: {key}")
        client.put_object(bucket_name, key, data)
        
        # Download the object
        print(f"\nDownloading object: {key}")
        retrieved_data = client.get_object(bucket_name, key)
        print(f"Retrieved data: {retrieved_data.decode('utf-8')}")
        
        # Clean up
        print(f"\nDeleting object: {key}")
        client.delete_object(bucket_name, key)
        
        print(f"\nDeleting bucket: {bucket_name}")
        client.delete_bucket(bucket_name)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the client
        client.close()

if __name__ == "__main__":
    main() 