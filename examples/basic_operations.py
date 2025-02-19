# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
from acs_sdk.client import ACSClient
import time
import uuid

def main():
    # Create a new client
    client = ACSClient()

    try:
        # Create a bucket
        bucket = f"my-test-bucket-{uuid.uuid4()}"
        client.create_bucket(bucket, "us-east-1")
        print("Created bucket: my-test-bucket")

        # Upload an object
        data = b"Hello, World!"
        client.put_object(bucket, "hello.txt", data)
        print("Uploaded object: hello.txt")

        # Get object metadata
        metadata = client.head_object(bucket, "hello.txt")
        print(f"Object size: {metadata.content_length} bytes")
        print(f"Last modified: {metadata.last_modified}")
        
        # Download the object
        downloaded = client.get_object(bucket, "hello.txt")
        print(f"Downloaded content: {downloaded.decode()}")

        # List objects in the bucket
        objects = client.list_objects(bucket)
        print("Objects in bucket:")
        for obj in objects:
            print(f"- {obj}")

        # Delete the object
        client.delete_object(bucket, "hello.txt")
        print("Deleted object: hello.txt")

        # Delete the bucket
        client.delete_bucket(bucket)
        print("Deleted bucket: my-test-bucket")

    finally:
        client.close()

if __name__ == "__main__":
    main()
