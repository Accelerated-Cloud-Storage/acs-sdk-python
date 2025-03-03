# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
from acs_sdk import ACSClient, Session
from acs_sdk.client.types import ListObjectsOptions
import os
import uuid

def main():
    # Create a session with a specific region
    session = Session(region="us-east-1")
    
    # Create a new client with the session
    client = ACSClient(session=session)

    try:
        # Create a bucket with error handling
        bucket = f"my-test-bucket-{uuid.uuid4()}"
        try:
            client.create_bucket(bucket)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            return

        # Read and upload a large file with compression
        # Create a large file for upload before the test
        test_data = b"Sample data for large file upload." * 1024  # Adjust size as needed
        try:
            with open("large_file.dat", "wb") as f:
                f.write(test_data)
            
            # Read and upload the large file
            with open("large_file.dat", "rb") as f:
                data = f.read()
                client.put_object(bucket, "large_file.dat", data)
        finally:
            # Delete the file after the test
            if os.path.exists("large_file.dat"):
                os.remove("large_file.dat")

        # Ranged GET request
        try:
            # Create a moderate sized test file with predictable content
            test_content = b"0123456789" * 1000  # 10KB of sequential digits
            client.put_object(bucket, "range_test.dat", test_content)
            
            # Get only bytes 2000-2999 (1000 bytes)
            byte_range = "bytes=2000-2999"
            partial_content = client.get_object(bucket, "range_test.dat", byte_range=byte_range)
            
            # Verify that the retrieved content matches the expected range
            expected_slice = test_content[2000:3000]
            if partial_content == expected_slice:
                print(f"Ranged GET test successful: retrieved {len(partial_content)} bytes")
                print(f"First 20 bytes of range: {partial_content[:20]}")
            else:
                print(f"Ranged GET test failed: content doesn't match expected range")
                print(f"Retrieved {len(partial_content)} bytes, expected {len(expected_slice)} bytes")
        except Exception as e:
            print(f"Ranged GET test failed with error: {e}")

        # List objects with filtering
        options = ListObjectsOptions(
            prefix="large",
            max_keys=100,
            start_after=""
        )
        objects = list(client.list_objects(bucket, options))

        # Bulk delete objects
        if objects:
            keys_to_delete = [obj for obj in objects]
            try:
                client.delete_objects(bucket, keys_to_delete)
                print(f"Deleted {len(keys_to_delete)} objects")
            except Exception as e:
                print(f"Failed to delete some objects: {e}")

        # Copy objects between buckets
        destBucket = f"my-destination-bucket-{uuid.uuid4()}"
        try:
            client.create_bucket(destBucket)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            return
        try:
            client.put_object(bucket, "copied_file.dat", b"")
        except Exception as e:
            print(f"Failed to create object: {e}")
            return
        try:
            client.copy_object(
                destBucket,
                f"{bucket}/copied_file.dat",
                "copied_file.dat"
            )
            print("Successfully copied object")
        except Exception as e:
            print(f"Failed to copy object: {e}")

        # Force key rotation due to security issues with the current key
        try:
            client.rotate_key(force=True)
            print("Successfully rotated key")
        except Exception as e:
            print(f"Failed to rotate key: {e}")

        # Delete bucket with error handling
        try:
            client.delete_object(bucket, "copied_file.dat")
            client.delete_object(bucket, "range_test.dat")  # Clean up our range test file
        except Exception as e:
            print(f"Failed to delete object: {e}")
            return
        try:
            client.delete_bucket(bucket)
        except Exception as e:
            print(f"Failed to delete bucket: {e}")
            return

    finally:
        client.close()

if __name__ == "__main__":
    main()
