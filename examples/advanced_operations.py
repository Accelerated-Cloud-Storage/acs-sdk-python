from acs_sdk.client import ACSClient
from acs_sdk.client.types import ListObjectsOptions
import os
import uuid

def main():
    client = ACSClient()

    try:
        # Create a bucket with error handling
        bucket = f"my-test-bucket-{uuid.uuid4()}"
        try:
            client.create_bucket(bucket, "us-east-1")
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
            client.create_bucket(destBucket, "us-east-1")
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
        except Exception as e:
            print(f"Failed to create object: {e}")
            return
        try:
            client.delete_bucket(bucket)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            return

    finally:
        client.close()

if __name__ == "__main__":
    main()
