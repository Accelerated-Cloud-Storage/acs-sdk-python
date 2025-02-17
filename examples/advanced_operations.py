from acs_sdk_python.client import ACSClient
from acs_sdk_python.client.types import ListObjectsOptions
import os

def main():
    client = ACSClient()

    try:
        # Create a bucket with error handling
        bucket = "my-test-bucket"
        try:
            client.create_bucket(bucket, "us-east-1")
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            return

        # Read and upload a large file with compression
        with open("large_file.dat", "rb") as f:
            data = f.read()
            client.put_object(bucket, "large_file.dat", data)

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
        try:
            client.copy_object(
                "destination-bucket",
                f"{bucket}/large_file.dat",
                "copied_file.dat"
            )
            print("Successfully copied object")
        except Exception as e:
            print(f"Failed to copy object: {e}")

        # Share a bucket
        try:
            client.share_bucket("shared-bucket")
            print("Successfully shared bucket")
        except Exception as e:
            print(f"Failed to share bucket: {e}")

        # Force key rotation
        try:
            client.rotate_key(force=True)
            print("Successfully rotated key")
        except Exception as e:
            print(f"Failed to rotate key: {e}")

    finally:
        client.close()

if __name__ == "__main__":
    main()
