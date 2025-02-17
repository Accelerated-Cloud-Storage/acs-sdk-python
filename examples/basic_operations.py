from acs_sdk_python.client import ACSClient
import time

def main():
    # Create a new client
    client = ACSClient()

    try:
        # Create a bucket
        client.create_bucket("my-test-bucket", "us-east-1")
        print("Created bucket: my-test-bucket")

        # Upload an object
        data = b"Hello, World!"
        client.put_object("my-test-bucket", "hello.txt", data)
        print("Uploaded object: hello.txt")
        
        # Download the object
        downloaded = client.get_object("my-test-bucket", "hello.txt")
        print(f"Downloaded content: {downloaded.decode()}")

        # List objects in the bucket
        objects = client.list_objects("my-test-bucket")
        print("Objects in bucket:")
        for obj in objects:
            print(f"- {obj}")

        # Delete the object
        client.delete_object("my-test-bucket", "hello.txt")
        print("Deleted object: hello.txt")

        # Delete the bucket
        client.delete_bucket("my-test-bucket")
        print("Deleted bucket: my-test-bucket")

    finally:
        client.close()

if __name__ == "__main__":
    main()
