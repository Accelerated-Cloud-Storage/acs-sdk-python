import os
import time
import pytest
import random
import gzip
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from acs_sdk_python.client import ACSClient
from acs_sdk_python.client.exceptions import ACSError
from acs_sdk_python.client.types import ListObjectsOptions

def setup_grpc_test():
    """Setup test client and cleanup handler."""
    client = ACSClient()
    return client
@pytest.fixture
def client():
    """Fixture to provide client for tests."""
    client = setup_grpc_test()
    yield client
    client.close()

def test_bucket_operations(client):
    bucket = f"nkdev-grpc-{int(time.time_ns())}"
    
    # Test CreateBucket
    client.create_bucket(bucket, "us-east-1")
    with pytest.raises(Exception):
        client.create_bucket(bucket, "us-east-1")  # Try creating same bucket again

    # Test ListBuckets
    buckets = client.list_buckets()
    assert any(b.name == bucket for b in buckets), "Created bucket not found in list"

    # Test DeleteBucket
    with pytest.raises(Exception):
        client.delete_bucket("nonexistent-bucket")
    
    client.delete_bucket(bucket)
    buckets = client.list_buckets()
    assert not any(b.name == bucket for b in buckets), "Deleted bucket still present"

def test_head_bucket(client):
    """Test head bucket operations."""
    bucket = f"nkdev-grpc-{int(time.time_ns())}"
    
    # Test nonexistent bucket
    with pytest.raises(Exception):
        client.head_bucket("nonexistent-bucket")
    
    # Create bucket and test head operation
    client.create_bucket(bucket, "us-east-1")
    try:
        # Test successful head bucket
        result = client.head_bucket(bucket)
        assert result.region == "us-east-1", "Wrong region returned"
        
        # Test head on invalid bucket names
        invalid_buckets = ["", "a" * 64, "invalid.bucket", "UPPERCASE"]
        for invalid_bucket in invalid_buckets:
            with pytest.raises(Exception):
                client.head_bucket(invalid_bucket)
                
    finally:
        client.delete_bucket(bucket)

def test_bucket_validation(client):
    test_cases = [
        ("Empty bucket name", "", True),
        ("Bucket name too long", "a" * 64, True),
        (f"Valid bucket name", f"my-valid-bucket-name-{int(time.time_ns())}", False),
    ]

    for name, bucket, want_error in test_cases:
        if want_error:
            with pytest.raises(Exception):
                client.create_bucket(bucket, "us-east-1")
        else:
            client.create_bucket(bucket, "us-east-1")
            client.delete_bucket(bucket)

def test_concurrent_bucket_operations(client):
    num_concurrent = 10
    buckets = [f"nkdev-grpc-concurrent-{int(time.time_ns())}-{i}" for i in range(num_concurrent)]
    
    # Create buckets concurrently
    with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
        list(executor.map(
            lambda b: client.create_bucket(b, "us-east-1"),
            buckets
        ))

    # Verify all buckets were created
    all_buckets = client.list_buckets()
    for bucket in buckets:
        assert any(b.name == bucket for b in all_buckets), f"Bucket {bucket} not found"

    # Delete buckets concurrently
    with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
        list(executor.map(
            client.delete_bucket,
            buckets
        ))

def test_object_operations(client):
    bucket = f"nkdev-grpc-{int(time.time_ns())}"
    key = f"test-object-{int(time.time_ns())}"
    data = b"Hello, World!"

    # Create bucket for testing
    print("Creating bucket...")
    client.create_bucket(bucket, "us-east-1")

    try:
        # Test PutObject
        print("Testing PutObject...")
        client.put_object(bucket, key, data)
        with pytest.raises(Exception):
            client.put_object(f"nonexistent-bucket-{int(time.time_ns())}", key, data)

        # Test GetObject
        print("Testing GetObject...")
        retrieved = client.get_object(bucket, key)
        assert retrieved == data, "Retrieved data doesn't match original"
        with pytest.raises(Exception):
            client.get_object(bucket, "nonexistent-key")

        # Test HeadObject
        print("Testing HeadObject...")
        resp = client.head_object(bucket, key)
        assert resp.content_length == len(data), "Wrong content length"
        
        with pytest.raises(Exception):
            client.head_object(bucket, "nonexistent-key")
        with pytest.raises(Exception):
            client.head_object("nonexistent-bucket", key)

        # Test DeleteObject
        print("Testing DeleteObject...")
        client.delete_object(bucket, key)
        with pytest.raises(Exception):
            client.get_object(bucket, key)
        with pytest.raises(Exception):
            client.delete_object(bucket, "nonexistent-key")

    finally:
        client.delete_bucket(bucket)

def test_large_object_operations(client):
    bucket = f"nkdev-grpc-{int(time.time_ns())}"
    key = "large-test-object"
    
    # Create 100MB of random data
    data = bytes([i % 256 for i in range(10 * 1024 * 1024)])
    
    client.create_bucket(bucket, "us-east-1")
    try:
        # Test large object upload and download
        client.put_object(bucket, key, data)
        retrieved = client.get_object(bucket, key)
        
        assert len(retrieved) == len(data), "Retrieved data size mismatch"
        assert all(data[i] == retrieved[i] for i in range(0, 100)), "Data mismatch"        
    finally:
        client.delete_object(bucket, key)
        client.delete_bucket(bucket)

def test_list_and_bulk_object_operations(client):
    bucket = f"nkdev-grpc-{int(time.time_ns())}"
    objects = [
        ("test/obj1", b"data1"),
        ("test/obj2", b"data2"),
        ("test/obj3", b"data3"),
        ("other/obj4", b"data4"),
    ]

    client.create_bucket(bucket, "us-east-1")
    try:
        # Upload test objects
        for key, data in objects:
            client.put_object(bucket, key, data)

        # Test DeleteObjects
        keys_to_delete = ["test/obj1", "test/obj2"]
        client.delete_objects(bucket, keys_to_delete)

        # Verify deletion through listing
        remaining = list(client.list_objects(bucket, ListObjectsOptions(prefix="test/")))
        assert len(remaining) == 1, "Wrong number of remaining objects"

        # Try deleting non-existent objects
        with pytest.raises(Exception):
            client.delete_objects(bucket, ["nonexistent1", "nonexistent2"])

    finally:
        keys_to_clean = ["test/obj3", "other/obj4"]
        for key in keys_to_clean:
            client.delete_object(bucket, key)
        client.delete_bucket(bucket)

def test_object_storage_operations(client):
    bucket = f"nkdev-{int(time.time_ns())}"
    client.create_bucket(bucket, "us-east-1")

    try:
        # Define test object sizes
        object_sizes = [1024, 1024 * 1024, 1024 * 1024 * 10]  # 1KB, 1MB, 10MB
        num_objects = 50

        print("Starting write operations for varying object sizes...")
        for size in object_sizes:
            print(f"\nWriting {num_objects} objects of size {size} bytes")
            write_latencies = []
            
            for i in range(num_objects):
                key = f"key_{i}_size_{size}"
                data = random.randbytes(size)
                
                start = time.time()
                client.put_object(bucket, key, data)
                write_latencies.append(time.time() - start)
                
            calculate_metrics(write_latencies, f"Write (Size: {size} bytes)")

        # Step 2: Read objects
        print("\nStarting read operations for varying object sizes...")
        for size in object_sizes:
            print(f"\nReading {num_objects} objects of size {size} bytes")
            read_latencies = []
            
            for i in range(num_objects):
                key = f"key_{i}_size_{size}"
                
                start = time.time()
                client.get_object(bucket, key)
                read_latencies.append(time.time() - start)
                
            calculate_metrics(read_latencies, f"Read (Size: {size} bytes)")

        # Step 3: Delete objects
        print("\nStarting delete operations...")
        for size in object_sizes:
            delete_latencies = []
            
            for i in range(num_objects):
                key = f"key_{i}_size_{size}"
                
                start = time.time()
                client.delete_object(bucket, key)
                delete_latencies.append(time.time() - start)
                
            calculate_metrics(delete_latencies, f"Delete (Size: {size} bytes)")

    finally:
        client.delete_bucket(bucket)

def test_concurrent_object_operations(client):
    bucket = f"nkdev-{int(time.time_ns())}"
    client.create_bucket(bucket, "us-east-1")

    try:
        num_concurrent = 10
        errors = []

        # Concurrent puts
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            def put_object(idx):
                try:
                    key = f"concurrent-{idx}"
                    client.put_object(bucket, key, b"data")
                except Exception as e:
                    errors.append(f"concurrent put {idx} failed: {e}")
            
            list(executor.map(put_object, range(num_concurrent)))

        # Concurrent gets
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            def get_object(idx):
                try:
                    key = f"concurrent-{idx}"
                    client.get_object(bucket, key)
                except Exception as e:
                    errors.append(f"concurrent get {idx} failed: {e}")
            
            list(executor.map(get_object, range(num_concurrent)))

        # Concurrent deletes
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            def delete_object(idx):
                try:
                    key = f"concurrent-{idx}"
                    client.delete_object(bucket, key)
                except Exception as e:
                    errors.append(f"concurrent delete {idx} failed: {e}")
            
            list(executor.map(delete_object, range(num_concurrent)))

        assert not errors, f"Concurrent operations failed: {errors}"

    finally:
        client.delete_bucket(bucket)

def calculate_metrics(latencies, operation):
    """Calculate and print metrics for operations."""
    avg_latency = sum(latencies) / len(latencies)
    throughput = len(latencies) / sum(latencies)
    
    print(f"\n{operation} Metrics:")
    print(f"Average Latency: {avg_latency:.3f}s")
    print(f"Throughput: {throughput:.2f} ops/sec")

def test_object_edge_cases(client):
    bucket = f"nkdev-{int(time.time_ns())}"
    client.create_bucket(bucket, "us-east-1")

    try:
        # Test empty object
        client.put_object(bucket, "empty", b"")
        data = client.get_object(bucket, "empty")
        assert data == b"", "Empty object data mismatch"

        # Test very small object
        client.put_object(bucket, "small", b"a")
        data = client.get_object(bucket, "small")
        assert data == b"a", "Small object data mismatch"

        # Test object with special characters in key
        special_key = "!@#$%^&*()_+-=[]{}|;:,.<>?"
        client.put_object(bucket, special_key, b"special")
        data = client.get_object(bucket, special_key)
        assert data == b"special", "Special key object data mismatch"

        # Test invalid keys
        invalid_keys = ["", "a" * 1025, "\0"]
        for key in invalid_keys:
            with pytest.raises(Exception):
                client.put_object(bucket, key, b"data")

    finally:
        # List and delete all objects in the bucket before deleting the bucket
        objects = client.list_objects(bucket)
        for obj in objects:
            client.delete_object(bucket, obj)
        client.delete_bucket(bucket)

def test_copy_object(client):
    """Test copying objects within and between buckets."""
    source_bucket = f"source-bucket-{int(time.time_ns())}"
    dest_bucket = f"dest-bucket-{int(time.time_ns())}"
    source_key = "source-object"
    dest_key = "dest-object"
    data = b"Hello, Copy World!"

    # Create source and destination buckets
    client.create_bucket(source_bucket, "us-east-1")
    client.create_bucket(dest_bucket, "us-east-1")

    try:
        # Put object in source bucket
        client.put_object(source_bucket, source_key, data)

        # Test cases for copy operations
        test_cases = [
            {
                "name": "Valid copy same bucket",
                "source_bucket": source_bucket,
                "source_key": source_key,
                "dest_bucket": source_bucket,
                "dest_key": "copy-in-same-bucket",
                "should_fail": False
            },
            {
                "name": "Valid copy different bucket",
                "source_bucket": source_bucket,
                "source_key": source_key,
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
                "should_fail": False
            },
            {
                "name": "Non-existent source bucket",
                "source_bucket": "nonexistent-bucket",
                "source_key": source_key,
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
                "should_fail": True
            },
            {
                "name": "Non-existent source object",
                "source_bucket": source_bucket,
                "source_key": "nonexistent-object",
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
                "should_fail": True
            },
            {
                "name": "Non-existent destination bucket",
                "source_bucket": source_bucket,
                "source_key": source_key,
                "dest_bucket": "nonexistent-bucket",
                "dest_key": dest_key,
                "should_fail": True
            }
        ]

        for tc in test_cases:
            copy_source = f"{tc['source_bucket']}/{tc['source_key']}"
            
            if tc["should_fail"]:
                with pytest.raises(Exception):  # Expected failure for {tc['name']}
                    client.copy_object(tc["dest_bucket"], copy_source, tc["dest_key"])
            else:
                client.copy_object(tc["dest_bucket"], copy_source, tc["dest_key"])
                
                # Verify copied object
                copied_data = client.get_object(tc["dest_bucket"], tc["dest_key"])
                assert copied_data == data, f"Copied data mismatch for {tc['name']}"
                
                # Verify metadata
                source_meta = client.head_object(tc["source_bucket"], tc["source_key"])
                dest_meta = client.head_object(tc["dest_bucket"], tc["dest_key"])
                assert source_meta.content_length == dest_meta.content_length, \
                    f"Content length mismatch for {tc['name']}"

    finally:
        # Clean up test objects and buckets
        try:
            client.delete_object(source_bucket, source_key)
            client.delete_object(dest_bucket, dest_key)
            client.delete_object(source_bucket, "copy-in-same-bucket")
            client.delete_bucket(source_bucket)
            client.delete_bucket(dest_bucket)
        except Exception as e:
            print(f"Cleanup error: {e}")


def test_authorization_scenarios():
    # Test as admin user
    os.environ["ACS_PROFILE"] = "default"
    admin_client = setup_grpc_test()
    
    try:
        # Test access to existing shared bucket
        shared_bucket = "testbucket-19nikanamarla2"
        example_key = f"example-key-{int(time.time_ns())}"
        
        admin_client.put_object(shared_bucket, example_key, b"Hello, World!")
        admin_client.get_object(shared_bucket, example_key)
        admin_client.delete_object(shared_bucket, example_key)
        
        # Test bucket operations
        bucket = f"example-bucket-{int(time.time_ns())}"
        admin_client.create_bucket(bucket, "us-east-1")
        
        buckets = admin_client.list_buckets()
        assert any(b.name == bucket for b in buckets), "Admin created bucket not found"
        
        admin_client.delete_bucket(bucket)
        
        # Test unauthorized access
        with pytest.raises(Exception):
            admin_client.head_bucket("myawsbucket-19nikanamarla")
            
    finally:
        admin_client.close()

    # Test as user1
    os.environ["ACS_PROFILE"] = "testuser1"
    user1_client = setup_grpc_test()
    
    try:
        shared_bucket = "testbucket-19nikanamarla3"
        user1_client.share_bucket(shared_bucket)
        
        example_key = f"example-key-{int(time.time_ns())}"
        user1_client.put_object(shared_bucket, example_key, b"Hello, World!")
        
        # Test bucket operations
        bucket = f"example-bucket-{int(time.time_ns())}"
        user1_client.create_bucket(bucket, "us-east-1")
        
        buckets = user1_client.list_buckets()
        assert any(b.name == bucket for b in buckets), "User1 created bucket not found"
        
        user1_client.delete_bucket(bucket)
        
        # Test unauthorized access
        with pytest.raises(Exception):
            user1_client.head_bucket("dest-1736473596499879827")
            
    finally:
        user1_client.close()

    # Reset to default profile
    os.environ["ACS_PROFILE"] = "default"

def test_bucket_throughput(client):
    print("\nStarting bucket creation test (1000 buckets)...")
    created_buckets = []
    create_latencies = []

    for i in range(1000):
        bucket = f"test-bucket-{int(time.time_ns())}-{i}"
        created_buckets.append(bucket)
        
        start = time.time()
        client.create_bucket(bucket, "us-east-1")
        create_latencies.append(time.time() - start)
        
        if i > 0 and i % 100 == 0:
            print(f"Created {i} buckets...")

    calculate_metrics(create_latencies, "Bucket Creation")

    # Test listing buckets
    print("\nStarting bucket listing test...")
    start = time.time()
    buckets = client.list_buckets()
    list_latency = time.time() - start
    print(f"ListBuckets latency for {len(buckets)} buckets: {list_latency:.2f}s")

    # Test deleting buckets
    print("\nStarting bucket deletion test...")
    delete_latencies = []
    
    for i, bucket in enumerate(created_buckets):
        start = time.time()
        client.delete_bucket(bucket)
        delete_latencies.append(time.time() - start)
        
        if i > 0 and i % 100 == 0:
            print(f"Deleted {i} buckets...")

    calculate_metrics(delete_latencies, "Bucket Deletion")