# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
ACS Client Module.

This module implements the client for Accelerated Cloud Storage (ACS) service using gRPC.
It provides functionality for interacting with ACS buckets and objects, including
creating, listing, and deleting buckets, as well as uploading, downloading, and
managing objects within buckets.

Classes:
    ACSClient: Main client class for interacting with the ACS service.
"""
import os
import yaml
import grpc
from pathlib import Path
from datetime import datetime
import lz4.frame
from typing import List, Dict, Optional, Union, Iterator
from dataclasses import dataclass
from acs_sdk.internal.generated import client_storage_pb2 as pb
from acs_sdk.internal.generated import client_storage_pb2_grpc as pb_grpc
from .retry import retry
from .types import *
from .exceptions import *

class ACSClient:
    """
    Client for the Accelerated Cloud Storage (ACS) service.
    
    This class provides methods to interact with the ACS service, including creating,
    deleting, and listing buckets and objects, as well as uploading and downloading data.
    It handles authentication, connection management, and error handling.
    
    Attributes:
        session (Session): Configuration for the client session.
        channel (grpc.Channel): The gRPC channel for communication.
        client (pb_grpc.ObjectStorageCacheStub): The gRPC client stub.
        SERVER_ADDRESS (str): The server address for the ACS service.
        BASE_CHUNK_SIZE (int): Base chunk size for streaming operations (64KB).
        COMPRESSION_THRESHOLD (int): Threshold for compression (5GB).
        MIN_SAMPLE_SIZE (int): Minimum sample size for compression estimation (1MB).
        MAX_SAMPLE_SIZE (int): Maximum sample size for compression estimation (256MB).
        SAMPLE_RATIO (float): Ratio of data to sample for compression estimation (1%).
    """
    
    # Constants 
    SERVER_ADDRESS = "acceleratedcloudstorageproduction.com:50050"
    BASE_CHUNK_SIZE = 64 * 1024  # 64KB base chunk size for streaming
    COMPRESSION_THRESHOLD = 5 * 1024 * 1024 * 1024  # 5GB threshold for compression
    MIN_SAMPLE_SIZE = 1 * 1024 * 1024  # 1MB minimum sample size
    MAX_SAMPLE_SIZE = 256 * 1024 * 1024  # 256MB maximum sample size
    SAMPLE_RATIO = 0.01  # Sample 1% of the data
    MIN_COMPRESSION_RATIO = 0.5  # Only compress if we can save at least 50%

    def __init__(self, session: Optional[Session] = None):
        """
        Initialize the ACSClient.

        Sets up a secure gRPC channel, loads credentials, authenticates,
        and checks for key rotation.

        Args:
            session (Optional[Session]): Configuration for the client session.
                Defaults to None, which creates a default session.

        Raises:
            RuntimeError: If the CA certificate is not found.
        """
        # Store the session or create a default one
        self.session = session or Session()
        
        # Load the CA certificate from the correct location
        pkg_root = os.path.dirname(os.path.dirname(__file__))  # Go up one level from client dir
        ca_cert_path = os.path.join(pkg_root, 'internal', 'ca-chain.pem')
        try:
            with open(ca_cert_path, 'rb') as f:
                ca_cert = f.read()
        except FileNotFoundError:
            raise RuntimeError(
                f"CA certificate not found at {ca_cert_path}. "
                "Please ensure the certificate is properly installed with the package."
            )

        # Setup secure channel with improved settings and TLS
        credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_cert
        )

        options = [
            ('grpc.max_send_message_length', 1024 * 1024 * 1024), # 1GB
            ('grpc.max_receive_message_length', 1024 * 1024 * 1024), # 1GB
            ('grpc.keepalive_time_ms', 10000), # 10 seconds
            ('grpc.keepalive_timeout_ms', 5000), # 5 seconds
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0), # unlimited
            ('grpc.http2.min_time_between_pings_ms', 10000), # 10 seconds
            ('grpc.enable_retries', 1), # enable retries
            ('grpc.max_connection_idle_ms', 60000), # 1 minute
            ('grpc.max_connection_age_ms', 3600000), # 1 hour
            ('grpc.max_connection_age_grace_ms', 5000), # 5 seconds
        ]
        
        self.channel = grpc.secure_channel(self.SERVER_ADDRESS,credentials,options=options)
        
        self._setup_channel_connectivity()
        self.client = pb_grpc.ObjectStorageCacheStub(self.channel)
        
        # Load and authenticate
        creds = self._load_credentials()
        self._authenticate(creds)
        self._check_key_rotation(creds)

    def _setup_channel_connectivity(self):
        """
        Setup channel connectivity monitoring and callbacks.
        
        Configures the gRPC channel to monitor connectivity state changes
        and attempt reconnection when failures occur.
        """
        def _on_channel_state_change(state):
            if state == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
                self.channel.subscribe(lambda s: None, try_to_connect=True)
        
        self.channel.subscribe(_on_channel_state_change, try_to_connect=True)

    def close(self):
        """
        Close the client.
        
        Closes the gRPC channel and releases resources.
        """
        self.channel.close()

    def _load_credentials(self) -> dict:
        """
        Load credentials from ~/.acs/credentials.yaml.

        Creates the credentials file with default values if it does not exist.

        Returns:
            dict: The credentials for the active profile.

        Raises:
            ValueError: If the active profile is not found in the credentials.
        """
        creds_path = Path.home() / '.acs' / 'credentials.yaml'
        
        # Create directory if needed
        creds_path.parent.mkdir(mode=0o700, exist_ok=True)
        
        # Create default credentials if file doesn't exist
        if not creds_path.exists():
            default_creds = {
                'default': {
                    'access_key_id': 'your_access_key_id',
                    'secret_access_key': 'your_secret_access_key'
                }
            }
            with open(creds_path, 'w') as f:
                yaml.dump(default_creds, f)
        
        # Load credentials
        with open(creds_path) as f:
            profiles = yaml.safe_load(f)
        
        profile = os.getenv('ACS_PROFILE', 'default')
        if profile not in profiles:
            raise ValueError(f"Profile '{profile}' not found in credentials file")
            
        return profiles[profile]

    def _authenticate(self, creds: dict):
        """
        Authenticate with the ACS service.

        Args:
            creds (dict): The credentials containing access and secret keys.
            
        Raises:
            grpc.RpcError: If authentication fails.
        """
        request = pb.AuthRequest(
            access_key_id=creds['access_key_id'],
            secret_access_key=creds['secret_access_key'],
            region=self.session.region
        )
        self.client.Authenticate(request)

    def _check_key_rotation(self, creds: dict):
        """
        Check and perform key rotation if necessary.

        Args:
            creds (dict): The credentials used for authentication.
        """
        try:
            self.rotate_key(False)
        except Exception as e:
            print(f"Warning: Key rotation check failed: {e}")

    @retry()
    def create_bucket(self, bucket: str) -> None:
        """
        Create a new bucket.

        Args:
            bucket (str): The bucket name.

        Raises:
            BucketError: If bucket creation fails.
        """
        request = pb.CreateBucketRequest(bucket=bucket)
        try:
            self.client.CreateBucket(request)
        except grpc.RpcError as e:
            raise BucketError(f"Failed to create bucket: {e.details()}") from e

    @retry()
    def delete_bucket(self, bucket: str) -> None:
        """
        Delete a bucket.

        Args:
            bucket (str): The bucket name.
            
        Raises:
            BucketError: If bucket deletion fails.
        """
        request = pb.DeleteBucketRequest(bucket=bucket)
        self.client.DeleteBucket(request)

    @retry()
    def list_buckets(self) -> List[pb.Bucket]:
        """
        List all buckets.

        Returns:
            List[pb.Bucket]: A list of buckets.
            
        Raises:
            ACSError: If listing buckets fails.
        """
        request = pb.ListBucketsRequest()
        response = self.client.ListBuckets(request)
        return list(response.buckets)

    def _estimate_compression_ratio(self, data: bytes) -> float:
        """
        Estimate LZ4 compression ratio by compressing a sample of the data.
        Sample size scales with data size: 1% of total size, bounded between 1MB and 256MB.
        
        Args:
            data: The data to estimate compression for
            
        Returns:
            float: Estimated compression ratio (compressed_size / original_size)
            A ratio < 1 means compression will help, > 1 means compression will make it larger
        """
        total_size = len(data)
        
        # Calculate sample size as 1% of total size, bounded between MIN and MAX
        target_sample_size = int(total_size * self.SAMPLE_RATIO)
        sample_size = max(min(target_sample_size, self.MAX_SAMPLE_SIZE), self.MIN_SAMPLE_SIZE)
        
        # Take three samples: beginning, middle, and end
        per_sample_size = sample_size // 3
        samples = [
            data[:per_sample_size],
            data[total_size//2 - per_sample_size//2:total_size//2 + per_sample_size//2],
            data[-per_sample_size:]
        ]
        
        # Test compression ratio on samples
        ratios = []
        total_sample_size = 0
        total_compressed_size = 0
        
        for sample in samples:
            compressed = lz4.frame.compress(sample, compression_level=0, block_size=lz4.frame.BLOCKSIZE_MAX64KB)
            total_sample_size += len(sample)
            total_compressed_size += len(compressed)
            ratios.append(len(compressed) / len(sample))
        
        # Return weighted average ratio
        return total_compressed_size / total_sample_size

    @retry()
    def put_object(self, bucket: str, key: str, data: bytes) -> None:
        """
        Upload data to a bucket with optional compression.

        Args:
            bucket (str): The bucket name.
            key (str): The object key.
            data (bytes): The data to upload.
            
        Raises:
            ObjectError: If object upload fails.
        """
        is_compressed = False
        data_len = len(data)
        
        if data_len >= self.COMPRESSION_THRESHOLD:
            # Estimate if compression will be beneficial
            ratio = self._estimate_compression_ratio(data)
            if ratio < self.MIN_COMPRESSION_RATIO:  # Only compress if we can save at least 50%
                # Note: In lz4.frame, compression_level=0 means default level
                compressed = memoryview(lz4.frame.compress(data, compression_level=0, block_size=lz4.frame.BLOCKSIZE_MAX64KB))
                compressed_len = len(compressed)
                if compressed_len < data_len:
                    data = compressed
                    data_len = compressed_len
                    is_compressed = True

        def request_generator():
            # Send parameters first
            yield pb.PutObjectRequest(
                parameters=pb.PutObjectInput(
                    bucket=bucket,
                    key=key,
                    isCompressed=is_compressed
                )
            )
            
            # Calculate appropriate chunk size based on data size
            if data_len < 1024 * 1024:  # < 1MB
                chunk_size = self.BASE_CHUNK_SIZE
            elif data_len < 10 * 1024 * 1024:  # < 10MB
                chunk_size = 256 * 1024  # 256KB
            elif data_len < 100 * 1024 * 1024:  # < 100MB
                chunk_size = 1024 * 1024  # 1MB
            else:
                chunk_size = 4 * 1024 * 1024  # 4MB
            
            # Send data chunks using memory views for large objects
            for i in range(0, data_len, chunk_size):
                chunk = data[i:i + chunk_size]
                yield pb.PutObjectRequest(chunk=bytes(chunk))

        self.client.PutObject(request_generator())

    @retry()
    def get_object(self, bucket: str, key: str, byte_range: Optional[str] = None) -> bytes:
        """
        Download an object from a bucket.

        Args:
            bucket (str): The bucket name.
            key (str): The object key.
            byte_range (Optional[str]): Byte range to download (e.g. "0-1023").
                Defaults to None.

        Returns:
            bytes: The downloaded object data.

        Raises:
            ObjectError: If retrieval fails or range format is invalid.
        """
        try:
            # Validate range format if provided
            if byte_range:
                if not byte_range.startswith("bytes="):
                    raise ObjectError("Invalid range format: must start with 'bytes='")
                
                try:
                    range_parts = byte_range[6:].split("-")
                    if len(range_parts) != 2:
                        raise ValueError("Invalid range format")
                    
                    # Handle empty parts
                    if not range_parts[0] and not range_parts[1]:
                        raise ValueError("Empty range")
                        
                    # Parse start and end
                    start = int(range_parts[0]) if range_parts[0] else 0
                    end = int(range_parts[1]) if range_parts[1] else None
                    
                    # Validate range values
                    if start < 0 or (end is not None and end < start):
                        raise ValueError("Invalid range values")
                        
                except ValueError as ve:
                    raise ObjectError(f"Invalid range format: {str(ve)}")

            request = pb.GetObjectRequest(bucket=bucket, key=key)
            if byte_range:
                request.range = byte_range
                
            response_stream = self.client.GetObject(request)
            first_message = True
            chunks = bytearray()  # Use bytearray for more efficient appending
            is_compressed = False
            estimated_size = 0
            
            for response in response_stream:
                if first_message:
                    first_message = False
                    # Process metadata from first message
                    if response.HasField('metadata'):
                        # Get estimated size if available
                        estimated_size = getattr(response.metadata, 'size', 0)
                        if estimated_size > 0:
                            chunks = bytearray(estimated_size)
                        # Safely access the is_compressed field
                        try:
                            is_compressed = response.metadata.is_compressed
                        except AttributeError:
                            print("Warning: Could not access is_compressed attribute in metadata")
                            is_compressed = False
                    continue
                    
                if response.HasField('chunk'):
                    chunks.extend(response.chunk)
            
            data = bytes(chunks)
            
            # Decompress if needed
            if is_compressed:
                try:
                    data = lz4.frame.decompress(data)
                except Exception as e:
                    raise ObjectError(f"Failed to decompress object: {str(e)}") from e
                    
            return data
        except grpc.RpcError as e:
            raise ObjectError(f"Failed to get object: {e.details()}") from e

    @retry()
    def delete_object(self, bucket: str, key: str) -> None:
        """
        Delete a single object from a bucket.

        Args:
            bucket (str): The bucket name.
            key (str): The object key.

        Raises:
            ObjectError: If deletion fails.
        """
        try:
            request = pb.DeleteObjectRequest(bucket=bucket, key=key)
            self.client.DeleteObject(request)
        except grpc.RpcError as e:
            raise ObjectError(f"Failed to delete object: {e.details()}") from e

    @retry()
    def delete_objects(self, bucket: str, keys: List[str]) -> None:
        """
        Delete multiple objects from a bucket.

        Args:
            bucket (str): The bucket name.
            keys (List[str]): A list of object keys to delete.

        Raises:
            ObjectError: If deletion fails.
        """
        try:
            objects = [pb.ObjectIdentifier(key=key) for key in keys]
            request = pb.DeleteObjectsRequest(bucket=bucket, objects=objects)
            response = self.client.DeleteObjects(request)
            
            if len(response.deletedObjects) != len(keys):
                raise ObjectError("Some objects failed to delete")
        except grpc.RpcError as e:
            raise ObjectError(f"Failed to delete objects: {e.details()}") from e
    
    @retry()
    def head_object(self, bucket: str, key: str) -> HeadObjectOutput:
        """
        Retrieve metadata for an object without downloading it.

        Args:
            bucket (str): The bucket name.
            key (str): The object key.

        Returns:
            HeadObjectOutput: The metadata of the object.

        Raises:
            BucketError: If the bucket does not exist or is not accessible.
            ObjectError: If the object metadata retrieval fails.
        """
        try:
            request = pb.HeadObjectRequest(bucket=bucket, key=key)
            response = self.client.HeadObject(request)

            if not response or not response.metadata:
                raise ObjectError("No metadata received", operation="HEAD")

            return HeadObjectOutput(
                content_type=getattr(response.metadata, 'content_type', ''),
                content_encoding=getattr(response.metadata, 'content_encoding', None),
                content_language=getattr(response.metadata, 'content_language', None),
                content_length=getattr(response.metadata, 'size', 0),
                last_modified=getattr(response.metadata, 'last_modified', datetime.now()).ToDatetime(),
                etag=getattr(response.metadata, 'etag', ''),
                user_metadata=getattr(response.metadata, 'user_metadata', {}),
                server_side_encryption=getattr(response.metadata, 'server_side_encryption', None),
                version_id=getattr(response.metadata, 'version_id', None)
            )
        except grpc.RpcError as e:
            error_msg = e.details() if hasattr(e, 'details') else str(e)
            if "bucket" in error_msg.lower():
                raise BucketError(error_msg) from e
            raise ObjectError(f"Failed to get object metadata: {error_msg}", operation="HEAD") from e
    
    @retry()
    def list_objects(self, bucket: str, options: Optional[ListObjectsOptions] = None) -> Iterator[str]:
        """
        List objects in a bucket with optional filtering.

        Args:
            bucket (str): The bucket name.
            options (Optional[ListObjectsOptions]): Filtering options.
                Defaults to None.

        Yields:
            Iterator[str]: Object keys.
        
        Raises:
            BucketError: If listing fails.
        """        
        try:
            request = pb.ListObjectsRequest(bucket=bucket)
            if options:
                if options.prefix:
                    request.prefix = options.prefix
                if options.start_after:
                    request.start_after = options.start_after
                if options.max_keys:
                    request.max_keys = options.max_keys

            # Buffer all responses to handle connection issues
            all_keys = []
            response_stream = self.client.ListObjects(request)
            try:
                for response in response_stream:
                    if response.HasField('object'):
                        all_keys.append(response.object.key)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    # Force channel reconnection
                    self.channel.subscribe(lambda s: None, try_to_connect=True)
                raise

            # Yield from buffered keys to enable iteration 
            for key in all_keys:
                yield key

        except grpc.RpcError as e:
            raise BucketError(f"Failed to list objects: {e.details()}", operation="LIST") from e
    
    @retry()
    def copy_object(self, bucket: str, copy_source: str, key: str) -> None:
        """
        Copy an object within or between buckets.

        Args:
            bucket (str): The destination bucket name.
            copy_source (str): The source bucket name and object key.
            key (str): The destination object key.

        Raises:
            ObjectError: If the copy operation fails.
        """
        try:
            request = pb.CopyObjectRequest(
                bucket=bucket,
                copySource=copy_source,
                key=key
            )
            self.client.CopyObject(request)
        except grpc.RpcError as e:
            raise ObjectError(f"Failed to copy object: {e.details()}") from e
    
    @retry()
    def head_bucket(self, bucket: str) -> HeadBucketOutput:
        """
        Retrieve metadata for a bucket.

        Args:
            bucket (str): The bucket name.

        Returns:
            HeadBucketOutput: Bucket metadata including region.

        Raises:
            BucketError: If the operation fails.
        """
        try:
            request = pb.HeadBucketRequest(bucket=bucket)
            response = self.client.HeadBucket(request)
            return HeadBucketOutput(region=response.bucketRegion)
        except grpc.RpcError as e:
            raise BucketError(f"Failed to get bucket metadata: {e.details()}") from e

    @retry()
    def rotate_key(self, force: bool = False) -> None:
        """
        Rotate access keys.

        Args:
            force (bool): Whether to force key rotation even if not needed.
                Defaults to False.

        Raises:
            ConfigurationError: If key rotation fails.
        """
        try:
            creds = self._load_credentials()
            request = pb.RotateKeyRequest(
                access_key_id=creds['access_key_id'],
                force=force
            )
            response = self.client.RotateKey(request)
            
            if response.rotated:
                self._update_credentials(response.new_secret_access_key)
        except grpc.RpcError as e:
            raise ConfigurationError(f"Failed to rotate key: {e.details()}") from e

    def _update_credentials(self, new_secret_key: str) -> None:
        """
        Update the stored credentials with a new secret key.

        Args:
            new_secret_key (str): The new secret access key.
        """
        creds_path = Path.home() / '.acs' / 'credentials.yaml'
        profile = os.getenv('ACS_PROFILE', 'default')
        
        with open(creds_path) as f:
            profiles = yaml.safe_load(f)
        
        profiles[profile]['secret_access_key'] = new_secret_key
        
        with open(creds_path, 'w') as f:
            yaml.dump(profiles, f)

    @retry()
    def share_bucket(self, bucket: str) -> None:
        """
        Share a bucket with the ACS service.

        Args:
            bucket (str): The bucket name.

        Raises:
            BucketError: If sharing fails.
        """
        try:
            request = pb.ShareBucketRequest(bucketName=bucket)
            self.client.ShareBucket(request)
        except grpc.RpcError as e:
            raise BucketError(f"Failed to share bucket: {e.details()}") from e

