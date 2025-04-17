# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Retry Module.

This module provides a retry decorator with exponential backoff for ACS client operations.
It handles transient errors and network issues by automatically retrying failed operations
with increasing delays between attempts.

Functions:
    retry: Decorator for retrying functions with exponential backoff.
    _convert_grpc_error: Helper function to convert gRPC errors to ACS-specific exceptions.
"""
import time
from functools import wraps
from typing import Type, Callable, Any, Union, Tuple
import grpc
from .exceptions import ACSError, BucketError, ObjectError

RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}

def _convert_grpc_error(e: grpc.RpcError, operation: str = None) -> Union[BucketError, ObjectError, ACSError]:
    """
    Convert gRPC errors to appropriate ACS errors.
    
    This function analyzes gRPC errors and converts them to more specific
    ACS exception types based on the error message and context.
    
    Args:
        e (grpc.RpcError): The gRPC error to convert.
        operation (str, optional): The operation being performed. Defaults to None.
        
    Returns:
        Union[BucketError, ObjectError, ACSError]: The converted error.
    """
    error_msg = str(e.details() if hasattr(e, 'details') else str(e))
    error_code = e.code() if hasattr(e, 'code') else None
    
    # Handle bucket-related errors
    if "bucket" in error_msg.lower():
        if "not empty" in error_msg.lower():
            return BucketError("Bucket is not empty", operation="DELETE")
        if any(x in error_msg.lower() for x in ["404", "not found"]):
            return BucketError("Bucket does not exist", operation="ACCESS") 
        if any(x in error_msg.lower() for x in ["403", "forbidden", "unauthorized"]):
            return BucketError("Access denied to bucket", operation="AUTH")
        if "not accessible" in error_msg.lower():
            return BucketError("Bucket is not accessible", operation="ACCESS")
        if "already exists" in error_msg.lower():
            return BucketError("Bucket already exists", operation="CREATE")
        if "invalid name" in error_msg.lower():
            return BucketError("Invalid bucket name", operation="VALIDATE")
        return BucketError(error_msg)

    # Handle object-related errors
    if "object" in error_msg.lower() or operation in ["HEAD", "GET", "PUT", "DELETE", "COPY", "LIST"]:
        if any(x in error_msg.lower() for x in ["404", "not found"]):
            return ObjectError("Object does not exist", operation=operation)
        if any(x in error_msg.lower() for x in ["403", "forbidden", "unauthorized"]):
            return ObjectError("Access denied to object", operation=operation)
        if "too large" in error_msg.lower():
            return ObjectError("Object size exceeds limits", operation=operation)
        if "checksum" in error_msg.lower():
            return ObjectError("Object checksum mismatch", operation=operation)
        if "encryption" in error_msg.lower():
            return ObjectError("Encryption error with object", operation=operation)
        if "storage class" in error_msg.lower():
            return ObjectError("Invalid storage class for object", operation=operation)
        return ObjectError(error_msg, operation=operation)

    # Handle specific gRPC status codes
    if error_code:
        if error_code == grpc.StatusCode.DEADLINE_EXCEEDED:
            return ACSError("Request timed out", code="ERR_TIMEOUT")
        if error_code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            return ACSError("Rate limit exceeded", code="ERR_RATE_LIMIT")
        if error_code == grpc.StatusCode.UNAVAILABLE:
            return ACSError("Service unavailable", code="ERR_UNAVAILABLE")
        if error_code == grpc.StatusCode.INTERNAL:
            return ACSError("Internal server error", code="ERR_INTERNAL")
        if error_code == grpc.StatusCode.UNAUTHENTICATED:
            return ACSError("Authentication failed", code="ERR_AUTH")
    
    return ACSError(error_msg)

def retry(
    max_attempts: int = 5,  # Increased default attempts
    initial_backoff: float = 0.1,
    max_backoff: float = 5.0,  # Increased max backoff
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (grpc.RpcError,)
) -> Callable:
    """
    Decorator for retrying a function with exponential backoff.
    
    This decorator wraps a function to automatically retry it when specified
    exceptions occur, with an exponential backoff delay between attempts.
    
    Args:
        max_attempts (int): Maximum number of retry attempts. Defaults to 5.
        initial_backoff (float): Initial backoff time in seconds. Defaults to 0.1.
        max_backoff (float): Maximum backoff time in seconds. Defaults to 5.0.
        backoff_multiplier (float): Multiplier for exponential backoff. Defaults to 2.0.
        retryable_exceptions (Tuple[Type[Exception], ...]): Exceptions that trigger a retry.
            Defaults to (grpc.RpcError,).

    Returns:
        Callable: A decorator that wraps the function.
    """
    def decorator(func: Callable) -> Callable:
        """
        Wraps a function to add retry logic.
        
        Args:
            func (Callable): The function to be retried.

        Returns:
            Callable: The wrapped function.
        """
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Executes the function with retry logic and exponential backoff.
            
            Returns:
                Any: Result of the function call.

            Raises:
                ACSError: If all retry attempts fail.
            """
            last_exception = None
            backoff = initial_backoff

            # Extract operation from function name if possible
            operation = None
            if func.__name__ in ["head_object", "get_object", "put_object", "delete_object"]:
                operation = func.__name__.split("_")[0].upper()

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if isinstance(e, grpc.RpcError):
                        # Get status code with better error handling
                        status_code = grpc.StatusCode.UNKNOWN
                        if hasattr(e, 'code'):
                            status_code = e.code()
                        elif hasattr(e, '_code'):
                            status_code = e._code
                        
                        # Convert error and raise if not retryable
                        if status_code not in RETRYABLE_STATUS_CODES:
                            raise _convert_grpc_error(e, operation)

                    # Don't sleep on the last attempt
                    if attempt < max_attempts - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * backoff_multiplier, max_backoff)

            # If we get here, we've exhausted all retries
            if isinstance(last_exception, grpc.RpcError):
                raise _convert_grpc_error(last_exception, operation)
            
            raise ACSError(
                f"Operation failed after {max_attempts} attempts: {str(last_exception)}"
            ) from last_exception

        return wrapper
    return decorator
