# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""Module providing a retry decorator with exponential backoff for ACS client operations."""
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
    """Convert gRPC errors to appropriate ACS errors.
    
    Args:
        e (grpc.RpcError): The gRPC error to convert.
        operation (str, optional): The operation being performed. Defaults to None.
        
    Returns:
        Union[BucketError, ObjectError, ACSError]: The converted error.
    """
    error_msg = str(e.details() if hasattr(e, 'details') else str(e))
    
    # Handle bucket-related errors
    if "bucket" in error_msg.lower():
        if "not empty" in error_msg.lower():
            return BucketError(error_msg)
        if "404" in error_msg or "not found" in error_msg.lower():
            return BucketError(f"Bucket does not exist: {error_msg}")
        if "403" in error_msg or "forbidden" in error_msg.lower():
            return BucketError(f"Bucket access denied: {error_msg}")
        if "not accessible" in error_msg.lower():
            return BucketError(error_msg)
        return BucketError(error_msg)
    
    # Handle object-related errors
    if "object" in error_msg.lower() or operation in ["HEAD", "GET", "PUT", "DELETE"]:
        if "404" in error_msg or "not found" in error_msg.lower():
            return ObjectError(f"Object does not exist: {error_msg}", operation=operation)
        if "403" in error_msg or "forbidden" in error_msg.lower():
            return ObjectError(f"Object access denied: {error_msg}", operation=operation)
        return ObjectError(error_msg, operation=operation)
        
    return ACSError(error_msg)

def retry(
    max_attempts: int = 5,  # Increased default attempts
    initial_backoff: float = 0.1,
    max_backoff: float = 5.0,  # Increased max backoff
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (grpc.RpcError,)
) -> Callable:
    """Decorator for retrying a function with exponential backoff.

    Args:
        max_attempts (int): Maximum number of retry attempts.
        initial_backoff (float): Initial backoff time in seconds.
        max_backoff (float): Maximum backoff time in seconds.
        backoff_multiplier (float): Multiplier for exponential backoff.
        retryable_exceptions (Tuple[Type[Exception], ...]): Exceptions that trigger a retry.

    Returns:
        Callable: A decorator that wraps the function.
    """
    def decorator(func: Callable) -> Callable:
        """Wraps a function to add retry logic.

        Args:
            func (Callable): The function to be retried.

        Returns:
            Callable: The wrapped function.
        """
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Executes the function with retry logic and exponential backoff.

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
