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
            re_authenticated_this_cycle = False # Flag to prevent infinite re-auth loops

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
                        if hasattr(e, 'code') and callable(e.code):
                            status_code = e.code()
                        elif hasattr(e, '_code'): # Fallback for some error types
                            status_code = e._code
                        
                        # Check for UNAUTHENTICATED first
                        if status_code == grpc.StatusCode.UNAUTHENTICATED:
                            if not re_authenticated_this_cycle and attempt < max_attempts - 1:
                                print(f"Caught UNAUTHENTICATED error during {func.__name__}, attempting re-authentication...")
                                try:
                                    # Assume the first arg is the client instance ('self')
                                    if args and hasattr(args[0], '_re_authenticate') and callable(getattr(args[0], '_re_authenticate')):
                                        client_instance = args[0]
                                        client_instance._re_authenticate() # Call the re-auth method
                                        re_authenticated_this_cycle = True # Set flag after successful call
                                        print(f"Re-authentication successful for {func.__name__}. Retrying operation...")
                                        # Continue the loop to retry the operation
                                        continue 
                                    else:
                                        print("Could not find _re_authenticate method on client instance. Raising original error.")
                                        raise _convert_grpc_error(e, operation) # Cannot re-authenticate
                                except Exception as reauth_err:
                                    # If re-authentication itself fails, raise that error immediately
                                    print(f"Re-authentication failed during retry for {func.__name__}: {reauth_err}. Raising error.")
                                    raise ACSError(f"Re-authentication failed: {reauth_err}") from reauth_err
                            else:
                                # Already tried re-authenticating or it's the last attempt
                                print(f"UNAUTHENTICATED error on attempt {attempt + 1}/{max_attempts} for {func.__name__} (already tried re-auth: {re_authenticated_this_cycle}). Raising error.")
                                raise _convert_grpc_error(e, operation)

                        # If not UNAUTHENTICATED, check other retryable codes
                        elif status_code in RETRYABLE_STATUS_CODES:
                            print(f"Caught retryable gRPC error ({status_code}) during {func.__name__}. Attempt {attempt + 1}/{max_attempts}. Retrying after {backoff:.2f}s...")
                            # Proceed with standard backoff/retry logic below
                            pass 
                        else:
                            # Not UNAUTHENTICATED and not in standard retryable codes
                            print(f"Caught non-retryable gRPC error ({status_code}) during {func.__name__}. Raising error.")
                            raise _convert_grpc_error(e, operation)
                    else:
                        # Handle non-gRPC exceptions if they are in retryable_exceptions
                        # (Currently defaults to only grpc.RpcError, so this part might not be hit)
                        print(f"Caught non-gRPC retryable exception {type(e).__name__} during {func.__name__}. Attempt {attempt + 1}/{max_attempts}. Retrying after {backoff:.2f}s...")
                        # Proceed with standard backoff/retry logic below
                        pass

                    # Common backoff logic for retryable errors (excluding UNAUTHENTICATED which uses 'continue')
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
