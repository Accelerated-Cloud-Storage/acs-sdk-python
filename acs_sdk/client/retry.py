# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
import time
from functools import wraps
from typing import Type, Callable, Any, Union, Tuple
import grpc
from .exceptions import ACSError

RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}

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
                        
                        # Only retry on specific status codes
                        if status_code not in RETRYABLE_STATUS_CODES:
                            raise

                    # Don't sleep on the last attempt
                    if attempt < max_attempts - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * backoff_multiplier, max_backoff)

            raise ACSError(
                f"Operation failed after {max_attempts} attempts: {str(last_exception)}"
            ) from last_exception

        return wrapper
    return decorator
