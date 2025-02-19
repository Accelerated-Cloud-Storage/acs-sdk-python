import time
from functools import wraps
from typing import Type, Callable, Any, Union, Tuple
import grpc
from .exceptions import ACSError

RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.DEADLINE_EXCEEDED
}

def retry(
    max_attempts: int = 3,
    initial_backoff: float = 0.1,
    max_backoff: float = 1.0,
    backoff_multiplier: float = 2.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (grpc.RpcError,)
) -> Callable:
    """
    Retry decorator with exponential backoff for gRPC operations.
    
    Args:
        max_attempts: Maximum number of retry attempts
        initial_backoff: Initial backoff time in seconds
        max_backoff: Maximum backoff time in seconds
        backoff_multiplier: Multiplier for exponential backoff
        retryable_exceptions: Tuple of exceptions that should trigger a retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception = None
            backoff = initial_backoff

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if isinstance(e, grpc.RpcError):
                        status_code = grpc.StatusCode.UNKNOWN
                        if hasattr(e, 'code'):
                            status_code = e.code()
                        
                        # Don't retry if not a retryable status code
                        if status_code not in RETRYABLE_STATUS_CODES:
                            raise

                    # Don't sleep on the last attempt
                    if attempt < max_attempts - 1:
                        time.sleep(backoff)
                        backoff = min(backoff * backoff_multiplier, max_backoff)

            # If we get here, we've exhausted our retries
            raise ACSError(f"Operation failed after {max_attempts} attempts") from last_exception

        return wrapper
    return decorator
