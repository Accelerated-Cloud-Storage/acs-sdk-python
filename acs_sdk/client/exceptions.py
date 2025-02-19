# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
class ACSError(Exception):
    """Base exception for ACS client errors.

    Args:
        message (str): Error message.
        code (str, optional): Error code. Defaults to "ERR_UNKNOWN".
    """
    def __init__(self, message: str, code: str = "ERR_UNKNOWN"):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

class AuthenticationError(ACSError):
    """Exception raised when authentication fails.

    Args:
        message (str): Error message.
    """
    def __init__(self, message: str):
        super().__init__(message, code="ERR_AUTH")

class BucketError(ACSError):
    """Exception raised for bucket operation failures.

    Args:
        message (str): Error message.
        operation (str, optional): The bucket operation that failed.
    """
    def __init__(self, message: str, operation: str = None):
        code = "ERR_BUCKET"
        if operation:
            code = f"ERR_BUCKET_{operation.upper()}"
        super().__init__(message, code=code)

class ObjectError(ACSError):
    """Exception raised for object operation failures.

    Args:
        message (str): Error message.
        operation (str, optional): The object operation that failed.
    """
    def __init__(self, message: str, operation: str = None):
        code = "ERR_OBJECT"
        if operation:
            code = f"ERR_OBJECT_{operation.upper()}"
        super().__init__(message, code=code)

class ConfigurationError(ACSError):
    """Exception raised for configuration or credential errors.

    Args:
        message (str): Error message.
    """
    def __init__(self, message: str):
        super().__init__(message, code="ERR_CONFIG")
