# Copyright 2025 Accelerated Cloud Storage Corporation. All Rights Reserved.
"""
Exceptions Module.

This module defines the exception hierarchy for the ACS client, providing
specific exception types for different categories of errors that may occur
during ACS operations.

Classes:
    ACSError: Base exception for all ACS client errors.
    AuthenticationError: Exception for authentication failures.
    BucketError: Exception for bucket operation failures.
    ObjectError: Exception for object operation failures.
    ConfigurationError: Exception for configuration or credential errors.
"""
class ACSError(Exception):
    """
    Base exception for ACS client errors.
    
    This is the parent class for all ACS-specific exceptions, providing
    a common structure for error codes and messages.
    
    Args:
        message (str): Error message.
        code (str, optional): Error code. Defaults to "ERR_UNKNOWN".
        
    Attributes:
        code (str): Error code for categorizing the error.
        message (str): Descriptive error message.
    """
    def __init__(self, message: str, code: str = "ERR_UNKNOWN"):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

class AuthenticationError(ACSError):
    """
    Exception raised when authentication fails.
    
    This exception is raised when there are issues with authentication,
    such as invalid credentials or expired tokens.
    
    Args:
        message (str): Error message.
        
    Attributes:
        code (str): Error code, set to "ERR_AUTH".
        message (str): Descriptive error message.
    """
    def __init__(self, message: str):
        super().__init__(message, code="ERR_AUTH")

class BucketError(ACSError):
    """
    Exception raised for bucket operation failures.
    
    This exception is raised when operations on buckets fail, such as
    creating, deleting, or accessing buckets.
    
    Args:
        message (str): Error message.
        operation (str, optional): The bucket operation that failed.
            Defaults to None.
            
    Attributes:
        code (str): Error code, formatted as "ERR_BUCKET" or "ERR_BUCKET_{OPERATION}".
        message (str): Descriptive error message.
    """
    def __init__(self, message: str, operation: str = None):
        code = "ERR_BUCKET"
        if operation:
            code = f"ERR_BUCKET_{operation.upper()}"
        super().__init__(message, code=code)

class ObjectError(ACSError):
    """
    Exception raised for object operation failures.
    
    This exception is raised when operations on objects fail, such as
    uploading, downloading, or deleting objects.
    
    Args:
        message (str): Error message.
        operation (str, optional): The object operation that failed.
            Defaults to None.
            
    Attributes:
        code (str): Error code, formatted as "ERR_OBJECT" or "ERR_OBJECT_{OPERATION}".
        message (str): Descriptive error message.
    """
    def __init__(self, message: str, operation: str = None):
        code = "ERR_OBJECT"
        if operation:
            code = f"ERR_OBJECT_{operation.upper()}"
        super().__init__(message, code=code)

class ConfigurationError(ACSError):
    """
    Exception raised for configuration or credential errors.
    
    This exception is raised when there are issues with the client configuration
    or credentials, such as missing or invalid configuration files.
    
    Args:
        message (str): Error message.
        
    Attributes:
        code (str): Error code, set to "ERR_CONFIG".
        message (str): Descriptive error message.
    """
    def __init__(self, message: str):
        super().__init__(message, code="ERR_CONFIG")
