class ACSError(Exception):
    """Base exception for ACS client errors."""
    def __init__(self, message: str, code: str = "ERR_UNKNOWN"):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")

class AuthenticationError(ACSError):
    """Authentication failed."""
    def __init__(self, message: str):
        super().__init__(message, code="ERR_AUTH")

class BucketError(ACSError):
    """Bucket operation failed."""
    def __init__(self, message: str, operation: str = None):
        code = "ERR_BUCKET"
        if operation:
            code = f"ERR_BUCKET_{operation.upper()}"
        super().__init__(message, code=code)

class ObjectError(ACSError):
    """Object operation failed."""
    def __init__(self, message: str, operation: str = None):
        code = "ERR_OBJECT"
        if operation:
            code = f"ERR_OBJECT_{operation.upper()}"
        super().__init__(message, code=code)

class ConfigurationError(ACSError):
    """Configuration or credential error."""
    def __init__(self, message: str):
        super().__init__(message, code="ERR_CONFIG")
