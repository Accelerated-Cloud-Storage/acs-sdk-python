class ACSError(Exception):
    """Base exception for ACS client errors."""
    pass

class AuthenticationError(ACSError):
    """Authentication failed."""
    pass

class BucketError(ACSError):
    """Bucket operation failed."""
    pass

class ObjectError(ACSError):
    """Object operation failed."""
    pass

class ConfigurationError(ACSError):
    """Configuration or credential error."""
    pass
