class InvalidParametersError(Exception):
    """Custom error raised when invalid parameters are provided to a handler."""
    pass


class MethodNotFoundError(Exception):
    """Custom error raised when a method is not found or not implemented."""
    pass
