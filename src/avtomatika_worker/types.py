# Error codes for worker task results
TRANSIENT_ERROR = "TRANSIENT_ERROR"
PERMANENT_ERROR = "PERMANENT_ERROR"
INVALID_INPUT_ERROR = "INVALID_INPUT_ERROR"


class ParamValidationError(Exception):
    """Custom exception for parameter validation errors."""
