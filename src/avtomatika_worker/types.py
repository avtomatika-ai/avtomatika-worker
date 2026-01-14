from .constants import (
    ERROR_CODE_INVALID_INPUT as INVALID_INPUT_ERROR,
)
from .constants import (
    ERROR_CODE_PERMANENT as PERMANENT_ERROR,
)
from .constants import (
    ERROR_CODE_TRANSIENT as TRANSIENT_ERROR,
)


class ParamValidationError(Exception):
    """Custom exception for parameter validation errors."""


__all__ = [
    "INVALID_INPUT_ERROR",
    "PERMANENT_ERROR",
    "TRANSIENT_ERROR",
    "ParamValidationError",
]
