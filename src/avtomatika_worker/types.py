from typing import Any, Awaitable, Callable, Dict

from rxon.constants import (
    ERROR_CODE_INVALID_INPUT as INVALID_INPUT_ERROR,
)
from rxon.constants import (
    ERROR_CODE_PERMANENT as PERMANENT_ERROR,
)
from rxon.constants import (
    ERROR_CODE_TRANSIENT as TRANSIENT_ERROR,
)

Middleware = Callable[[Dict[str, Any], Callable[[], Awaitable[Any]]], Awaitable[Any]]
CapacityChecker = Callable[[str], bool]


class ParamValidationError(Exception):
    pass


__all__ = [
    "INVALID_INPUT_ERROR",
    "PERMANENT_ERROR",
    "TRANSIENT_ERROR",
    "ParamValidationError",
]
