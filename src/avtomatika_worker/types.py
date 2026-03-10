# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from typing import Any, Awaitable, Callable

from rxon.constants import (
    ERROR_CODE_CONTRACT_VIOLATION as CONTRACT_VIOLATION_ERROR,
)
from rxon.constants import (
    ERROR_CODE_DEPENDENCY as DEPENDENCY_ERROR,
)
from rxon.constants import (
    ERROR_CODE_DEPENDENCY_MISSING as DEPENDENCY_MISSING_ERROR,
)
from rxon.constants import (
    ERROR_CODE_INTERNAL as INTERNAL_ERROR,
)
from rxon.constants import (
    ERROR_CODE_INVALID_INPUT as INVALID_INPUT_ERROR,
)
from rxon.constants import (
    ERROR_CODE_PERMANENT as PERMANENT_ERROR,
)
from rxon.constants import (
    ERROR_CODE_RESOURCE_EXHAUSTED as RESOURCE_EXHAUSTED_ERROR,
)
from rxon.constants import (
    ERROR_CODE_SECURITY as SECURITY_ERROR,
)
from rxon.constants import (
    ERROR_CODE_TIMEOUT as TIMEOUT_ERROR,
)
from rxon.constants import (
    ERROR_CODE_TRANSIENT as TRANSIENT_ERROR,
)

Middleware = Callable[[dict[str, Any], Callable[[], Awaitable[Any]]], Awaitable[Any]]
CapacityChecker = Callable[[str], bool]


class ParamValidationError(Exception):
    pass


__all__ = [
    "INVALID_INPUT_ERROR",
    "PERMANENT_ERROR",
    "TRANSIENT_ERROR",
    "RESOURCE_EXHAUSTED_ERROR",
    "SECURITY_ERROR",
    "DEPENDENCY_ERROR",
    "DEPENDENCY_MISSING_ERROR",
    "TIMEOUT_ERROR",
    "INTERNAL_ERROR",
    "CONTRACT_VIOLATION_ERROR",
    "ParamValidationError",
    "CapacityChecker",
    "Middleware",
]
