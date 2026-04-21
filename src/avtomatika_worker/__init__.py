# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

"""A Python SDK for creating workers for the Avtomatika Orchestrator."""

from importlib.metadata import PackageNotFoundError, version

from .logging import setup_logging
from .observability import ObservabilityManager
from .task_files import TaskFiles
from .worker import (
    DeviceUsage,
    ResourcesUsage,
    SkillBlueprint,
    SkillInfo,
    Worker,
    is_valid_identifier,
    validate_identifier,
)

__all__ = [
    "Worker",
    "SkillBlueprint",
    "SkillInfo",
    "TaskFiles",
    "setup_logging",
    "ObservabilityManager",
    "ResourcesUsage",
    "DeviceUsage",
    "is_valid_identifier",
    "validate_identifier",
]

try:
    __version__ = version("avtomatika-worker")
except PackageNotFoundError:
    __version__ = "unknown"
