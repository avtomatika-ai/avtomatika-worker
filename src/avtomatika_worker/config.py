# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from _socket import gaierror, gethostbyname, gethostname
from contextlib import suppress
from logging import getLogger
from os import environ, getenv
from typing import Any, cast
from uuid import uuid4

from orjson import JSONDecodeError, loads
from rxon.validators import validate_identifier

logger = getLogger(__name__)


class WorkerConfig:
    """A class for centralized management of worker configuration.
    Reads parameters from environment variables and provides default values.
    """

    def __init__(self) -> None:
        self.WORKER_ID: str = getenv("WORKER_ID", f"worker-{uuid4()}")
        self.WORKER_TYPE: str = getenv("WORKER_TYPE", "generic-cpu-worker")
        self.WORKER_PORT: int = int(getenv("WORKER_PORT", "8083"))
        self.HOSTNAME: str = gethostname()
        self.IP_ADDRESS: str = "127.0.0.1"
        with suppress(gaierror):
            self.IP_ADDRESS = gethostbyname(self.HOSTNAME)

        self.ORCHESTRATORS: list[dict[str, Any]] = self._get_orchestrators_config()

        self.WORKER_TOKEN: str = getenv(
            "WORKER_INDIVIDUAL_TOKEN",
            getenv("WORKER_TOKEN", "your-secret-worker-token"),
        )
        self.TLS_CA_PATH: str | None = getenv("TLS_CA_PATH")
        self.TLS_CERT_PATH: str | None = getenv("TLS_CERT_PATH")
        self.TLS_KEY_PATH: str | None = getenv("TLS_KEY_PATH")

        self.COST_PER_SKILL: dict[str, float] = self._load_json_from_env("COST_PER_SKILL", default={})
        self.MAX_CONCURRENT_TASKS: int = int(getenv("MAX_CONCURRENT_TASKS", "10"))
        self.RESOURCES: dict[str, Any] = {
            "cpu_cores": int(getenv("CPU_CORES", "4")),
            "ram_gb": float(getenv("RAM_GB", "0.0")),
            "devices": self._get_devices(),
        }

        self.INSTALLED_SOFTWARE: dict[str, str] = self._load_json_from_env(
            "INSTALLED_SOFTWARE",
            default={"python": "3.11"},
        )
        self.INSTALLED_ARTIFACTS: list[dict[str, str]] = self._load_json_from_env(
            "INSTALLED_ARTIFACTS",
            default=[],
        )

        self.TASK_FILES_DIR: str = getenv("TASK_FILES_DIR", "/tmp/payloads")
        self.S3_ENDPOINT_URL: str | None = getenv("S3_ENDPOINT_URL")
        self.S3_ACCESS_KEY: str | None = getenv("S3_ACCESS_KEY")
        self.S3_SECRET_KEY: str | None = getenv("S3_SECRET_KEY")
        self.S3_DEFAULT_BUCKET: str = getenv("S3_DEFAULT_BUCKET", "avtomatika-payloads")
        self.S3_REGION: str = getenv("S3_REGION", "us-east-1")

        self.HEARTBEAT_INTERVAL: float = float(getenv("HEARTBEAT_INTERVAL", "15"))
        self.RESULT_MAX_RETRIES: int = int(getenv("RESULT_MAX_RETRIES", "5"))
        self.RESULT_RETRY_INITIAL_DELAY: float = float(
            getenv("RESULT_RETRY_INITIAL_DELAY", "1.0"),
        )
        self.REGISTRATION_RETRY_INITIAL_DELAY: float = float(getenv("REGISTRATION_RETRY_INITIAL_DELAY", "1.0"))
        self.REGISTRATION_RETRY_MAX_DELAY: float = float(getenv("REGISTRATION_RETRY_MAX_DELAY", "60.0"))
        self.HEARTBEAT_DEBOUNCE_DELAY: float = float(getenv("WORKER_HEARTBEAT_DEBOUNCE_DELAY", 0.1))
        self.TASK_POLL_TIMEOUT: float = float(getenv("TASK_POLL_TIMEOUT", "30"))
        self.TASK_POLL_ERROR_DELAY: float = float(
            getenv("TASK_POLL_ERROR_DELAY", "5.0"),
        )
        self.IDLE_POLL_DELAY: float = float(getenv("IDLE_POLL_DELAY", "0.01"))
        self.SHUTDOWN_TIMEOUT: float = float(getenv("WORKER_SHUTDOWN_TIMEOUT", "30.0"))
        self.ENABLE_WEBSOCKETS: bool = getenv("WORKER_ENABLE_WEBSOCKETS", "false").lower() == "true"
        self.MULTI_ORCHESTRATOR_MODE: str = getenv("MULTI_ORCHESTRATOR_MODE", "WATERFALL")
        self.WORKER_SKILLS_DIR: str = getenv("WORKER_SKILLS_DIR", "skills")
        self.STRICT_EVENT_VALIDATION: bool = getenv("STRICT_EVENT_VALIDATION", "true").lower() == "true"
        self.WORKER_ENABLE_METRICS: bool = getenv("WORKER_ENABLE_METRICS", "false").lower() == "true"

        self.EXTRA_CAPABILITIES: dict[str, Any] = self._load_extra_from_env()

    def _load_extra_from_env(self) -> dict[str, Any]:
        """Loads all environment variables starting with WORKER_EXTRA_ into a dictionary."""
        extra = {}
        prefix = "WORKER_EXTRA_"
        for key, value in environ.items():
            if key.startswith(prefix):
                name = key[len(prefix) :].lower()
                if value.startswith(("{", "[")):
                    try:
                        extra[name] = loads(value)
                    except (JSONDecodeError, TypeError):
                        extra[name] = value
                else:
                    extra[name] = value
        return extra

    def validate(self) -> None:
        """Validates critical configuration parameters."""
        validate_identifier(self.WORKER_ID, "WORKER_ID")
        if self.WORKER_TOKEN == "your-secret-worker-token":
            logger.warning("WORKER_TOKEN is set to the default value. Tasks might fail authentication.")

        if not self.ORCHESTRATORS:
            raise ValueError("No orchestrators configured.")

        for o in self.ORCHESTRATORS:
            if not o.get("url"):
                raise ValueError("Orchestrator configuration missing URL.")

    def _get_orchestrators_config(self) -> list[dict[str, Any]]:
        if orchestrators_json := getenv("ORCHESTRATORS_CONFIG"):
            try:
                orchestrators = loads(orchestrators_json)
                if getenv("ORCHESTRATOR_URL"):
                    logger.info("Both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set. Using ORCHESTRATORS_CONFIG.")
                for o in orchestrators:
                    if "priority" not in o:
                        o["priority"] = 10
                    if "weight" not in o:
                        o["weight"] = 1
                orchestrators.sort(key=lambda x: (x.get("priority", 10), x.get("url")))
                return cast(list[dict[str, Any]], orchestrators)
            except JSONDecodeError:
                logger.warning("Could not decode JSON from ORCHESTRATORS_CONFIG. Falling back to default.")

        orchestrator_url = getenv("ORCHESTRATOR_URL", "http://localhost:8080")
        return [{"url": orchestrator_url, "priority": 1, "weight": 1}]

    @staticmethod
    def _get_devices() -> list[dict[str, Any]] | None:
        """Collects device information from environment variables.
        Returns a list of HardwareDevice-compatible dictionaries.
        """
        devices = []
        if gpu_model := getenv("GPU_MODEL"):
            devices.append(
                {
                    "type": "gpu",
                    "model": gpu_model,
                    "id": getenv("GPU_ID", "0"),
                    "properties": {
                        "memory_gb": int(getenv("GPU_VRAM_GB", "0")),
                    },
                }
            )
        if generic_devices_json := getenv("WORKER_DEVICES"):
            try:
                generic_devices = loads(generic_devices_json)
                if isinstance(generic_devices, list):
                    devices.extend(generic_devices)
            except (JSONDecodeError, TypeError):
                logger.warning("Could not decode JSON from WORKER_DEVICES.")

        return devices if devices else None

    @staticmethod
    def _load_json_from_env(key: str, default: Any) -> Any:
        """Safely loads a JSON string from an environment variable."""
        if value := getenv(key):
            try:
                return loads(value)
            except JSONDecodeError:
                logger.warning("Could not decode JSON from environment variable %s.", key)
                return default
        return default
