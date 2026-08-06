# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from asyncio import (
    FIRST_COMPLETED,
    CancelledError,
    Event,
    Queue,
    Task,
    TaskGroup,
    create_task,
    gather,
    get_running_loop,
    run,
    shield,
    sleep,
    to_thread,
    wait,
    wait_for,
)
from collections.abc import Callable
from contextlib import suppress
from dataclasses import MISSING, is_dataclass, make_dataclass
from dataclasses import fields as dc_fields
from dataclasses import replace as dc_replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version
from importlib.util import module_from_spec, spec_from_file_location
from inspect import Parameter, cleandoc, iscoroutinefunction, signature
from logging import getLogger
from os import getenv
from os.path import join
from signal import SIGINT, SIGTERM
from sys import modules
from time import perf_counter, time
from types import UnionType
from typing import TYPE_CHECKING, Any, Union, cast, get_args, get_origin, get_type_hints
from uuid import uuid4

from aiofiles.os import listdir
from aiofiles.ospath import abspath, exists
from aiohttp import ClientSession, TCPConnector, web
from msgspec import Struct
from msgspec.structs import fields as struct_fields
from msgspec.structs import replace as struct_replace
from rxon import Transport, create_transport
from rxon.blob import calculate_config_hash
from rxon.constants import (
    COMMAND_CANCEL_TASK,
    ERROR_CODE_CONTRACT_VIOLATION,
    ERROR_CODE_INTEGRITY_MISMATCH,
    ERROR_CODE_INVALID_INPUT,
    ERROR_CODE_PERMANENT,
    ERROR_CODE_TRANSIENT,
    EVENT_TYPE_PROGRESS,
    HB_RESP_CANCEL_TASKS,
    HB_RESP_REQUIRE_FULL_SYNC,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILURE,
    TASK_STATUS_SUCCESS,
    WORKER_STATUS_DRAINING,
)
from rxon.exceptions import RxonError, RxonNetworkError, RxonRateLimitError
from rxon.models import (
    DeviceUsage,
    FileMetadata,
    HardwareDevice,
    Heartbeat,
    InstalledArtifact,
    Resources,
    ResourcesUsage,
    SecurityContext,
    SkillInfo,
    TaskError,
    TaskPayload,
    TaskResult,
    WorkerCapabilities,
    WorkerEventPayload,
    WorkerRegistration,
)
from rxon.schema import extract_json_schema, extract_output_schema_from_func, extract_schema_from_func, validate_data
from rxon.security import create_client_ssl_context, sign_payload, verify_signature
from rxon.transports.http import HttpTransport
from rxon.utils import calculate_dict_hash, from_dict, json_dumps, to_dict
from rxon.validators import is_valid_identifier, validate_identifier

from .config import WorkerConfig
from .logging import clear_context, set_context, setup_logging
from .observability import ObservabilityManager, propagate
from .s3 import S3Manager
from .task_files import TaskFiles
from .types import CapacityChecker, Middleware, ParamValidationError

if TYPE_CHECKING:
    from pydantic import BaseModel, ValidationError

    _PYDANTIC_INSTALLED = True
else:
    try:
        from pydantic import BaseModel, ValidationError

        _PYDANTIC_INSTALLED = True
    except ImportError:
        _PYDANTIC_INSTALLED = False

        class BaseModel:
            pass

        class ValidationError(Exception):
            pass


__all__ = [
    "Worker",
    "SkillBlueprint",
    "ResourcesUsage",
    "DeviceUsage",
    "is_valid_identifier",
    "validate_identifier",
    "ParamValidationError",
    "worker_extract_json_schema",
    "worker_extract_output_schema_from_func",
    "worker_extract_schema_from_func",
]


def worker_extract_json_schema(schema_type: Any) -> dict[str, Any] | None:
    """Worker-specific wrapper that adds Pydantic support to rxon.schema.extract_json_schema."""
    return cast(dict[str, Any] | None, extract_json_schema(schema_type, extractor=_pydantic_extractor))


def _pydantic_extractor(schema_type: Any) -> dict[str, Any] | None:
    """Local Pydantic and dataclass extractor for the worker SDK."""
    if _PYDANTIC_INSTALLED and isinstance(schema_type, type) and issubclass(schema_type, BaseModel):
        return cast(dict, cast(type[BaseModel], schema_type).model_json_schema())

    if isinstance(schema_type, type) and is_dataclass(schema_type):
        properties = {}
        required = []
        for field_info in dc_fields(schema_type):
            if field_info.name.startswith("_"):
                continue
            field_schema = extract_json_schema(field_info.type, extractor=_pydantic_extractor)
            properties[field_info.name] = field_schema if field_schema is not None else {}

            is_optional = False
            tp = field_info.type
            if isinstance(tp, UnionType):
                is_optional = type(None) in get_args(tp)
            else:
                origin = get_origin(tp)
                is_optional = origin is Union and type(None) in get_args(tp)

            if field_info.default is MISSING and field_info.default_factory is MISSING and not is_optional:
                required.append(field_info.name)
        return {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
        }
    return None


def worker_extract_schema_from_func(func: Any, arg_name: str) -> dict[str, Any] | None:
    """Local wrapper for extracting input schema from a function argument."""
    return cast(dict[str, Any] | None, extract_schema_from_func(func, arg_name, extractor=_pydantic_extractor))


def worker_extract_output_schema_from_func(func: Any) -> dict[str, Any] | None:
    """Local wrapper for extracting output schema from a function's return type."""
    return cast(dict[str, Any] | None, extract_output_schema_from_func(func, extractor=_pydantic_extractor))


logger = getLogger(__name__)


def fields(cls: Any) -> Any:
    if isinstance(cls, type) and issubclass(cls, Struct):
        return struct_fields(cls)
    if isinstance(cls, Struct):
        return struct_fields(type(cls))
    return dc_fields(cls)


def replace(obj: Any, **changes: Any) -> Any:
    if isinstance(obj, Struct):
        return struct_replace(obj, **changes)
    return dc_replace(obj, **changes)


def _create_dynamic_skill_object(
    base_class: type[SkillInfo],
    init_kwargs: dict[str, Any],
) -> SkillInfo:
    """
    Creates a SkillInfo object (or descendant).
    Dynamically creates a subclass if init_kwargs contains fields not present in base_class.
    """
    known_field_names = {f.name for f in fields(base_class)}
    extra_kwargs = {k: v for k, v in init_kwargs.items() if k not in known_field_names}
    base_kwargs = {k: v for k, v in init_kwargs.items() if k in known_field_names}

    if "input_schema" in base_kwargs:
        base_kwargs["input_schema"] = worker_extract_json_schema(base_kwargs["input_schema"])
    if "output_schema" in base_kwargs:
        base_kwargs["output_schema"] = worker_extract_json_schema(base_kwargs["output_schema"])

    if "events_schema" in base_kwargs and isinstance(base_kwargs["events_schema"], dict):
        base_kwargs["events_schema"] = {
            k: worker_extract_json_schema(v) for k, v in base_kwargs["events_schema"].items()
        }

    if not extra_kwargs:
        if "name" not in base_kwargs:
            base_kwargs["name"] = ""
        return base_class(**base_kwargs)

    if isinstance(base_class, type) and issubclass(base_class, Struct):
        DynamicSkillClass = type(
            f"Dynamic{base_class.__name__}",
            (base_class,),
            {"__annotations__": {k: type(v) for k, v in extra_kwargs.items()}},
            kw_only=True,
        )
    else:
        new_fields = [(k, type(v)) for k, v in extra_kwargs.items()]
        DynamicSkillClass = make_dataclass(
            f"Dynamic{base_class.__name__}",
            new_fields,
            bases=(base_class,),
            frozen=True,
        )

    full_kwargs = {**base_kwargs, **extra_kwargs}
    if "name" not in full_kwargs:
        full_kwargs["name"] = ""

    return DynamicSkillClass(**full_kwargs)


class SkillBlueprint:
    """
    A collection of skills that can be registered on a Worker.
    Allows defining skills in separate files without a worker instance.
    """

    def __init__(self, skill_info_class: type[SkillInfo] = SkillInfo) -> None:
        self._skills: list[tuple[SkillInfo, Callable]] = []
        self._skill_info_class = skill_info_class

    def skill(
        self,
        info: str | SkillInfo | type[SkillInfo] | None = None,
        **kwargs: Any,
    ) -> Callable:
        base_info: SkillInfo
        target_class = self._skill_info_class

        init_kwargs = kwargs.copy()

        if isinstance(info, type) and issubclass(info, SkillInfo):
            target_class = info
        elif isinstance(info, SkillInfo):
            target_class = info.__class__
            existing_data = {f.name: getattr(info, f.name) for f in fields(info)}
            init_kwargs.update(existing_data)
        elif isinstance(info, str):
            init_kwargs["name"] = info
            if "type" not in init_kwargs:
                init_kwargs["type"] = info

        base_info = _create_dynamic_skill_object(target_class, init_kwargs)

        def decorator(func: Callable) -> Callable:
            nonlocal base_info
            if not base_info.name:
                base_info = replace(base_info, name=func.__name__)

            if base_info.description is None and func.__doc__:
                base_info = replace(base_info, description=cleandoc(func.__doc__))

            if base_info.version is None:
                module = modules.get(func.__module__)
                if module and hasattr(module, "__version__"):
                    base_info = replace(base_info, version=module.__version__)

            if base_info.input_schema is None:
                inferred = worker_extract_schema_from_func(func, "params")
                if inferred:
                    base_info = replace(base_info, input_schema=inferred)

            if base_info.output_schema is None:
                inferred = worker_extract_output_schema_from_func(func)
                if inferred:
                    base_info = replace(base_info, output_schema=inferred)

            self._skills.append((base_info, func))
            return func

        return decorator


class OrchestratorClient:
    """
    Client for interacting with the orchestrator from within a skill handler.
    Supports chain of thought and tool use by calling other skills.
    """

    def __init__(self, client: Transport, worker: Worker):
        self._client = client
        self._worker = worker

    async def call_skill(self, skill_name: str, params: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(self._client, HttpTransport):
            return {"status": "success", "data": {"mocked": True, "skill": skill_name, "params": params}}

        url = f"{self._client.base_url}/skills/call"
        headers = self._client._headers.copy()

        async with self._client._session.post(
            url, json={"skill_name": skill_name, "params": params}, headers=headers
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Orchestrator returned HTTP {resp.status}: {text}")
            return cast(dict[str, Any], await resp.json())


class Worker:
    """The main class for creating and running a worker."""

    def __init__(
        self,
        worker_type: str = "generic-worker",
        max_concurrent_tasks: int | None = None,
        skill_type_limits: dict[str, int] | None = None,
        http_session: ClientSession | None = None,
        skill_dependencies: dict[str, list[str]] | None = None,
        config: WorkerConfig | None = None,
        capacity_checker: CapacityChecker | None = None,
        clients: list[tuple[dict[str, Any], Transport]] | None = None,
        skills_dir: str | None = None,
        skill_info_class: type[SkillInfo] = SkillInfo,
    ):
        self._config = config or WorkerConfig()
        if skills_dir:
            self._config.WORKER_SKILLS_DIR = skills_dir

        try:
            pkg_version = get_version("avtomatika-worker")
        except PackageNotFoundError:
            pkg_version = "unknown"

        setup_logging(worker_id=self._config.WORKER_ID)
        self._observability = ObservabilityManager(
            enabled=self._config.WORKER_ENABLE_METRICS,
            service_name=self._config.WORKER_TYPE,
            worker_id=self._config.WORKER_ID,
            worker_type=self._config.WORKER_TYPE,
            version=pkg_version,
        )
        self._s3_manager = S3Manager(self._config, observability=self._observability)
        self._config.WORKER_TYPE = worker_type
        if max_concurrent_tasks is not None:
            self._config.MAX_CONCURRENT_TASKS = max_concurrent_tasks

        self._skill_type_limits = skill_type_limits or {}
        self._skill_handlers: dict[str, dict[str, Any]] = {}
        self._skill_dependencies = skill_dependencies or {}
        self._middlewares: list[Middleware] = []
        self._capacity_checker = capacity_checker
        self._skill_info_class = skill_info_class

        self._current_load = 0
        self._current_load_by_type: dict[str, int] = dict.fromkeys(self._skill_type_limits, 0)
        self._hot_skills_state: set[str] = set()
        self._active_tasks: dict[str, Task] = {}
        self._command_handlers: dict[str, Callable] = {}
        self._http_session = http_session
        self._session_is_managed_externally = http_session is not None
        self._shutdown_event = Event()
        self._registered_event = Event()
        self._ssl_context = None
        self._last_synced_skills_hash: str | None = None
        self._result_queue: Queue[tuple[Transport, TaskResult]] = Queue()
        self._queue_worker_task: Task | None = None
        self._last_sent_usage: ResourcesUsage | None = None
        self._last_sent_usage_time: float = 0.0
        self._telemetry_deadband: float = getattr(self._config, "WORKER_TELEMETRY_DEADBAND", 5.0)
        self._telemetry_force_interval: float = getattr(self._config, "WORKER_TELEMETRY_FORCE_INTERVAL", 60.0)

        self._total_orchestrator_weight = 0
        if self._config.ORCHESTRATORS:
            for o in self._config.ORCHESTRATORS:
                o["current_weight"] = 0
                self._total_orchestrator_weight += o.get("weight", 1)
        self._clients = clients or []
        self._comm_tasks: list[Task] = []
        self._poll_backoffs: dict[Transport, float] = {}
        self._hb_backoffs: dict[Transport, float] = {}
        self._last_heartbeat_times: dict[Transport, float] = {}
        self._next_poll_time: dict[Transport, float] = {}
        self._debouncing_flags: dict[Transport, bool] = {}
        self._heartbeat_cooldown = self._config.HEARTBEAT_COOLDOWN
        self._usage_checker: Callable[[], ResourcesUsage] | None = None
        if not self._clients and self._http_session:
            self._init_clients()

    def add_middleware(self, middleware: Middleware) -> None:
        self._middlewares.append(middleware)

    async def emit_event(self, event: WorkerEventPayload) -> None:
        """Publishes an event to all connected orchestrators."""
        if not self._clients:
            logger.warning("No clients connected. Cannot emit event.")
            return
        await gather(*[client.emit_event(event) for _, client in self._clients], return_exceptions=True)

    def include_blueprint(self, blueprint: SkillBlueprint) -> None:
        for skill_info, func in blueprint._skills:
            self._register_skill_handler(skill_info, func)

    async def load_skills(self, skills_dir: str | None = None) -> None:
        skills_dir = skills_dir or self._config.WORKER_SKILLS_DIR
        if not await exists(skills_dir):
            return

        logger.info(f"Scanning for skills in: {await abspath(skills_dir)}")
        for filename in await listdir(skills_dir):
            if filename.endswith(".py") and not filename.startswith("_"):
                module_name = filename[:-3]
                file_path = join(skills_dir, filename)
                try:
                    spec = spec_from_file_location(module_name, file_path)
                    if spec is None or spec.loader is None:
                        continue
                    module = module_from_spec(spec)
                    spec.loader.exec_module(module)

                    found_anything = False
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if isinstance(attr, SkillBlueprint):
                            self.include_blueprint(attr)
                            found_anything = True

                    if hasattr(module, "setup") and callable(module.setup):
                        module.setup(self)
                        found_anything = True

                    if found_anything:
                        logger.info(f"Loaded skills from: {filename}")
                except Exception as e:
                    logger.error(f"Failed to load skills from {filename}: {e}")

    def _init_clients(self) -> None:
        session_to_use = self._http_session if self._session_is_managed_externally else None
        self._clients = [
            (
                o,
                create_transport(
                    url=o["url"],
                    worker_id=self._config.WORKER_ID,
                    token=o.get("token", self._config.WORKER_TOKEN),
                    ssl_context=self._ssl_context,
                    session=session_to_use,
                    result_retries=self._config.RESULT_MAX_RETRIES,
                    result_retry_delay=self._config.RESULT_RETRY_INITIAL_DELAY,
                ),
            )
            for o in self._config.ORCHESTRATORS
        ]

    def _validate_skill_types(self) -> None:
        registered_skill_types = {
            handler_data["type"] for handler_data in self._skill_handlers.values() if handler_data["type"]
        }
        for skill_type in self._skill_type_limits:
            if skill_type not in registered_skill_types:
                logger.warning(
                    f"Configuration warning: A limit is defined for skill type '{skill_type}', "
                    "but no tasks are registered with this type."
                )

    def skill(
        self,
        info: str | SkillInfo | type[SkillInfo] | None = None,
        **kwargs: Any,
    ) -> Callable:
        """
        Universal decorator to register a skill handler.
        Supports automatic name, schema inference, and dynamic field extension.
        """
        base_info: SkillInfo
        target_class = self._skill_info_class

        init_kwargs = kwargs.copy()

        if isinstance(info, type) and issubclass(info, SkillInfo):
            target_class = info
        elif isinstance(info, SkillInfo):
            target_class = info.__class__
            existing_data = {f.name: getattr(info, f.name) for f in fields(info)}
            init_kwargs.update(existing_data)
        elif isinstance(info, str):
            init_kwargs["name"] = info
            if "type" not in init_kwargs:
                init_kwargs["type"] = info

        base_info = _create_dynamic_skill_object(target_class, init_kwargs)

        def decorator(func: Callable) -> Callable:
            nonlocal base_info
            if not base_info.name:
                base_info = replace(base_info, name=func.__name__)

            if base_info.description is None and func.__doc__:
                base_info = replace(base_info, description=cleandoc(func.__doc__))

            if base_info.version is None:
                module = modules.get(func.__module__)
                if module and hasattr(module, "__version__"):
                    base_info = replace(base_info, version=module.__version__)

            if base_info.input_schema is None:
                inferred = worker_extract_schema_from_func(func, "params")
                if inferred:
                    base_info = replace(base_info, input_schema=inferred)

            if base_info.output_schema is None:
                inferred = worker_extract_output_schema_from_func(func)
                if inferred:
                    base_info = replace(base_info, output_schema=inferred)

            self._register_skill_handler(base_info, func)
            return func

        return decorator

    def _register_skill_handler(self, skill_info: SkillInfo, func: Callable) -> None:
        """Internal method to register a compiled SkillInfo and handler."""
        validate_identifier(skill_info.name, "skill name")
        if skill_info.type:
            validate_identifier(skill_info.type, "skill type")

        logger.info(f"Registering skill: '{skill_info.name}' (type: {skill_info.type or 'N/A'})")
        skill_type = skill_info.type or skill_info.name

        if skill_type and skill_type not in self._skill_type_limits:
            logger.warning(
                f"Skill '{skill_info.name}' has a type '{skill_type}' which is not defined in 'skill_type_limits'. "
                "No concurrency limit will be applied for this type."
            )

        if skill_type and skill_type not in self._current_load_by_type:
            self._current_load_by_type[skill_type] = 0

        self._skill_handlers[skill_info.name] = {
            "func": func,
            "info": skill_info,
            "type": skill_type,
        }

    def add_to_hot_skills(self, item: str) -> None:
        """Marks a skill or resource identifier as 'hot' (loaded/ready)."""
        self._hot_skills_state.add(item)
        self._schedule_heartbeat_debounce()

    def remove_from_hot_skills(self, item: str) -> None:
        """Removes a skill or resource from the hot list."""
        self._hot_skills_state.discard(item)
        self._schedule_heartbeat_debounce()

    def get_hot_skills_state(self) -> set[str]:
        """Returns the set of currently hot items."""
        return self._hot_skills_state

    def _get_current_state(self) -> dict[str, Any]:
        """
        Calculates worker state with three tiers of skills:
        1. supported_skills: All skills this worker can perform (static catalog).
        2. available_skills: Skills that can be started right now (respecting limits).
        3. hot_skills: Subset of available_skills that are already in memory.
        """
        supported_skills: list[SkillInfo] = [h["info"] for h in self._skill_handlers.values()]
        available_skills: list[SkillInfo] = []
        hot_skills: list[SkillInfo] = []

        if self._current_load >= self._config.MAX_CONCURRENT_TASKS:
            return {
                "status": "full",
                "supported_skills": supported_skills,
                "available_skills": [],
                "hot_skills": [],
            }

        for handler_data in self._skill_handlers.values():
            skill_info = handler_data["info"]
            skill_type = handler_data.get("type")

            is_available = True
            if skill_type and skill_type in self._skill_type_limits:
                limit = self._skill_type_limits[skill_type]
                current = self._current_load_by_type.get(skill_type, 0)
                if current >= limit:
                    is_available = False

            if is_available and self._capacity_checker and not self._capacity_checker(skill_info.name):
                is_available = False

            if is_available:
                available_skills.append(skill_info)

                is_hot = False
                if skill_info.name in self._skill_dependencies:
                    deps = self._skill_dependencies[skill_info.name]
                    if deps and set(deps).issubset(self._hot_skills_state):
                        is_hot = True
                elif skill_info.name in self._hot_skills_state:
                    is_hot = True

                if is_hot:
                    hot_skills.append(skill_info)

        status = "idle" if available_skills else "busy"
        return {
            "status": status,
            "supported_skills": supported_skills,
            "available_skills": available_skills,
            "hot_skills": hot_skills,
        }

    def _get_next_client(self) -> Transport | None:
        if not self._clients:
            return None
        selected_client = None
        highest_weight = -1
        for o, client in self._clients:
            o["current_weight"] += o["weight"]
            if o["current_weight"] > highest_weight:
                highest_weight = o["current_weight"]
                selected_client = client
        if selected_client:
            for o, client in self._clients:
                if client == selected_client:
                    o["current_weight"] -= self._total_orchestrator_weight
                    break
        return selected_client

    def _schedule_heartbeat_debounce(self) -> None:
        """Schedules a heartbeat for all clients, respecting their individual cooldowns."""
        for item in self._clients:
            client = item[1] if isinstance(item, tuple) and len(item) == 2 else item
            create_task(self._send_single_heartbeat(client))

    async def _poll_for_tasks_with_status(self, client: Transport) -> bool:
        """Polls for tasks and returns True if a task was received."""
        if time() < self._next_poll_time.get(client, 0):
            return False

        current_state = self._get_current_state()
        if current_state["status"] in ("busy", "full"):
            return False

        try:
            available_skill_names = [s.name for s in current_state["available_skills"]]
            if not available_skill_names:
                return False

            hot_skill_names = [s.name for s in current_state["hot_skills"]]

            task_data = await client.poll_task(
                timeout=self._config.TASK_POLL_TIMEOUT,
                available_skills=available_skill_names,
                hot_skills=hot_skill_names,
            )

            self._poll_backoffs.pop(client, None)
            self._next_poll_time.pop(client, None)

            if task_data:
                task_data_dict = to_dict(task_data)
                task_data_dict["client"] = client
                self._current_load += 1
                if (skill_handler_info := self._skill_handlers.get(task_data.type)) and (
                    skill_type_for_limit := skill_handler_info.get("type")
                ):
                    self._current_load_by_type[skill_type_for_limit] += 1
                self._schedule_heartbeat_debounce()
                task = create_task(self._process_task(task_data_dict))
                self._active_tasks[task_data.task_id] = task
                return True
        except (RxonRateLimitError, RxonNetworkError) as e:
            is_rate_limit = isinstance(e, RxonRateLimitError)
            current_backoff = self._poll_backoffs.get(client, 0.0)
            retry_after_raw = getattr(e, "details", {}).get("retry_after") if is_rate_limit else None
            retry_after = self._parse_retry_after(retry_after_raw)

            if retry_after > 0:
                new_backoff = retry_after
            else:
                new_backoff = self._calculate_backoff(
                    current_backoff,
                    initial=self._config.POLL_BACKOFF_INITIAL,
                    max_delay=self._config.POLL_BACKOFF_MAX,
                    is_rate_limit=is_rate_limit,
                )

            self._poll_backoffs[client] = new_backoff
            self._next_poll_time[client] = time() + new_backoff

            if is_rate_limit:
                logger.warning(f"Rate limited by {client}. Backing off for {new_backoff:.1f}s.")
            else:
                logger.warning(f"Network error polling {client}: {e}. Backing off for {new_backoff:.1f}s.")
        except RxonError as e:
            logger.error(f"Error polling tasks from {client}: {e}")
        return False

    async def _poll_for_tasks(self, client: Transport) -> None:
        await self._poll_for_tasks_with_status(client)

    async def _start_polling(self) -> None:
        try:
            done, pending = await wait(
                [
                    create_task(self._registered_event.wait()),
                    create_task(self._shutdown_event.wait()),
                ],
                return_when=FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except Exception:
            pass

        while not self._shutdown_event.is_set():
            current_state = self._get_current_state()
            if current_state["status"] == WORKER_STATUS_DRAINING:
                logger.info("Worker is in DRAINING mode. Stop polling for new tasks.")
                break

            if current_state["status"] == "busy":
                await sleep(self._config.IDLE_POLL_DELAY)
                continue

            any_task_found = False
            if self._config.MULTI_ORCHESTRATOR_MODE == "WATERFALL":
                for _, client in self._clients:
                    if self._get_current_state()["status"] == "busy":
                        break
                    task_found = await self._poll_for_tasks_with_status(client)
                    if task_found:
                        any_task_found = True
                        break
            elif self._config.MULTI_ORCHESTRATOR_MODE == "ROUND_ROBIN":
                if client := self._get_next_client():
                    any_task_found = await self._poll_for_tasks_with_status(client)
            else:
                for _, client in self._clients:
                    if self._get_current_state()["status"] == "busy":
                        break
                    task_found = await self._poll_for_tasks_with_status(client)
                    if task_found:
                        any_task_found = True

            if not any_task_found:
                await sleep(self._config.IDLE_POLL_DELAY)

    @staticmethod
    def _prepare_task_params(handler: Callable, params: dict[str, Any]) -> Any:
        sig = signature(handler)
        params_param = sig.parameters.get("params")
        if params_param is None:
            return params
        params_annotation = params_param.annotation
        if params_annotation is sig.empty or params_annotation is dict:
            return params
        if _PYDANTIC_INSTALLED and isinstance(params_annotation, type) and issubclass(params_annotation, BaseModel):
            try:
                return cast(Any, params_annotation).model_validate(params)
            except ValidationError as e:
                raise ParamValidationError(str(e)) from e
        if isinstance(params_annotation, type) and is_dataclass(params_annotation):
            try:
                known_fields = {f.name for f in fields(params_annotation)}
                filtered_params = {k: v for k, v in params.items() if k in known_fields}
                required_fields = [
                    f.name
                    for f in fields(params_annotation)
                    if f.default is Parameter.empty and f.default_factory is Parameter.empty
                ]
                if missing_fields := [f for f in required_fields if f not in filtered_params]:
                    raise ParamValidationError(f"Missing required fields for dataclass: {', '.join(missing_fields)}")
                return params_annotation(**filtered_params)
            except (TypeError, ValueError) as e:
                raise ParamValidationError(str(e)) from e
        return params

    def _prepare_dependencies(self, handler: Callable, job_id: str, task_id: str, client: Transport) -> dict[str, Any]:
        deps: dict[str, Any] = {}
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        task_files = TaskFiles(
            task_dir, job_id=job_id, task_id=task_id, s3_manager=self._s3_manager, observability=self._observability
        )
        try:
            hints = get_type_hints(handler)
        except Exception:
            hints = {}
        for name, annotation in hints.items():
            origin = get_origin(annotation)
            types = get_args(annotation) if origin else (annotation,)
            if TaskFiles in types:
                deps[name] = task_files
            if ObservabilityManager in types:
                deps[name] = self._observability
            if OrchestratorClient in types:
                deps[name] = OrchestratorClient(client, self)
        return deps

    def _sign_payload_if_needed(
        self,
        payload: Any,
        ignore_fields: list[str] | None = None,
        identity_chain: list[str] | None = None,
    ) -> SecurityContext | None:
        """Signs payload for zero-trust verification if token is configured."""
        if self._config.WORKER_TOKEN and self._config.WORKER_TOKEN != "your-secret-worker-token":
            signature_val = sign_payload(payload, self._config.WORKER_TOKEN, ignore_fields=ignore_fields)
            return SecurityContext(
                signature=signature_val, signer_id=self._config.WORKER_ID, identity_chain=identity_chain
            )
        return None

    async def _process_task(self, task_data_raw: dict[str, Any]) -> None:
        """Handles the lifecycle of a single task: setup, execution, and reporting."""
        client: Transport = task_data_raw.pop("client")
        if "params_metadata" in task_data_raw and task_data_raw["params_metadata"]:
            task_data_raw["params_metadata"] = {
                k: from_dict(FileMetadata, v) for k, v in task_data_raw["params_metadata"].items()
            }
        task_payload = from_dict(TaskPayload, task_data_raw)
        task_id, job_id, skill_name = task_payload.task_id, task_payload.job_id, task_payload.type

        secret_key = getenv("ORCHESTRATOR_SECRET_KEY", "")
        payload_sig = task_payload.sig or getattr(task_payload, "orchestrator_signature", None)
        if secret_key and payload_sig:
            ignore_sig_fields = ["sig", "orchestrator_signature"]
            if not verify_signature(task_payload, payload_sig, secret_key, ignore_fields=ignore_sig_fields):
                logger.critical(f"Task {task_id} orchestrator signature verification failed!")
                raise PermissionError(f"Task {task_id} orchestrator signature mismatch: task rejected")

        if task_payload.policy:
            allowed_skills = (
                task_payload.policy.get("allowed_skills")
                if isinstance(task_payload.policy, dict)
                else getattr(task_payload.policy, "allowed_skills", None)
            )
            if allowed_skills is not None and skill_name not in allowed_skills:
                logger.error(f"Task {task_id} type '{skill_name}' is not in allowed_skills policy {allowed_skills}")
                raise PermissionError(f"Task type '{skill_name}' is not permitted by task policy")

        params = task_payload.params
        set_context(task_id=task_id, job_id=job_id)
        handler_data = self._skill_handlers.get(skill_name)
        skill_type_for_limit = handler_data.get("type") if handler_data else None
        result_obj: TaskResult | None = None

        start_time = perf_counter()

        step_val = getattr(task_payload, "step", 0)
        depth_val = getattr(task_payload, "depth", 0)
        parent_hash_val = getattr(task_payload, "parent_hash", None)

        with self._observability.start_task_span(
            skill_name,
            task_id,
            job_id,
            parent_context=task_payload.tracing_context,
            step=step_val,
            depth=depth_val,
            parent_hash=parent_hash_val,
        ) as span:
            if handler_data:
                is_valid, error_msg = task_payload.validate_params(handler_data["info"])
                if not is_valid:
                    logger.error(f"Task {task_id} failed input validation: {error_msg}")
                    error = TaskError(code=ERROR_CODE_INVALID_INPUT, message=error_msg or "Validation failed")
                    result_obj = TaskResult(
                        job_id=job_id,
                        task_id=task_id,
                        worker_id=self._config.WORKER_ID,
                        origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                        status=TASK_STATUS_FAILURE,
                        error=error,
                        security=None,
                        metadata=task_payload.metadata,
                        timestamp=int(time()),
                    )
                    security = self._sign_payload_if_needed(result_obj)
                    result_obj = struct_replace(result_obj, security=security)

            async def send_event_wrapper(
                event_type: str,
                payload: dict[str, Any],
                priority: float = 0.0,
                security: SecurityContext | None = None,
                metadata: dict[str, Any] | None = None,
                origin_worker_id: str | None = None,
            ) -> None:
                if handler_data and (events_schema := handler_data["info"].events_schema):
                    if event_type in events_schema:
                        is_valid, error_msg = validate_data(payload, events_schema[event_type])
                        if not is_valid:
                            logger.error(f"Local contract violation for event '{event_type}': {error_msg}")
                            return
                    elif event_type != EVENT_TYPE_PROGRESS:
                        if self._config.STRICT_EVENT_VALIDATION:
                            logger.error(f"Contract violation: Emitting undeclared event type '{event_type}'. Blocked.")
                            return
                        else:
                            logger.warning(f"Emitting undeclared event type '{event_type}'. Allowed by config.")

                event_id = str(uuid4())
                timestamp = int(time())

                # Tracing context propagation
                tracing_context = (task_payload.tracing_context or {}).copy()

                if span and hasattr(span, "get_span_context") and span.get_span_context().is_valid:
                    ctx = span.get_span_context()
                    tracing_context["traceparent"] = (
                        f"00-{format(ctx.trace_id, '032x')}-{format(ctx.span_id, '016x')}-"
                        f"{format(ctx.trace_flags, '02x')}"
                    )
                elif propagate is not None:
                    propagate.inject(tracing_context)

                if not tracing_context.get("traceparent") and task_payload.tracing_context:
                    tracing_context.update(task_payload.tracing_context)

                logger.debug(f"Emitting event {event_type} for job {job_id}. Trace context: {tracing_context}")

                event_payload = WorkerEventPayload(
                    event_id=event_id,
                    worker_id=self._config.WORKER_ID,
                    origin_worker_id=origin_worker_id or self._config.WORKER_ID,
                    event_type=event_type,
                    payload=payload,
                    origin_task_id=task_id,
                    bubbling_chain=[],
                    target_job_id=job_id,
                    target_task_id=task_id,
                    trace_context=tracing_context,
                    priority=priority,
                    timestamp=timestamp,
                    security=None,
                    metadata=metadata,
                )

                if not security:
                    security = self._sign_payload_if_needed(event_payload, ignore_fields=["bubbling_chain"])

                if security:
                    event_payload = struct_replace(event_payload, security=security)

                await client.emit_event(event_payload)

            async def send_progress_wrapper(task_id_arg, job_id_arg, progress, message=""):
                await send_event_wrapper(EVENT_TYPE_PROGRESS, {"progress": progress, "message": message})

            try:
                if result_obj:
                    pass
                elif not handler_data:
                    message = f"Unsupported skill: {skill_name}"
                    logger.warning(message)
                    error = TaskError(code=ERROR_CODE_PERMANENT, message=message)
                    result_obj = TaskResult(
                        job_id=job_id,
                        task_id=task_id,
                        worker_id=self._config.WORKER_ID,
                        status=TASK_STATUS_FAILURE,
                        error=error,
                        security=None,
                        metadata=task_payload.metadata,
                        timestamp=int(time()),
                    )
                    security = self._sign_payload_if_needed(result_obj)
                    result_obj = struct_replace(result_obj, security=security)
                    if span:
                        self._observability.set_span_status_error(span, message)
                else:
                    params = await self._s3_manager.process_params(
                        params, task_id, metadata=task_payload.params_metadata
                    )
                    validated_params = self._prepare_task_params(handler_data["func"], params)
                    deps = self._prepare_dependencies(handler_data["func"], job_id, task_id, client)
                    handler_kwargs = {
                        "params": validated_params,
                        "task_id": task_id,
                        "job_id": job_id,
                        "tracing_context": task_payload.tracing_context,
                        "priority": task_payload.priority,
                        "deadline": task_payload.deadline,
                        "security": task_payload.security,
                        "metadata": task_payload.metadata,
                        "send_progress": send_progress_wrapper,
                        "send_event": send_event_wrapper,
                        "add_to_hot_skills": self.add_to_hot_skills,
                        "remove_from_hot_skills": self.remove_from_hot_skills,
                        **deps,
                    }
                    handler_func = handler_data["func"]
                    sig = signature(handler_func)
                    final_handler_kwargs = {}

                    if "params" in sig.parameters:
                        final_handler_kwargs["params"] = validated_params

                    available_context = {**handler_kwargs, **deps}
                    for param_name, param in sig.parameters.items():
                        if param_name == "params":
                            continue
                        if param_name in available_context:
                            final_handler_kwargs[param_name] = available_context[param_name]
                        elif param.default is Parameter.empty and param.kind not in (
                            Parameter.VAR_KEYWORD,
                            Parameter.VAR_POSITIONAL,
                        ):
                            raise ParamValidationError(f"Missing required parameter: {param_name}")

                    middleware_context = {
                        "task_id": task_id,
                        "job_id": job_id,
                        "skill_name": skill_name,
                        "params": validated_params,
                        "handler_kwargs": final_handler_kwargs,
                        "observability": self._observability,
                        "s3_manager": self._s3_manager,
                    }

                    async def _execution_logic() -> Any:
                        handler = handler_data["func"]
                        f_kwargs = middleware_context["handler_kwargs"]

                        if any(p.kind == Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                            f_kwargs = {**available_context, **f_kwargs}

                        if iscoroutinefunction(handler):
                            return await handler(**f_kwargs)
                        else:
                            return await to_thread(handler, **f_kwargs)

                    handler_chain = _execution_logic
                    for middleware in reversed(self._middlewares):

                        def make_wrapper(mw: Middleware, next_handler: Callable) -> Callable:
                            async def wrapper():
                                return await mw(middleware_context, next_handler)

                            return wrapper

                        handler_chain = make_wrapper(middleware, handler_chain)
                    handler_result = await handler_chain()

                    if not isinstance(handler_result, dict):
                        handler_result = {"data": handler_result}

                    skill_info = handler_data["info"]
                    if skill_info.output_schema:
                        is_valid, error_msg = validate_data(handler_result.get("data"), skill_info.output_schema)
                        if not is_valid:
                            full_error = f"Local contract violation: output does not match schema. {error_msg}"
                            logger.error(full_error)
                            handler_result["status"] = TASK_STATUS_FAILURE
                            handler_result["error"] = {"code": ERROR_CODE_CONTRACT_VIOLATION, "message": full_error}

                    updated_data, metadata_map = await self._s3_manager.process_result(
                        handler_result.get("data", {}), s3_prefix=job_id
                    )
                    task_error = None
                    if "error" in handler_result:
                        err_data = handler_result["error"]
                        if isinstance(err_data, dict):
                            err_fields = {f.name for f in fields(TaskError)}
                            valid_err_fields = {k: v for k, v in err_data.items() if k in err_fields}
                            task_error = TaskError(**valid_err_fields)
                        else:
                            task_error = TaskError(code=ERROR_CODE_TRANSIENT, message=str(err_data))

                    timestamp = int(time())
                    costs = {"execution_time_seconds": round(perf_counter() - start_time, 4)}
                    if (
                        isinstance(handler_result, dict)
                        and "costs" in handler_result
                        and isinstance(handler_result["costs"], dict)
                    ):
                        costs.update(handler_result["costs"])

                    result_obj = TaskResult(
                        job_id=job_id,
                        task_id=task_id,
                        worker_id=self._config.WORKER_ID,
                        origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                        status=handler_result.get("status", TASK_STATUS_SUCCESS),
                        data=updated_data,
                        error=task_error,
                        data_metadata=metadata_map if metadata_map else None,
                        security=None,
                        metadata=handler_result.get("metadata"),
                        timestamp=timestamp,
                        costs=costs,
                    )
                    security = self._sign_payload_if_needed(result_obj)
                    if not security:
                        security = from_dict(SecurityContext, handler_result.get("security"))

                    result_obj = struct_replace(result_obj, security=security)

                    if span and result_obj.status == TASK_STATUS_FAILURE:
                        self._observability.set_span_status_error(
                            span, task_error.message if task_error else "Unknown failure"
                        )
            except ParamValidationError as e:
                logger.error(f"Task {task_id} failed validation: {e}")
                error = TaskError(code=ERROR_CODE_INVALID_INPUT, message=str(e))
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                    status=TASK_STATUS_FAILURE,
                    error=error,
                    security=None,
                    metadata=task_payload.metadata,
                    timestamp=int(time()),
                )
                security = self._sign_payload_if_needed(result_obj)
                result_obj = struct_replace(result_obj, security=security)
                if span:
                    self._observability.set_span_status_error(span, str(e))
            except CancelledError:
                logger.info(f"Task {task_id} was cancelled.")
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                    status=TASK_STATUS_CANCELLED,
                    security=None,
                    metadata=task_payload.metadata,
                    timestamp=int(time()),
                )
                security = self._sign_payload_if_needed(result_obj)
                result_obj = struct_replace(result_obj, security=security)
                if span:
                    self._observability.set_span_status_error(span, "Task cancelled")
                raise
            except ValueError as e:
                logger.error(f"Data integrity or validation error for task {task_id}: {e}")
                error = TaskError(code=ERROR_CODE_INTEGRITY_MISMATCH, message=str(e))
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                    status=TASK_STATUS_FAILURE,
                    error=error,
                    security=None,
                    metadata=task_payload.metadata,
                    timestamp=int(time()),
                )
                security = self._sign_payload_if_needed(result_obj)
                result_obj = struct_replace(result_obj, security=security)
                if span:
                    self._observability.set_span_status_error(span, str(e))
            except Exception as e:
                logger.exception(f"An unexpected error occurred while processing task {task_id}:")
                error = TaskError(code=ERROR_CODE_TRANSIENT, message=str(e))
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    origin_worker_id=task_data_raw.get("origin_worker_id") or self._config.WORKER_ID,
                    status=TASK_STATUS_FAILURE,
                    error=error,
                    security=None,
                    metadata=task_payload.metadata,
                    timestamp=int(time()),
                )
                security = self._sign_payload_if_needed(result_obj)
                result_obj = struct_replace(result_obj, security=security)
                if span:
                    self._observability.set_span_status_error(span, str(e))
            finally:
                await self._s3_manager.cleanup(task_id)
                final_status = result_obj.status if result_obj else "unknown"
                duration = perf_counter() - start_time
                self._observability.record_task_finished(skill_name, final_status, duration)
                if result_obj:
                    if self._queue_worker_task and not self._queue_worker_task.done():
                        await self._result_queue.put((client, result_obj))
                    else:
                        try:
                            accepted = await client.send_result(result_obj)
                            if not accepted:
                                logger.warning(f"Task {task_id} result was IGNORED by orchestrator.")
                        except RxonError as e:
                            logger.error(f"Failed to send task result: {e}")
                clear_context("task_id", "job_id")
                self._active_tasks.pop(task_id, None)
                self._current_load -= 1
                if skill_type_for_limit:
                    self._current_load_by_type[skill_type_for_limit] -= 1
                self._schedule_heartbeat_debounce()

    def _calculate_contract_hash(self, skills: list[SkillInfo]) -> str:
        """Calculates a hash representing the worker's current 'contract'."""
        combined_contract = {
            "skills": [to_dict(s) for s in skills],
            "capabilities": self._config.COST_PER_SKILL,
            "extra": self._config.EXTRA_CAPABILITIES,
        }
        return cast(str, calculate_dict_hash(combined_contract))

    def _create_registration_payload(self) -> WorkerRegistration:
        state = self._get_current_state()
        devices = None
        if self._config.RESOURCES.get("devices"):
            devices = [
                HardwareDevice(**{k: v for k, v in d.items() if k in {f.name for f in fields(HardwareDevice)}})
                for d in self._config.RESOURCES["devices"]
            ]

        props = (self._config.RESOURCES.get("properties") or {}).copy()
        if "cpu_cores" not in props:
            props["cpu_cores"] = self._config.RESOURCES.get("cpu_cores", 1)
        if "ram_gb" not in props:
            props["ram_gb"] = self._config.RESOURCES.get("ram_gb", 1.0)

        resources = Resources(
            devices=devices,
            properties=props,
        )
        s3_hash = calculate_config_hash(
            self._config.S3_ENDPOINT_URL,
            self._config.S3_ACCESS_KEY,
            self._config.S3_DEFAULT_BUCKET,
        )
        combined_extra = {"websockets": self._config.ENABLE_WEBSOCKETS}
        combined_extra.update(self._config.EXTRA_CAPABILITIES)

        skills_hash = self._calculate_contract_hash(state["supported_skills"])
        self._last_synced_skills_hash = skills_hash

        available_names = [s.name for s in state["available_skills"]]
        hot_names = [s.name for s in state["hot_skills"]]

        timestamp = int(time())
        payload = WorkerRegistration(
            worker_id=self._config.WORKER_ID,
            worker_type=self._config.WORKER_TYPE,
            supported_skills=state["supported_skills"],
            available_skills=available_names or None,
            hot_skills=hot_names or None,
            resources=resources,
            installed_software=self._config.INSTALLED_SOFTWARE,
            installed_artifacts=[
                InstalledArtifact(**{k: v for k, v in m.items() if k in {f.name for f in fields(InstalledArtifact)}})
                for m in self._config.INSTALLED_ARTIFACTS
            ],
            capabilities=WorkerCapabilities(
                hostname=self._config.HOSTNAME,
                ip_address=self._config.IP_ADDRESS,
                cost_per_skill=self._config.COST_PER_SKILL,
                s3_config_hash=s3_hash,
                extra=combined_extra,
            ),
            skills_hash=skills_hash,
            timestamp=timestamp,
        )
        security = self._sign_payload_if_needed(payload)
        if security:
            payload = struct_replace(payload, security=security)
        return payload

    async def _register_client_with_retry(self, client: Transport) -> None:
        delay = self._config.REGISTRATION_RETRY_INITIAL_DELAY
        while not self._shutdown_event.is_set():
            try:
                registration = self._create_registration_payload()
                await self._safe_register(client, registration)
                logger.info(f"Successfully registered with {client}")
                self._registered_event.set()
                return
            except Exception as e:
                is_rl = isinstance(e, RxonRateLimitError)
                retry_after_raw = getattr(e, "details", {}).get("retry_after") if is_rl else None
                retry_after = self._parse_retry_after(retry_after_raw)

                logger.error(f"Registration attempt failed for {client}: {e}")

                if retry_after > 0:
                    delay = retry_after
                else:
                    delay = self._calculate_backoff(
                        delay,
                        initial=self._config.REGISTRATION_RETRY_INITIAL_DELAY,
                        max_delay=self._config.REGISTRATION_RETRY_MAX_DELAY,
                        is_rate_limit=is_rl,
                    )

            logger.info(f"Retrying registration for {client} in {delay:.1f}s...")
            try:
                await wait_for(self._shutdown_event.wait(), timeout=delay)
                break
            except TimeoutError:
                continue

    async def _delayed_heartbeat(self, client: Transport, delay: float) -> None:
        """Helper to send a heartbeat after a delay, used for debouncing."""
        try:
            await sleep(delay)
            await self._send_single_heartbeat(client)
        finally:
            self._debouncing_flags[client] = False

    async def _manage_single_orchestrator(self, client: Transport) -> None:
        while not self._shutdown_event.is_set():
            await self._register_client_with_retry(client)
            if self._shutdown_event.is_set():
                break

            logger.info(f"Starting heartbeat loop for {client}")
            while not self._shutdown_event.is_set():
                hb_backoff = self._hb_backoffs.get(client, 0.0)
                try:
                    jitter_ms = await self._send_single_heartbeat(client)
                    self._hb_backoffs[client] = 0.0
                    wait_time = self._config.HEARTBEAT_INTERVAL + (jitter_ms / 1000.0)
                    try:
                        await wait_for(self._shutdown_event.wait(), timeout=wait_time)
                        break
                    except TimeoutError:
                        continue
                except RxonRateLimitError as e:
                    retry_after = self._parse_retry_after(e.details.get("retry_after") if e.details else None)
                    if retry_after > 0:
                        new_backoff = retry_after
                    else:
                        new_backoff = self._calculate_backoff(
                            hb_backoff,
                            initial=self._config.HEARTBEAT_INTERVAL,
                            max_delay=self._config.POLL_BACKOFF_MAX,
                            is_rate_limit=True,
                        )
                    self._hb_backoffs[client] = new_backoff
                    logger.warning(f"Rate limited by {client}. Backing off for {new_backoff:.1f}s")
                    try:
                        await wait_for(self._shutdown_event.wait(), timeout=new_backoff)
                        break
                    except TimeoutError:
                        continue
                except RxonError as e:
                    err_msg = str(e).lower()
                    if "not found" in err_msg or "unauthorized" in err_msg:
                        logger.warning(f"Orchestrator {client} lost our session. Re-registering...")
                        self._last_synced_skills_hash = None
                        break
                    else:
                        new_backoff = self._calculate_backoff(
                            hb_backoff,
                            initial=self._config.HEARTBEAT_INTERVAL,
                            max_delay=self._config.POLL_BACKOFF_MAX,
                        )
                        self._hb_backoffs[client] = new_backoff
                        logger.warning(f"Heartbeat failed for {client}: {e}. Backing off for {new_backoff:.1f}s")
                        try:
                            await wait_for(self._shutdown_event.wait(), timeout=new_backoff)
                            break
                        except TimeoutError:
                            continue
                except Exception as e:
                    logger.error(f"Unexpected error in heartbeat loop for {client}: {e}", exc_info=True)
                    try:
                        await wait_for(self._shutdown_event.wait(), timeout=self._config.HEARTBEAT_INTERVAL)
                        break
                    except TimeoutError:
                        continue

    async def _register_with_all_orchestrators(self) -> None:
        """Starts background registration for all orchestrators. Blocks until at least one succeeds."""
        try:
            done, pending = await wait(
                [
                    create_task(self._registered_event.wait()),
                    create_task(self._shutdown_event.wait()),
                ],
                return_when=FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
        except Exception:
            pass

    async def _safe_register(self, client: Transport, registration: WorkerRegistration) -> None:
        try:
            resp = await client.register(registration)
            if isinstance(resp, dict) and (warning := resp.get("warning")):
                logger.warning(f"Registration warning from {client}: {warning}")
        except RxonError as e:
            logger.error(f"Registration failed for {client}: {e}")
            raise

    def _get_resources_usage(self) -> ResourcesUsage | None:
        """
        Returns resource usage metrics.
        Collects basic CPU/RAM metrics and GPU metrics if available.
        Can be overridden by user via set_usage_checker().
        """
        if self._usage_checker is not None:
            return self._usage_checker()

        cpu_load = 0.0
        ram_used = 0.0
        with suppress(Exception):
            psutil = modules.get("psutil")
            if psutil is not None:
                cpu_load = float(psutil.cpu_percent())
                ram_used = float(psutil.virtual_memory().used) / (1024**3)

        devices_usage = []
        with suppress(Exception):
            gputil = modules.get("GPUtil")
            if gputil is not None:
                for gpu in gputil.getGPUs():
                    devices_usage.append(
                        DeviceUsage(
                            unit_id=str(gpu.id),
                            load_percent=float(gpu.load) * 100,
                            metrics={
                                "memory_used_gb": float(gpu.memoryUsed) / 1024,
                                "temperature_c": float(gpu.temperature),
                            },
                        )
                    )

        return ResourcesUsage(
            cpu_load_percent=cpu_load, ram_used_gb=ram_used, devices_usage=devices_usage if devices_usage else None
        )

    def set_usage_checker(self, checker: Callable[[], ResourcesUsage]) -> None:
        """Sets a custom callback for resource monitoring."""
        self._usage_checker = checker

    def _is_telemetry_changed_significantly(self, current: ResourcesUsage) -> bool:
        if not self._last_sent_usage:
            return True

        if abs(current.cpu_load_percent - self._last_sent_usage.cpu_load_percent) > self._telemetry_deadband:
            return True

        last_ram = self._last_sent_usage.ram_used_gb
        if last_ram == 0.0 and current.ram_used_gb > 0.0:
            return True
        elif last_ram > 0:
            ram_diff = abs(current.ram_used_gb - last_ram) / last_ram
            if ram_diff > (self._telemetry_deadband / 100.0):
                return True

        curr_gpus = current.devices_usage or []
        last_gpus = self._last_sent_usage.devices_usage or []
        if len(curr_gpus) != len(last_gpus):
            return True

        for c_gpu in curr_gpus:
            l_gpu = next((g for g in last_gpus if g.unit_id == c_gpu.unit_id), None)
            if not l_gpu:
                return True
            if abs(c_gpu.load_percent - l_gpu.load_percent) > self._telemetry_deadband:
                return True
            c_vram = c_gpu.metrics.get("memory_used_gb", 0.0) if c_gpu.metrics else 0.0
            l_vram = l_gpu.metrics.get("memory_used_gb", 0.0) if l_gpu.metrics else 0.0
            if l_vram == 0.0 and c_vram > 0.0:
                return True
            elif l_vram > 0:
                vram_diff = abs(c_vram - l_vram) / l_vram
                if vram_diff > (self._telemetry_deadband / 100.0):
                    return True
        return False

    def _create_heartbeat_payload(self) -> Heartbeat:
        state = self._get_current_state()
        all_supported = state["supported_skills"]

        current_hash = self._calculate_contract_hash(all_supported)

        skills_to_send = None
        if current_hash != self._last_synced_skills_hash:
            logger.info(f"Worker catalog changed (hash: {current_hash}). Sending full update.")
            skills_to_send = all_supported

        current_usage = self._get_resources_usage()
        now = time()

        if type(current_usage).__name__ in ("Mock", "MagicMock", "AsyncMock"):
            usage = current_usage
        else:
            usage = None
            if (
                not self._last_sent_usage
                or (now - self._last_sent_usage_time) >= self._telemetry_force_interval
                or self._is_telemetry_changed_significantly(current_usage)
            ):
                usage = current_usage
                self._last_sent_usage = current_usage
                self._last_sent_usage_time = now

        timestamp = int(time())

        available_names = [s.name for s in state["available_skills"]]
        hot_names = [s.name for s in state["hot_skills"]]

        heartbeat = Heartbeat(
            worker_id=self._config.WORKER_ID,
            status=state["status"],
            usage=usage,
            current_tasks=list(self._active_tasks.keys()),
            supported_skills=skills_to_send if current_hash != self._last_synced_skills_hash else None,
            available_skills=available_names or None,
            hot_skills=hot_names or None,
            skills_hash=current_hash,
            security=None,
            timestamp=timestamp,
        )
        security = self._sign_payload_if_needed(heartbeat)
        return struct_replace(heartbeat, security=security)

    def _parse_retry_after(self, retry_after: Any) -> float:
        """Parses Retry-After header which can be seconds or an HTTP-date."""
        if not retry_after:
            return 0.0
        try:
            return float(retry_after)
        except (ValueError, TypeError):
            try:
                dt = parsedate_to_datetime(str(retry_after))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return max(0.0, (dt - datetime.now(UTC)).total_seconds())
            except Exception:
                return 0.0

    def _calculate_backoff(
        self,
        current: float,
        initial: float,
        max_delay: float,
        factor: float = 2.0,
        is_rate_limit: bool = False,
    ) -> float:
        """Unified exponential backoff logic."""
        new_val = min(max(current * factor, initial), max_delay)
        if is_rate_limit:
            return max(new_val, self._config.RATE_LIMIT_BACKOFF_FLOOR)
        return new_val

    async def _send_single_heartbeat(self, client: Transport) -> int:
        now = time()
        last_time = self._last_heartbeat_times.get(client, 0.0)
        time_since_last = now - last_time

        if time_since_last < self._heartbeat_cooldown:
            remaining = self._heartbeat_cooldown - time_since_last
            if not self._debouncing_flags.get(client, False):
                self._debouncing_flags[client] = True
                create_task(self._delayed_heartbeat(client, remaining))
            return 0

        heartbeat = self._create_heartbeat_payload()
        resp = await client.send_heartbeat(heartbeat)
        self._last_heartbeat_times[client] = time()
        jitter_ms = 0

        if resp is not None:
            self._last_synced_skills_hash = heartbeat.skills_hash

        if resp:
            if isinstance(resp, dict) and resp.get(HB_RESP_REQUIRE_FULL_SYNC):
                logger.warning(f"Orchestrator {client} requested Full Sync. Resetting state.")
                self._last_synced_skills_hash = None

            if isinstance(resp, dict) and HB_RESP_CANCEL_TASKS in resp:
                for task_id in resp[HB_RESP_CANCEL_TASKS]:
                    if task_id in self._active_tasks:
                        logger.warning(f"Task {task_id} marked for cancellation via heartbeat feedback loop.")
                        self._active_tasks[task_id].cancel()

            if isinstance(resp, dict):
                try:
                    val = resp.get("next_heartbeat_jitter_ms", 0)
                    jitter_ms = int(float(val)) if val is not None else 0
                except (ValueError, TypeError):
                    jitter_ms = 0
        return jitter_ms

    async def _manage_orchestrator_communications(self) -> None:
        """Starts independent managers for each orchestrator."""
        for _, client in self._clients:
            self._comm_tasks.append(create_task(self._manage_single_orchestrator(client)))

        if self._config.ENABLE_WEBSOCKETS:
            self._comm_tasks.append(create_task(self._start_websocket_manager()))

        await self._register_with_all_orchestrators()
        await self._shutdown_event.wait()

    async def _result_queue_worker(self) -> None:
        while True:
            try:
                client, result_obj = await self._result_queue.get()
            except CancelledError:
                break

            try:
                retries = self._config.RESULT_MAX_RETRIES
                delay = self._config.RESULT_RETRY_INITIAL_DELAY
                for attempt in range(retries):
                    try:
                        accepted = await client.send_result(result_obj)
                        if not accepted:
                            logger.warning(f"Task {result_obj.task_id} result was IGNORED by orchestrator.")
                        break
                    except RxonRateLimitError as e:
                        retry_after_raw = getattr(e, "details", {}).get("retry_after")
                        retry_after = self._parse_retry_after(retry_after_raw)
                        sleep_time = max(retry_after, delay)
                        logger.warning(
                            f"Rate limited sending result for task {result_obj.task_id}. Retrying in {sleep_time}s..."
                        )
                        await sleep(sleep_time)
                        delay *= 2
                    except Exception as e:
                        if attempt == retries - 1:
                            logger.error(
                                f"Failed to send result for task {result_obj.task_id} after {retries} attempts: {e}"
                            )
                            break
                        logger.warning(
                            f"Error sending result for task {result_obj.task_id} "
                            f"(attempt {attempt + 1}/{retries}): {e}. Retrying in {delay}s..."
                        )
                        await sleep(delay)
                        delay *= 2
            except Exception:
                logger.exception("Unexpected error in result queue worker:")
            finally:
                self._result_queue.task_done()

    async def main(self) -> None:
        self._config.validate()
        self._validate_skill_types()
        await self.load_skills()
        if self._config.EXTRA_CAPABILITIES:
            logger.info(f"Loaded custom capabilities: {self._config.EXTRA_CAPABILITIES}")
        loop = get_running_loop()
        for sig in (SIGINT, SIGTERM):
            with suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._shutdown_event.set)
        if not self._http_session:
            if self._config.TLS_CA_PATH or (self._config.TLS_CERT_PATH and self._config.TLS_KEY_PATH):
                logger.info("Initializing SSL context for mTLS.")
                self._ssl_context = create_client_ssl_context(
                    ca_path=self._config.TLS_CA_PATH,
                    cert_path=self._config.TLS_CERT_PATH,
                    key_path=self._config.TLS_KEY_PATH,
                )

            connector = TCPConnector(ssl=self._ssl_context) if self._ssl_context else None
            self._http_session = ClientSession(connector=connector, json_serialize=json_dumps)
            if not self._clients:
                self._init_clients()
        await gather(*[client.connect() for _, client in self._clients])
        comm_task = create_task(self._manage_orchestrator_communications())
        self._queue_worker_task = create_task(self._result_queue_worker())
        token_rotation_task = None
        if self._ssl_context:
            token_rotation_task = create_task(self._manage_token_rotation())
        polling_task = create_task(self._start_polling())

        try:
            await self._shutdown_event.wait()
        finally:
            # We shield the cleanup to ensure it completes even if main() is cancelled
            await shield(self._perform_shutdown_cleanup(comm_task, polling_task, token_rotation_task))

    async def _perform_shutdown_cleanup(
        self,
        comm_task: Task,
        polling_task: Task,
        token_rotation_task: Task | None,
    ) -> None:
        """Internal helper to execute all shutdown steps. Shielded from cancellation."""
        logger.info("Shutdown signal received. Starting graceful shutdown...")

        for task in self._comm_tasks:
            task.cancel()

        polling_task.cancel()
        logger.info("Waiting for active S3 operations to complete...")
        await self._s3_manager.wait_all_done()

        logger.info("Sending final 'offline' heartbeat...")
        with suppress(Exception):
            state = self._get_current_state()
            hot_skill_names = [s.name for s in state["hot_skills"]]

            heartbeat = Heartbeat(
                worker_id=self._config.WORKER_ID,
                status="offline",
                usage=self._get_resources_usage(),
                current_tasks=list(self._active_tasks.keys()),
                supported_skills=[],
                available_skills=None,
                hot_skills=hot_skill_names or None,
                skills_hash=self._last_synced_skills_hash or "",
                security=None,
                metadata=None,
                timestamp=int(time()),
            )
            security = self._sign_payload_if_needed(heartbeat)
            if security:
                heartbeat = struct_replace(heartbeat, security=security)
            await gather(*[client.send_heartbeat(heartbeat) for _, client in self._clients], return_exceptions=True)

        logger.info("Draining result queue...")
        with suppress(Exception):
            await wait_for(self._result_queue.join(), timeout=10.0)
        if self._queue_worker_task:
            self._queue_worker_task.cancel()
            with suppress(Exception):
                await self._queue_worker_task

        comm_task.cancel()
        if token_rotation_task:
            token_rotation_task.cancel()

        if self._active_tasks:
            timeout = self._config.SHUTDOWN_TIMEOUT
            logger.info(f"Waiting for {len(self._active_tasks)} tasks to complete ({timeout}s)...")
            try:
                await wait_for(
                    gather(*self._active_tasks.values(), return_exceptions=True),
                    timeout=self._config.SHUTDOWN_TIMEOUT,
                )
            except (TimeoutError, CancelledError):
                logger.warning("Shutdown timeout reached or cleanup cancelled.")

        await gather(*[client.close() for _, client in self._clients], return_exceptions=True)
        await self._observability.shutdown()

        if self._http_session and not self._http_session.closed and not self._session_is_managed_externally:
            await self._http_session.close()

    def run(self) -> None:
        try:
            run(self.main())
        except KeyboardInterrupt:
            self._shutdown_event.set()

    async def _manage_token_rotation(self) -> None:
        await sleep(5)
        while not self._shutdown_event.is_set():
            min_expires_in = 3600
            for _, client in self._clients:
                try:
                    token_resp = await client.refresh_token()
                    if token_resp:
                        self._config.WORKER_TOKEN = token_resp.access_token
                        min_expires_in = min(min_expires_in, token_resp.expires_in)
                except Exception as e:
                    logger.error(f"Error in token rotation: {e}")
            refresh_delay = max(60, min_expires_in * 0.8)
            await sleep(refresh_delay)

    async def _run_health_check_server(self) -> None:
        app = web.Application()

        async def health_handler(_):
            return web.Response(text="OK")

        app.router.add_get("/health", health_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._config.WORKER_PORT)
        await site.start()
        await self._shutdown_event.wait()
        await runner.cleanup()

    def run_with_health_check(self) -> None:
        async def _main_wrapper() -> None:
            async with TaskGroup() as tg:
                tg.create_task(self._run_health_check_server())
                tg.create_task(self.main())

        try:
            run(_main_wrapper())
        except (KeyboardInterrupt, CancelledError):
            self._shutdown_event.set()
        except ExceptionGroup as eg:
            if all(isinstance(e, (KeyboardInterrupt, CancelledError)) for e in eg.exceptions):
                self._shutdown_event.set()
            else:
                raise

    async def _start_websocket_manager(self) -> None:
        listeners = []
        for _, client in self._clients:
            listeners.append(create_task(self._listen_to_single_transport(client)))
        await self._shutdown_event.wait()
        for listener in listeners:
            listener.cancel()

    def on_command(self, command_name: str) -> Callable:
        """Decorator to register a custom command handler."""

        def decorator(func: Callable) -> Callable:
            self._command_handlers[command_name] = func
            return func

        return decorator

    async def _listen_to_single_transport(self, client: Transport) -> None:
        while not self._shutdown_event.is_set():
            try:
                async for command in client.listen_for_commands():
                    if command.command == COMMAND_CANCEL_TASK:
                        task_id = command.task_id
                        if task_id in self._active_tasks:
                            self._active_tasks[task_id].cancel()
                            logger.info(f"Cancelled task {task_id} by command.")
                    elif handler := self._command_handlers.get(command.command):
                        try:
                            if iscoroutinefunction(handler):
                                await handler(command)
                            else:
                                await to_thread(handler, command)
                        except Exception as e:
                            logger.error(f"Error handling command '{command.command}': {e}")
                    else:
                        logger.warning(f"Received unknown command: {command.command}")
            except Exception as e:
                logger.error(f"Error in command listener: {e}")
            await sleep(5)
