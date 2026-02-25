from __future__ import annotations

from asyncio import (
    CancelledError,
    Event,
    Task,
    TaskGroup,
    create_task,
    gather,
    get_running_loop,
    run,
    sleep,
    to_thread,
    wait_for,
)
from contextlib import suppress
from dataclasses import fields, is_dataclass, make_dataclass, replace
from importlib.util import module_from_spec, spec_from_file_location
from inspect import Parameter, iscoroutinefunction, signature
from logging import getLogger
from os.path import join
from signal import SIGINT, SIGTERM
from typing import Any, Callable, Type, cast, get_args, get_origin, get_type_hints

from aiofiles.os import listdir
from aiofiles.ospath import abspath, exists
from aiohttp import ClientSession, TCPConnector, web
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
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILURE,
    TASK_STATUS_SUCCESS,
)
from rxon.exceptions import RxonError
from rxon.models import (
    FileMetadata,
    HardwareDevice,
    Heartbeat,
    InstalledArtifact,
    Resources,
    ResourcesUsage,
    SkillInfo,
    TaskError,
    TaskPayload,
    TaskResult,
    WorkerCapabilities,
    WorkerEventPayload,
    WorkerRegistration,
)
from rxon.schema import extract_json_schema, extract_output_schema_from_func, extract_schema_from_func
from rxon.security import create_client_ssl_context
from rxon.utils import to_dict
from rxon.validators import validate_identifier

from .config import WorkerConfig
from .logging import clear_context, set_context, setup_logging
from .s3 import S3Manager
from .task_files import TaskFiles
from .types import CapacityChecker, Middleware, ParamValidationError

try:
    from pydantic import BaseModel, ValidationError

    _PYDANTIC_INSTALLED = True
except ImportError:
    _PYDANTIC_INSTALLED = False


def worker_extract_json_schema(schema_type: Any) -> dict[str, Any] | None:
    """Worker-specific wrapper that adds Pydantic support to rxon.schema.extract_json_schema."""
    return cast(dict[str, Any] | None, extract_json_schema(schema_type, extractor=_pydantic_extractor))


def _pydantic_extractor(schema_type: Any) -> dict[str, Any] | None:
    """Local Pydantic extractor for the worker SDK."""
    if _PYDANTIC_INSTALLED and isinstance(schema_type, type) and issubclass(schema_type, BaseModel):
        return cast(dict, cast(Type[BaseModel], schema_type).model_json_schema())
    return None


def worker_extract_schema_from_func(func: Any, arg_name: str) -> dict[str, Any] | None:
    """Local wrapper for extracting input schema from a function argument."""
    return cast(dict[str, Any] | None, extract_schema_from_func(func, arg_name, extractor=_pydantic_extractor))


def worker_extract_output_schema_from_func(func: Any) -> dict[str, Any] | None:
    """Local wrapper for extracting output schema from a function's return type."""
    return cast(dict[str, Any] | None, extract_output_schema_from_func(func, extractor=_pydantic_extractor))


logger = getLogger(__name__)


def _create_dynamic_skill_object(
    base_class: Type[SkillInfo],
    init_kwargs: dict[str, Any],
) -> SkillInfo:
    """
    Creates a SkillInfo object (or descendant).
    If init_kwargs contain fields not present in base_class, a new subclass is dynamically created.
    """
    known_field_names = {f.name for f in fields(base_class)}
    extra_kwargs = {k: v for k, v in init_kwargs.items() if k not in known_field_names}
    base_kwargs = {k: v for k, v in init_kwargs.items() if k in known_field_names}

    # Handle Schema Extraction for base args
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

    def __init__(self, skill_info_class: Type[SkillInfo] = SkillInfo) -> None:
        self._skills: list[tuple[SkillInfo, Callable]] = []
        self._skill_info_class = skill_info_class

    def skill(
        self,
        info: str | SkillInfo | Type[SkillInfo] | None = None,
        **kwargs: Any,
    ) -> Callable:
        """Universal decorator to register a skill handler on the blueprint."""
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

        base_info = _create_dynamic_skill_object(target_class, init_kwargs)

        def decorator(func: Callable) -> Callable:
            nonlocal base_info
            if not base_info.name:
                base_info = replace(base_info, name=func.__name__)

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
        skill_info_class: Type[SkillInfo] = SkillInfo,
    ):
        self._config = config or WorkerConfig()
        if skills_dir:
            self._config.WORKER_SKILLS_DIR = skills_dir

        setup_logging(worker_id=self._config.WORKER_ID)
        self._s3_manager = S3Manager(self._config)
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
        self._hot_cache: set[str] = set()
        self._active_tasks: dict[str, Task] = {}
        self._http_session = http_session
        self._session_is_managed_externally = http_session is not None
        self._shutdown_event = Event()
        self._registered_event = Event()
        self._debounce_task: Task | None = None
        self._ssl_context = None

        # HLN Optimization: Traffic reduction
        self._last_synced_skills_hash: str | None = None

        self._total_orchestrator_weight = 0
        if self._config.ORCHESTRATORS:
            for o in self._config.ORCHESTRATORS:
                o["current_weight"] = 0
                self._total_orchestrator_weight += o.get("weight", 1)
        self._clients = clients or []
        if not self._clients and self._http_session:
            self._init_clients()

    def add_middleware(self, middleware: Middleware) -> None:
        self._middlewares.append(middleware)

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
        info: str | SkillInfo | Type[SkillInfo] | None = None,
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

        base_info = _create_dynamic_skill_object(target_class, init_kwargs)

        def decorator(func: Callable) -> Callable:
            nonlocal base_info
            if not base_info.name:
                base_info = replace(base_info, name=func.__name__)

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

    def add_to_hot_cache(self, model_name: str) -> None:
        self._hot_cache.add(model_name)
        self._schedule_heartbeat_debounce()

    def remove_from_hot_cache(self, model_name: str) -> None:
        self._hot_cache.discard(model_name)
        self._schedule_heartbeat_debounce()

    def get_hot_cache(self) -> set[str]:
        return self._hot_cache

    def _get_current_state(self) -> dict[str, Any]:
        if self._current_load >= self._config.MAX_CONCURRENT_TASKS:
            return {"status": "busy", "supported_skills": []}

        supported_skills: list[SkillInfo] = []
        for handler_data in self._skill_handlers.values():
            is_available = True
            skill_type = handler_data.get("type")

            if skill_type and skill_type in self._skill_type_limits:
                limit = self._skill_type_limits[skill_type]
                current_load = self._current_load_by_type.get(skill_type, 0)
                if current_load >= limit:
                    is_available = False

            if is_available:
                skill_info = handler_data["info"]
                if self._capacity_checker:
                    if self._capacity_checker(skill_info.name):
                        supported_skills.append(skill_info)
                else:
                    supported_skills.append(skill_info)

        status = "idle" if supported_skills else "busy"
        return {"status": status, "supported_skills": supported_skills}

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

    async def _debounced_heartbeat_sender(self) -> None:
        await sleep(self._config.HEARTBEAT_DEBOUNCE_DELAY)
        await self._send_heartbeats_to_all()

    def _schedule_heartbeat_debounce(self) -> None:
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = create_task(self._debounced_heartbeat_sender())

    async def _poll_for_tasks(self, client: Transport) -> None:
        current_state = self._get_current_state()
        if current_state["status"] == "busy":
            return
        try:
            task_data = await client.poll_task(timeout=self._config.TASK_POLL_TIMEOUT)
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
        except RxonError as e:
            logger.error(f"Error polling tasks: {e}")

    async def _start_polling(self) -> None:
        await self._registered_event.wait()
        while not self._shutdown_event.is_set():
            if self._get_current_state()["status"] == "busy":
                await sleep(self._config.IDLE_POLL_DELAY)
                continue
            if self._config.MULTI_ORCHESTRATOR_MODE == "ROUND_ROBIN":
                if client := self._get_next_client():
                    await self._poll_for_tasks(client)
            else:
                for _, client in self._clients:
                    if self._get_current_state()["status"] == "busy":
                        break
                    await self._poll_for_tasks(client)
            if self._current_load == 0:
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

    def _prepare_dependencies(self, handler: Callable, job_id: str, task_id: str) -> dict[str, Any]:
        deps = {}
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        task_files = TaskFiles(task_dir, job_id=job_id, task_id=task_id, s3_manager=self._s3_manager)
        try:
            hints = get_type_hints(handler)
        except Exception:
            hints = {}
        for name, annotation in hints.items():
            origin = get_origin(annotation)
            types = get_args(annotation) if origin else (annotation,)
            if TaskFiles in types:
                deps[name] = task_files
        return deps

    async def _process_task(self, task_data_raw: dict[str, Any]) -> None:
        client: Transport = task_data_raw.pop("client")
        if "params_metadata" in task_data_raw and task_data_raw["params_metadata"]:
            task_data_raw["params_metadata"] = {
                k: self._from_dict(FileMetadata, v) for k, v in task_data_raw["params_metadata"].items()
            }
        task_payload = self._from_dict(TaskPayload, task_data_raw)
        task_id, job_id, skill_name = task_payload.task_id, task_payload.job_id, task_payload.type
        params = task_payload.params
        set_context(task_id=task_id, job_id=job_id)
        handler_data = self._skill_handlers.get(skill_name)
        skill_type_for_limit = handler_data.get("type") if handler_data else None
        result_obj: TaskResult | None = None

        async def send_event_wrapper(event_type: str, payload: dict[str, Any], priority: float = 0.0) -> None:
            if handler_data and (events_schema := handler_data["info"].events_schema):
                if event_type in events_schema:
                    from rxon.schema import validate_data

                    is_valid, error_msg = validate_data(payload, events_schema[event_type])
                    if not is_valid:
                        full_error = f"Local contract violation for event '{event_type}': {error_msg}"
                        logger.error(full_error)
                        return
                elif event_type != EVENT_TYPE_PROGRESS:
                    if self._config.STRICT_EVENT_VALIDATION:
                        logger.error(f"Contract violation: Emitting undeclared event type '{event_type}'. Blocked.")
                        return
                    else:
                        logger.warning(f"Emitting undeclared event type '{event_type}'. Allowed by config.")

            from time import time
            from uuid import uuid4

            event_payload = WorkerEventPayload(
                event_id=str(uuid4()),
                worker_id=self._config.WORKER_ID,
                origin_worker_id=self._config.WORKER_ID,
                event_type=event_type,
                payload=payload,
                bubbling_chain=[],
                target_job_id=job_id,
                target_task_id=task_id,
                trace_context=task_payload.tracing_context,
                priority=priority,
                timestamp=time(),
            )
            await client.emit_event(event_payload)

        async def send_progress_wrapper(task_id_arg, job_id_arg, progress, message=""):
            await send_event_wrapper(EVENT_TYPE_PROGRESS, {"progress": progress, "message": message})

        try:
            if not handler_data:
                message = f"Unsupported skill: {skill_name}"
                logger.warning(message)
                error = TaskError(code=ERROR_CODE_PERMANENT, message=message)
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    status=TASK_STATUS_FAILURE,
                    error=error,
                )
            else:
                # RUNTIME VALIDATION depends on function annotations (Pydantic/Dataclass)
                # Raw JSON Schema validation is removed to keep it lean.

                params = await self._s3_manager.process_params(params, task_id, metadata=task_payload.params_metadata)
                validated_params = self._prepare_task_params(handler_data["func"], params)
                deps = self._prepare_dependencies(handler_data["func"], job_id, task_id)
                handler_kwargs = {
                    "params": validated_params,
                    "task_id": task_id,
                    "job_id": job_id,
                    "tracing_context": task_payload.tracing_context,
                    "priority": task_data_raw.get("priority", 0),
                    "send_progress": send_progress_wrapper,
                    "send_event": send_event_wrapper,
                    "add_to_hot_cache": self.add_to_hot_cache,
                    "remove_from_hot_cache": self.remove_from_hot_cache,
                    **deps,
                }
                middleware_context = {
                    "task_id": task_id,
                    "job_id": job_id,
                    "skill_name": skill_name,
                    "params": validated_params,
                    "handler_kwargs": handler_kwargs,
                }

                async def _execution_logic() -> Any:
                    handler = handler_data["func"]
                    final_kwargs = middleware_context["handler_kwargs"]
                    if iscoroutinefunction(handler):
                        return await handler(**final_kwargs)
                    else:
                        return await to_thread(handler, **final_kwargs)

                handler_chain = _execution_logic
                for middleware in reversed(self._middlewares):

                    def make_wrapper(mw: Middleware, next_handler: Callable) -> Callable:
                        async def wrapper():
                            return await mw(middleware_context, next_handler)

                        return wrapper

                    handler_chain = make_wrapper(middleware, handler_chain)
                handler_result = await handler_chain()

                # FAIL FAST: Validate output schema locally before sending
                skill_info = handler_data["info"]
                if skill_info.output_schema:
                    from rxon.schema import validate_data

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
                        valid_err_fields = {k: v for k, v in err_data.items() if k in TaskError._fields}
                        task_error = TaskError(**valid_err_fields)
                    else:
                        task_error = TaskError(code=ERROR_CODE_TRANSIENT, message=str(err_data))
                result_obj = TaskResult(
                    job_id=job_id,
                    task_id=task_id,
                    worker_id=self._config.WORKER_ID,
                    status=handler_result.get("status", TASK_STATUS_SUCCESS),
                    data=updated_data,
                    error=task_error,
                    data_metadata=metadata_map if metadata_map else None,
                )
        except ParamValidationError as e:
            logger.error(f"Task {task_id} failed validation: {e}")
            error = TaskError(code=ERROR_CODE_INVALID_INPUT, message=str(e))
            result_obj = TaskResult(
                job_id=job_id,
                task_id=task_id,
                worker_id=self._config.WORKER_ID,
                status=TASK_STATUS_FAILURE,
                error=error,
            )
        except CancelledError:
            logger.info(f"Task {task_id} was cancelled.")
            result_obj = TaskResult(
                job_id=job_id, task_id=task_id, worker_id=self._config.WORKER_ID, status=TASK_STATUS_CANCELLED
            )
            raise
        except ValueError as e:
            logger.error(f"Data integrity or validation error for task {task_id}: {e}")
            error = TaskError(code=ERROR_CODE_INTEGRITY_MISMATCH, message=str(e))
            result_obj = TaskResult(
                job_id=job_id,
                task_id=task_id,
                worker_id=self._config.WORKER_ID,
                status=TASK_STATUS_FAILURE,
                error=error,
            )
        except Exception as e:
            logger.exception(f"An unexpected error occurred while processing task {task_id}:")
            error = TaskError(code=ERROR_CODE_TRANSIENT, message=str(e))
            result_obj = TaskResult(
                job_id=job_id,
                task_id=task_id,
                worker_id=self._config.WORKER_ID,
                status=TASK_STATUS_FAILURE,
                error=error,
            )
        finally:
            await self._s3_manager.cleanup(task_id)
            if result_obj:
                try:
                    accepted = await client.send_result(result_obj)
                    if not accepted:
                        logger.warning(
                            f"Task {task_id} result was IGNORED by orchestrator (possibly deadline exceeded)."
                        )
                except RxonError as e:
                    logger.error(f"Failed to send task result: {e}")
            clear_context("task_id", "job_id")
            self._active_tasks.pop(task_id, None)
            self._current_load -= 1
            if skill_type_for_limit:
                self._current_load_by_type[skill_type_for_limit] -= 1
            self._schedule_heartbeat_debounce()

    async def _manage_orchestrator_communications(self) -> None:
        await self._register_with_all_orchestrators()
        self._registered_event.set()
        if self._config.ENABLE_WEBSOCKETS:
            create_task(self._start_websocket_manager())
        while not self._shutdown_event.is_set():
            await self._send_heartbeats_to_all()
            await sleep(self._config.HEARTBEAT_INTERVAL)

    @staticmethod
    def _from_dict(cls: type, data: Any) -> Any:
        if not data:
            return None
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            return data
        fields_list = cast(Any, cls)._fields
        filtered_data = {k: v for k, v in data.items() if k in fields_list}
        return cls(**filtered_data)

    async def _register_with_all_orchestrators(self) -> None:
        state = self._get_current_state()
        devices = None
        if self._config.RESOURCES.get("devices"):
            devices = [HardwareDevice(**d) for d in self._config.RESOURCES["devices"]]

        resources = Resources(
            max_concurrent_tasks=self._config.MAX_CONCURRENT_TASKS,
            cpu_cores=self._config.RESOURCES["cpu_cores"],
            devices=devices,
        )
        s3_hash = calculate_config_hash(
            self._config.S3_ENDPOINT_URL,
            self._config.S3_ACCESS_KEY,
            self._config.S3_DEFAULT_BUCKET,
        )
        combined_extra = {"websockets": self._config.ENABLE_WEBSOCKETS}
        combined_extra.update(self._config.EXTRA_CAPABILITIES)

        from rxon.utils import calculate_dict_hash

        skills_hash = calculate_dict_hash(state["supported_skills"])
        self._last_synced_skills_hash = skills_hash

        registration = WorkerRegistration(
            worker_id=self._config.WORKER_ID,
            worker_type=self._config.WORKER_TYPE,
            supported_skills=state["supported_skills"],
            resources=resources,
            installed_software=self._config.INSTALLED_SOFTWARE,
            installed_artifacts=[InstalledArtifact(**m) for m in self._config.INSTALLED_ARTIFACTS],
            capabilities=WorkerCapabilities(
                hostname=self._config.HOSTNAME,
                ip_address=self._config.IP_ADDRESS,
                cost_per_skill=self._config.COST_PER_SKILL,
                s3_config_hash=s3_hash,
                extra=combined_extra,
            ),
            skills_hash=skills_hash,
        )
        await gather(*[self._safe_register(client, registration) for _, client in self._clients])

    @staticmethod
    async def _safe_register(client: Transport, registration: WorkerRegistration) -> None:
        try:
            resp = await client.register(registration)
            if isinstance(resp, dict) and (warning := resp.get("warning")):
                logger.warning(f"Registration warning from {client}: {warning}")
        except RxonError as e:
            logger.error(f"Registration failed for {client}: {e}")

    def _get_resources_usage(self) -> ResourcesUsage:
        """Collects current resource usage metrics."""
        # For now, using basic placeholders as psutil is not a dependency
        # In a real scenario, this would use psutil or platform-specific tools
        return ResourcesUsage(cpu_load_percent=0.0, ram_used_gb=0.0, devices_usage=None)

    async def _send_heartbeats_to_all(self) -> None:
        state = self._get_current_state()
        hot_skills = None
        if self._skill_dependencies:
            hot_skills = [
                skill for skill, models in self._skill_dependencies.items() if set(models).issubset(self._hot_cache)
            ]
            final_hot_skills = []
            for hs_name in hot_skills:
                if hs_name in self._skill_handlers:
                    final_hot_skills.append(self._skill_handlers[hs_name]["info"])
                else:
                    final_hot_skills.append(SkillInfo(name=hs_name))
            hot_skills = final_hot_skills

        from rxon.utils import calculate_dict_hash

        current_skills = state["supported_skills"]

        # HLN Optimization: Hash includes skills AND capabilities (prices, etc.)
        combined_contract = {
            "skills": current_skills,
            "capabilities": self._config.COST_PER_SKILL,
            "extra": self._config.EXTRA_CAPABILITIES,
        }
        current_hash = calculate_dict_hash(combined_contract)

        # HLN OPTIMIZATION: Only send skills if hash changed
        skills_to_send = None
        if current_hash != self._last_synced_skills_hash:
            logger.info(f"Worker contract changed (hash: {current_hash}). Sending full update.")
            skills_to_send = current_skills

        heartbeat = Heartbeat(
            worker_id=self._config.WORKER_ID,
            status=state["status"],
            usage=self._get_resources_usage(),
            current_tasks=list(self._active_tasks.keys()),
            supported_skills=skills_to_send,
            hot_cache=list(self._hot_cache),
            skill_dependencies=self._skill_dependencies or None,
            hot_skills=hot_skills or None,
            skills_hash=current_hash,
        )
        await gather(*[self._safe_heartbeat(client, heartbeat, current_hash) for _, client in self._clients])

    async def _safe_heartbeat(self, client: Transport, heartbeat: Heartbeat, current_hash: str) -> None:
        try:
            resp = await client.send_heartbeat(heartbeat)
            # If heartbeat accepted, update the synced hash
            self._last_synced_skills_hash = current_hash

            if resp:
                from rxon.constants import HB_RESP_CANCEL_TASKS, HB_RESP_REQUIRE_FULL_SYNC

                # HLN SELF-HEALING: Forced Full Sync from Orchestrator
                if resp.get(HB_RESP_REQUIRE_FULL_SYNC):
                    logger.warning(f"Orchestrator {client} requested Full Sync. Resetting state.")
                    self._last_synced_skills_hash = None

                if HB_RESP_CANCEL_TASKS in resp:
                    for task_id in resp[HB_RESP_CANCEL_TASKS]:
                        if task_id in self._active_tasks:
                            logger.warning(f"Task {task_id} marked for cancellation via heartbeat feedback loop.")
                            self._active_tasks[task_id].cancel()
        except RxonError as e:
            # HLN SELF-HEALING: If orchestrator doesn't know us, re-register
            if "not found" in str(e).lower() or "unauthorized" in str(e).lower():
                logger.warning(f"Orchestrator {client} lost our session. Re-registering...")
                self._last_synced_skills_hash = None  # Reset hash to force full sync
                await self._register_with_all_orchestrators()
            else:
                logger.warning(f"Heartbeat failed for {client}: {e}")

    async def main(self) -> None:
        self._config.validate()
        self._validate_skill_types()
        await self.load_skills()
        if self._config.EXTRA_CAPABILITIES:
            logger.info(f"Loaded custom capabilities from environment: {self._config.EXTRA_CAPABILITIES}")
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
            self._http_session = ClientSession(connector=connector)
            if not self._clients:
                self._init_clients()
        await gather(*[client.connect() for _, client in self._clients])
        comm_task = create_task(self._manage_orchestrator_communications())
        token_rotation_task = None
        if self._ssl_context:
            token_rotation_task = create_task(self._manage_token_rotation())
        polling_task = create_task(self._start_polling())
        await self._shutdown_event.wait()
        logger.info("Shutdown signal received. Starting graceful shutdown...")
        polling_task.cancel()
        logger.info("Sending final 'offline' heartbeat to all orchestrators...")
        with suppress(Exception):
            heartbeat = Heartbeat(
                worker_id=self._config.WORKER_ID,
                status="offline",
                usage=self._get_resources_usage(),
                current_tasks=list(self._active_tasks.keys()),
                supported_skills=[],
                hot_cache=list(self._hot_cache),
            )
            await gather(*[client.send_heartbeat(heartbeat) for _, client in self._clients], return_exceptions=True)
        comm_task.cancel()
        if token_rotation_task:
            token_rotation_task.cancel()
        if self._active_tasks:
            timeout = self._config.SHUTDOWN_TIMEOUT
            logger.info(f"Waiting for {len(self._active_tasks)} active tasks to complete (timeout: {timeout}s)...")
            try:
                await wait_for(
                    gather(*self._active_tasks.values(), return_exceptions=True), timeout=self._config.SHUTDOWN_TIMEOUT
                )
            except TimeoutError:
                logger.warning("Shutdown timeout reached. Some tasks may be interrupted.")
            except CancelledError:
                pass
        await gather(*[client.close() for _, client in self._clients])
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
                    logger.error(f"Error in token rotation loop: {e}")
            refresh_delay = max(60, min_expires_in * 0.8)
            logger.debug(f"Next token refresh scheduled in {refresh_delay:.1f}s")
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

    async def _listen_to_single_transport(self, client: Transport) -> None:
        while not self._shutdown_event.is_set():
            try:
                async for command in client.listen_for_commands():
                    if command.command == COMMAND_CANCEL_TASK:
                        task_id = command.task_id
                        job_id = command.job_id
                        if task_id in self._active_tasks:
                            self._active_tasks[task_id].cancel()
                            logger.info(f"Cancelled task {task_id} (Job: {job_id or 'N/A'}) by orchestrator command.")
            except Exception as e:
                logger.error(f"Error in command listener: {e}")
            await sleep(5)
