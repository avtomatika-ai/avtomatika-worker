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
from dataclasses import fields, is_dataclass
from importlib.util import module_from_spec, spec_from_file_location
from inspect import Parameter, iscoroutinefunction, signature
from logging import getLogger
from os.path import join
from signal import SIGINT, SIGTERM
from typing import Any, Callable, cast, get_args, get_origin, get_type_hints

from aiofiles.os import listdir
from aiofiles.ospath import abspath, exists
from aiohttp import ClientSession, TCPConnector, web
from rxon import Transport, create_transport
from rxon.blob import calculate_config_hash
from rxon.constants import (
    COMMAND_CANCEL_TASK,
    ERROR_CODE_INTEGRITY_MISMATCH,
    ERROR_CODE_INVALID_INPUT,
    ERROR_CODE_PERMANENT,
    ERROR_CODE_TRANSIENT,
    MSG_TYPE_PROGRESS,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_FAILURE,
    TASK_STATUS_SUCCESS,
)
from rxon.exceptions import RxonError
from rxon.models import (
    FileMetadata,
    GPUInfo,
    Heartbeat,
    InstalledModel,
    ProgressUpdatePayload,
    Resources,
    TaskError,
    TaskPayload,
    TaskResult,
    WorkerCapabilities,
    WorkerRegistration,
)
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

# Logging setup
logger = getLogger(__name__)


class SkillBlueprint:
    """
    A collection of tasks that can be registered on a Worker.
    Allows defining tasks in separate files without a worker instance.
    """

    def __init__(self) -> None:
        self._tasks: list[tuple[str, str | None, Callable]] = []

    def task(self, name: str, task_type: str | None = None) -> Callable:
        """Decorator to register a task handler on the blueprint."""

        def decorator(func: Callable) -> Callable:
            self._tasks.append((name, task_type, func))
            return func

        return decorator


class Worker:
    """The main class for creating and running a worker.
    Implements a hybrid interaction model with the Orchestrator:
    - PULL model for fetching tasks.
    - Transport layer for real-time commands (cancellation) and sending progress.
    """

    def __init__(
        self,
        worker_type: str = "generic-worker",
        max_concurrent_tasks: int | None = None,
        task_type_limits: dict[str, int] | None = None,
        http_session: ClientSession | None = None,
        skill_dependencies: dict[str, list[str]] | None = None,
        config: WorkerConfig | None = None,
        capacity_checker: CapacityChecker | None = None,
        clients: list[tuple[dict[str, Any], Transport]] | None = None,
        skills_dir: str | None = None,
    ):
        self._config = config or WorkerConfig()
        if skills_dir:
            self._config.WORKER_SKILLS_DIR = skills_dir

        setup_logging(worker_id=self._config.WORKER_ID)
        self._s3_manager = S3Manager(self._config)
        self._config.WORKER_TYPE = worker_type  # Allow overriding worker_type
        if max_concurrent_tasks is not None:
            self._config.MAX_CONCURRENT_TASKS = max_concurrent_tasks

        self._task_type_limits = task_type_limits or {}
        self._task_handlers: dict[str, dict[str, Any]] = {}
        self._skill_dependencies = skill_dependencies or {}
        self._middlewares: list[Middleware] = []
        self._capacity_checker = capacity_checker

        # Worker state
        self._current_load = 0
        self._current_load_by_type: dict[str, int] = dict.fromkeys(self._task_type_limits, 0)
        self._hot_cache: set[str] = set()
        self._active_tasks: dict[str, Task] = {}
        self._http_session = http_session
        self._session_is_managed_externally = http_session is not None
        self._shutdown_event = Event()
        self._registered_event = Event()
        self._debounce_task: Task | None = None
        self._ssl_context = None

        # --- Weighted Round-Robin State ---
        self._total_orchestrator_weight = 0
        if self._config.ORCHESTRATORS:
            for o in self._config.ORCHESTRATORS:
                o["current_weight"] = 0
                self._total_orchestrator_weight += o.get("weight", 1)
        self._clients = clients or []
        if not self._clients and self._http_session:
            self._init_clients()

    def add_middleware(self, middleware: Middleware) -> None:
        """Adds a middleware to the execution chain."""
        self._middlewares.append(middleware)

    def include_blueprint(self, blueprint: SkillBlueprint) -> None:
        """Registers all tasks from a blueprint."""
        for name, task_type, func in blueprint._tasks:
            self.task(name, task_type)(func)

    async def load_skills(self, skills_dir: str | None = None) -> None:
        """
        Dynamically loads skills from the specified directory.
        Scans for .py files and looks for 'SkillBlueprint' instances or 'setup(worker)' functions.
        """
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

                    # 1. Look for SkillBlueprint instances
                    found_anything = False
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if isinstance(attr, SkillBlueprint):
                            self.include_blueprint(attr)
                            found_anything = True

                    # 2. Look for setup(worker) function
                    if hasattr(module, "setup") and callable(module.setup):
                        module.setup(self)
                        found_anything = True

                    if found_anything:
                        logger.info(f"Loaded skills from: {filename}")
                except Exception as e:
                    logger.error(f"Failed to load skills from {filename}: {e}")

    def _init_clients(self) -> None:
        """Initializes Transport instances for each configured orchestrator."""
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

    def _validate_task_types(self) -> None:
        """Checks for unused task type limits and warns the user."""
        registered_task_types = {
            handler_data["type"] for handler_data in self._task_handlers.values() if handler_data["type"]
        }

        for task_type in self._task_type_limits:
            if task_type not in registered_task_types:
                logger.warning(
                    f"Configuration warning: A limit is defined for task type '{task_type}', "
                    "but no tasks are registered with this type."
                )

    def task(self, name: str, task_type: str | None = None) -> Callable:
        """Decorator to register a function as a task handler."""
        validate_identifier(name, "task name")
        if task_type:
            validate_identifier(task_type, "task type")

        def decorator(func: Callable) -> Callable:
            logger.info(f"Registering task: '{name}' (type: {task_type or 'N/A'})")
            if task_type and task_type not in self._task_type_limits:
                logger.warning(
                    f"Task '{name}' has a type '{task_type}' which is not defined in 'task_type_limits'. "
                    "No concurrency limit will be applied for this type."
                )
            if task_type and task_type not in self._current_load_by_type:
                self._current_load_by_type[task_type] = 0
            self._task_handlers[name] = {"func": func, "type": task_type}
            return func

        return decorator

    def add_to_hot_cache(self, model_name: str) -> None:
        """Adds a model to the hot cache."""
        self._hot_cache.add(model_name)
        self._schedule_heartbeat_debounce()

    def remove_from_hot_cache(self, model_name: str) -> None:
        """Removes a model from the hot cache."""
        self._hot_cache.discard(model_name)
        self._schedule_heartbeat_debounce()

    def get_hot_cache(self) -> set[str]:
        """Returns the hot cache."""
        return self._hot_cache

    def _get_current_state(self) -> dict[str, Any]:
        """
        Calculates the current worker state including status and available skills.
        """
        if self._current_load >= self._config.MAX_CONCURRENT_TASKS:
            return {"status": "busy", "supported_skills": []}

        supported_skills = []
        for name, handler_data in self._task_handlers.items():
            is_available = True
            task_type = handler_data.get("type")

            if task_type and task_type in self._task_type_limits:
                limit = self._task_type_limits[task_type]
                current_load = self._current_load_by_type.get(task_type, 0)
                if current_load >= limit:
                    is_available = False

            if is_available:
                supported_skills.append(name)

        if self._capacity_checker:
            supported_skills = [skill for skill in supported_skills if self._capacity_checker(skill)]

        status = "idle" if supported_skills else "busy"
        return {"status": status, "supported_skills": supported_skills}

    def _get_next_client(self) -> Transport | None:
        """
        Selects the next orchestrator client using a smooth weighted round-robin algorithm.
        """
        if not self._clients:
            return None

        # The orchestrator with the highest current_weight is selected.
        selected_client = None
        highest_weight = -1

        for o, client in self._clients:
            o["current_weight"] += o["weight"]
            if o["current_weight"] > highest_weight:
                highest_weight = o["current_weight"]
                selected_client = client

        if selected_client:
            # Find the config for the selected client to decrement its weight
            for o, client in self._clients:
                if client == selected_client:
                    o["current_weight"] -= self._total_orchestrator_weight
                    break

        return selected_client

    async def _debounced_heartbeat_sender(self) -> None:
        """Waits for the debounce delay then sends a heartbeat."""
        await sleep(self._config.HEARTBEAT_DEBOUNCE_DELAY)
        await self._send_heartbeats_to_all()

    def _schedule_heartbeat_debounce(self) -> None:
        """Schedules a debounced heartbeat, cancelling any pending one."""
        # Cancel the previously scheduled task, if it exists and is not done.
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        # Schedule the new debounced call.
        self._debounce_task = create_task(self._debounced_heartbeat_sender())

    async def _poll_for_tasks(self, client: Transport) -> None:
        """Polls a specific Orchestrator for new tasks."""
        current_state = self._get_current_state()
        if current_state["status"] == "busy":
            return

        try:
            task_data = await client.poll_task(timeout=self._config.TASK_POLL_TIMEOUT)
            if task_data:
                task_data_dict = to_dict(task_data)
                task_data_dict["client"] = client

                self._current_load += 1
                if (task_handler_info := self._task_handlers.get(task_data.type)) and (
                    task_type_for_limit := task_handler_info.get("type")
                ):
                    self._current_load_by_type[task_type_for_limit] += 1
                self._schedule_heartbeat_debounce()

                task = create_task(self._process_task(task_data_dict))
                self._active_tasks[task_data.task_id] = task
        except RxonError as e:
            logger.error(f"Error polling tasks: {e}")

    async def _start_polling(self) -> None:
        """The main loop for polling tasks."""
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
        """
        Inspects the handler's signature to validate and instantiate params.
        """
        sig = signature(handler)
        params_param = sig.parameters.get("params")
        if params_param is None:
            return params
        params_annotation = params_param.annotation

        if params_annotation is sig.empty or params_annotation is dict:
            return params

        # Pydantic Model Validation
        if _PYDANTIC_INSTALLED and isinstance(params_annotation, type) and issubclass(params_annotation, BaseModel):
            try:
                return cast(Any, params_annotation).model_validate(params)
            except ValidationError as e:
                raise ParamValidationError(str(e)) from e

        # Dataclass Instantiation
        if isinstance(params_annotation, type) and is_dataclass(params_annotation):
            try:
                # Filter unknown fields
                known_fields = {f.name for f in fields(params_annotation)}
                filtered_params = {k: v for k, v in params.items() if k in known_fields}

                # Check required fields
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
        """Injects dependencies based on type hints."""
        deps = {}
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        task_files = TaskFiles(task_dir, job_id=job_id, task_id=task_id, s3_manager=self._s3_manager)

        try:
            hints = get_type_hints(handler)
        except Exception:
            # Fallback for handlers where hints can't be resolved
            hints = {}

        for name, annotation in hints.items():
            origin = get_origin(annotation)
            types = get_args(annotation) if origin else (annotation,)

            if TaskFiles in types:
                deps[name] = task_files

        return deps

    async def _process_task(self, task_data_raw: dict[str, Any]) -> None:
        """Executes the task logic."""
        client: Transport = task_data_raw.pop("client")

        # Parse incoming task data using protocol model
        if "params_metadata" in task_data_raw and task_data_raw["params_metadata"]:
            task_data_raw["params_metadata"] = {
                k: self._from_dict(FileMetadata, v) for k, v in task_data_raw["params_metadata"].items()
            }

        task_payload = self._from_dict(TaskPayload, task_data_raw)
        task_id, job_id, task_name = task_payload.task_id, task_payload.job_id, task_payload.type
        params = task_payload.params

        set_context(task_id=task_id, job_id=job_id)

        handler_data = self._task_handlers.get(task_name)
        task_type_for_limit = handler_data.get("type") if handler_data else None

        result_obj: TaskResult | None = None

        # Create a progress sender wrapper attached to this specific client
        async def send_progress_wrapper(task_id_arg, job_id_arg, progress, message=""):
            payload = ProgressUpdatePayload(
                event=MSG_TYPE_PROGRESS, task_id=task_id_arg, job_id=job_id_arg, progress=progress, message=message
            )
            await client.send_progress(payload)

        try:
            if not handler_data:
                message = f"Unsupported task: {task_name}"
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
                # Download files
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
                    "add_to_hot_cache": self.add_to_hot_cache,
                    "remove_from_hot_cache": self.remove_from_hot_cache,
                    **deps,
                }

                middleware_context = {
                    "task_id": task_id,
                    "job_id": job_id,
                    "task_name": task_name,
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

                updated_data, metadata_map = await self._s3_manager.process_result(
                    handler_result.get("data", {}), s3_prefix=job_id
                )

                # Prepare error object safely
                task_error = None
                if "error" in handler_result:
                    err_data = handler_result["error"]
                    if isinstance(err_data, dict):
                        # Ensure we only pass valid fields to TaskError
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
            if task_type_for_limit:
                self._current_load_by_type[task_type_for_limit] -= 1
            self._schedule_heartbeat_debounce()

    async def _manage_orchestrator_communications(self) -> None:
        """Registers the worker and sends heartbeats."""
        await self._register_with_all_orchestrators()

        self._registered_event.set()
        if self._config.ENABLE_WEBSOCKETS:
            create_task(self._start_websocket_manager())

        while not self._shutdown_event.is_set():
            await self._send_heartbeats_to_all()
            await sleep(self._config.HEARTBEAT_INTERVAL)

    @staticmethod
    def _from_dict(cls: type, data: Any) -> Any:
        """Safely instantiates a NamedTuple from a dict, ignoring unknown fields."""
        if not data:
            return None
        if isinstance(data, cls):
            return data
        if not isinstance(data, dict):
            return data
        fields = cast(Any, cls)._fields
        filtered_data = {k: v for k, v in data.items() if k in fields}
        return cls(**filtered_data)

    async def _register_with_all_orchestrators(self) -> None:
        """Registers the worker with all orchestrators."""
        state = self._get_current_state()

        gpu_info = None
        if self._config.RESOURCES.get("gpu_info"):
            gpu_info = GPUInfo(**self._config.RESOURCES["gpu_info"])

        resources = Resources(
            max_concurrent_tasks=self._config.MAX_CONCURRENT_TASKS,
            cpu_cores=self._config.RESOURCES["cpu_cores"],
            gpu_info=gpu_info,
        )

        s3_hash = calculate_config_hash(
            self._config.S3_ENDPOINT_URL,
            self._config.S3_ACCESS_KEY,
            self._config.S3_DEFAULT_BUCKET,
        )

        # Merge default extra fields with dynamic ones from ENV
        combined_extra = {"websockets": self._config.ENABLE_WEBSOCKETS}
        combined_extra.update(self._config.EXTRA_CAPABILITIES)

        registration = WorkerRegistration(
            worker_id=self._config.WORKER_ID,
            worker_type=self._config.WORKER_TYPE,
            supported_skills=state["supported_skills"],
            resources=resources,
            installed_software=self._config.INSTALLED_SOFTWARE,
            installed_models=[InstalledModel(**m) for m in self._config.INSTALLED_MODELS],
            capabilities=WorkerCapabilities(
                hostname=self._config.HOSTNAME,
                ip_address=self._config.IP_ADDRESS,
                cost_per_skill=self._config.COST_PER_SKILL,
                s3_config_hash=s3_hash,
                extra=combined_extra,
            ),
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

    async def _send_heartbeats_to_all(self) -> None:
        """Sends heartbeat messages to all orchestrators."""
        state = self._get_current_state()

        hot_skills = None
        if self._skill_dependencies:
            hot_skills = [
                skill for skill, models in self._skill_dependencies.items() if set(models).issubset(self._hot_cache)
            ]

        heartbeat = Heartbeat(
            worker_id=self._config.WORKER_ID,
            status=state["status"],
            load=float(self._current_load),
            current_tasks=list(self._active_tasks.keys()),
            supported_skills=state["supported_skills"],
            hot_cache=list(self._hot_cache),
            skill_dependencies=self._skill_dependencies or None,
            hot_skills=hot_skills or None,
        )

        await gather(*[self._safe_heartbeat(client, heartbeat) for _, client in self._clients])

    async def _safe_heartbeat(self, client: Transport, heartbeat: Heartbeat) -> None:
        try:
            resp = await client.send_heartbeat(heartbeat)
            if resp and "cancel_task_ids" in resp:
                for task_id in resp["cancel_task_ids"]:
                    if task_id in self._active_tasks:
                        logger.warning(f"Task {task_id} marked for cancellation via heartbeat feedback loop.")
                        self._active_tasks[task_id].cancel()
        except RxonError as e:
            logger.warning(f"Heartbeat failed for {client}: {e}")

    async def main(self) -> None:
        """The main asynchronous function."""
        self._config.validate()
        self._validate_task_types()

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

        # Connect transports
        await gather(*[client.connect() for _, client in self._clients])

        comm_task = create_task(self._manage_orchestrator_communications())

        token_rotation_task = None
        if self._ssl_context:
            token_rotation_task = create_task(self._manage_token_rotation())

        polling_task = create_task(self._start_polling())

        # Wait for shutdown signal
        await self._shutdown_event.wait()
        logger.info("Shutdown signal received. Starting graceful shutdown...")

        # 1. Stop polling and communications
        polling_task.cancel()

        # HLN RELIABILITY: Explicitly tell orchestrators we are going offline
        # This prevents new tasks from being dispatched to us during the drain period.
        logger.info("Sending final 'offline' heartbeat to all orchestrators...")
        with suppress(Exception):
            heartbeat = Heartbeat(
                worker_id=self._config.WORKER_ID,
                status="offline",
                load=float(self._current_load),
                current_tasks=list(self._active_tasks.keys()),
                supported_skills=[],
                hot_cache=list(self._hot_cache),
            )
            await gather(*[client.send_heartbeat(heartbeat) for _, client in self._clients], return_exceptions=True)

        comm_task.cancel()
        if token_rotation_task:
            token_rotation_task.cancel()

        # 2. Wait for active tasks with timeout (Drain Mode)
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

        # 3. Final cleanup
        # Close transports
        await gather(*[client.close() for _, client in self._clients])

        if self._http_session and not self._http_session.closed and not self._session_is_managed_externally:
            await self._http_session.close()

    def run(self) -> None:
        """Runs the worker."""
        try:
            run(self.main())
        except KeyboardInterrupt:
            self._shutdown_event.set()

    async def _manage_token_rotation(self) -> None:
        """Periodically refreshes auth tokens for all clients."""
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
            # Check if all exceptions in the group are related to shutdown
            if all(isinstance(e, (KeyboardInterrupt, CancelledError)) for e in eg.exceptions):
                self._shutdown_event.set()
            else:
                raise

    async def _start_websocket_manager(self) -> None:
        """Manages the command listeners."""
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
