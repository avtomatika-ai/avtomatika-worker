from asyncio import CancelledError, Event, Task, create_task, gather, run, sleep
from asyncio import TimeoutError as AsyncTimeoutError
from dataclasses import is_dataclass
from inspect import Parameter, signature
from json import JSONDecodeError
from logging import getLogger
from os.path import join
from typing import Any, Callable

from aiohttp import ClientError, ClientSession, ClientTimeout, ClientWebSocketResponse, WSMsgType, web

from .config import WorkerConfig
from .s3 import S3Manager
from .task_files import TaskFiles
from .types import INVALID_INPUT_ERROR, PERMANENT_ERROR, TRANSIENT_ERROR, ParamValidationError

try:
    from pydantic import BaseModel, ValidationError

    _PYDANTIC_INSTALLED = True
except ImportError:
    _PYDANTIC_INSTALLED = False

# Logging setup
logger = getLogger(__name__)


class Worker:
    """The main class for creating and running a worker.
    Implements a hybrid interaction model with the Orchestrator:
    - PULL model for fetching tasks.
    - WebSocket for real-time commands (cancellation) and sending progress.
    """

    def __init__(
        self,
        worker_type: str = "generic-worker",
        max_concurrent_tasks: int | None = None,
        task_type_limits: dict[str, int] | None = None,
        http_session: ClientSession | None = None,
        skill_dependencies: dict[str, list[str]] | None = None,
        config: WorkerConfig | None = None,
    ):
        self._config = config or WorkerConfig()
        self._s3_manager = S3Manager(self._config)
        self._config.WORKER_TYPE = worker_type  # Allow overriding worker_type
        if max_concurrent_tasks is not None:
            self._config.max_concurrent_tasks = max_concurrent_tasks

        self._task_type_limits = task_type_limits or {}
        self._task_handlers: dict[str, dict[str, Any]] = {}
        self._skill_dependencies = skill_dependencies or {}

        # Worker state
        self._current_load = 0
        self._current_load_by_type: dict[str, int] = dict.fromkeys(self._task_type_limits, 0)
        self._hot_cache: set[str] = set()
        self._active_tasks: dict[str, Task] = {}
        self._http_session = http_session
        self._session_is_managed_externally = http_session is not None
        self._ws_connection: ClientWebSocketResponse | None = None
        # Removed: self._headers = {"X-Worker-Token": self._config.WORKER_TOKEN}
        self._shutdown_event = Event()
        self._registered_event = Event()
        self._round_robin_index = 0
        self._debounce_task: Task | None = None

        # --- Weighted Round-Robin State ---
        self._total_orchestrator_weight = 0
        if self._config.ORCHESTRATORS:
            for o in self._config.ORCHESTRATORS:
                o["current_weight"] = 0
                self._total_orchestrator_weight += o.get("weight", 1)

    def _validate_config(self):
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

    def add_to_hot_cache(self, model_name: str):
        """Adds a model to the hot cache."""
        self._hot_cache.add(model_name)
        self._schedule_heartbeat_debounce()

    def remove_from_hot_cache(self, model_name: str):
        """Removes a model from the hot cache."""
        self._hot_cache.discard(model_name)
        self._schedule_heartbeat_debounce()

    def get_hot_cache(self) -> set[str]:
        """Returns the hot cache."""
        return self._hot_cache

    def _get_current_state(self) -> dict[str, Any]:
        """
        Calculates the current worker state including status and available tasks.
        """
        if self._current_load >= self._config.MAX_CONCURRENT_TASKS:
            return {"status": "busy", "supported_tasks": []}

        supported_tasks = []
        for name, handler_data in self._task_handlers.items():
            is_available = True
            task_type = handler_data.get("type")

            if task_type and task_type in self._task_type_limits:
                limit = self._task_type_limits[task_type]
                current_load = self._current_load_by_type.get(task_type, 0)
                if current_load >= limit:
                    is_available = False

            if is_available:
                supported_tasks.append(name)

        status = "idle" if supported_tasks else "busy"
        return {"status": status, "supported_tasks": supported_tasks}

    def _get_headers(self, orchestrator: dict[str, Any]) -> dict[str, str]:
        """Builds authentication headers for a specific orchestrator."""
        token = orchestrator.get("token", self._config.WORKER_TOKEN)
        return {"X-Worker-Token": token}

    def _get_next_orchestrator(self) -> dict[str, Any] | None:
        """
        Selects the next orchestrator using a smooth weighted round-robin algorithm.
        """
        if not self._config.ORCHESTRATORS:
            return None

        # The orchestrator with the highest current_weight is selected.
        selected_orchestrator = None
        highest_weight = -1

        for o in self._config.ORCHESTRATORS:
            o["current_weight"] += o["weight"]
            if o["current_weight"] > highest_weight:
                highest_weight = o["current_weight"]
                selected_orchestrator = o

        if selected_orchestrator:
            selected_orchestrator["current_weight"] -= self._total_orchestrator_weight

        return selected_orchestrator

    async def _debounced_heartbeat_sender(self):
        """Waits for the debounce delay then sends a heartbeat."""
        await sleep(self._config.HEARTBEAT_DEBOUNCE_DELAY)
        await self._send_heartbeats_to_all()

    def _schedule_heartbeat_debounce(self):
        """Schedules a debounced heartbeat, cancelling any pending one."""
        # Cancel the previously scheduled task, if it exists and is not done.
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        # Schedule the new debounced call.
        self._debounce_task = create_task(self._debounced_heartbeat_sender())

    async def _poll_for_tasks(self, orchestrator: dict[str, Any]):
        """Polls a specific Orchestrator for new tasks."""
        url = f"{orchestrator['url']}/_worker/workers/{self._config.WORKER_ID}/tasks/next"
        try:
            if not self._http_session:
                return
            timeout = ClientTimeout(total=self._config.TASK_POLL_TIMEOUT + 5)
            headers = self._get_headers(orchestrator)
            async with self._http_session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 200:
                    task_data = await resp.json()
                    task_data["orchestrator"] = orchestrator

                    self._current_load += 1
                    if (task_handler_info := self._task_handlers.get(task_data["type"])) and (
                        task_type_for_limit := task_handler_info.get("type")
                    ):
                        self._current_load_by_type[task_type_for_limit] += 1
                    self._schedule_heartbeat_debounce()

                    task = create_task(self._process_task(task_data))
                    self._active_tasks[task_data["task_id"]] = task
                elif resp.status != 204:
                    await sleep(self._config.TASK_POLL_ERROR_DELAY)
        except (AsyncTimeoutError, ClientError) as e:
            logger.error(f"Error polling for tasks: {e}")
            await sleep(self._config.TASK_POLL_ERROR_DELAY)

    async def _start_polling(self):
        """The main loop for polling tasks."""
        await self._registered_event.wait()

        while not self._shutdown_event.is_set():
            if self._get_current_state()["status"] == "busy":
                await sleep(self._config.IDLE_POLL_DELAY)
                continue

            if self._config.MULTI_ORCHESTRATOR_MODE == "ROUND_ROBIN":
                if orchestrator := self._get_next_orchestrator():
                    await self._poll_for_tasks(orchestrator)
            else:
                for orchestrator in self._config.ORCHESTRATORS:
                    if self._get_current_state()["status"] == "busy":
                        break
                    await self._poll_for_tasks(orchestrator)

            if self._current_load == 0:
                await sleep(self._config.IDLE_POLL_DELAY)

    @staticmethod
    def _prepare_task_params(handler: Callable, params: dict[str, Any]) -> Any:
        """
        Inspects the handler's signature to validate and instantiate params.
        Supports dict, dataclasses, and optional pydantic models.
        """
        sig = signature(handler)
        params_annotation = sig.parameters.get("params").annotation

        if params_annotation is sig.empty or params_annotation is dict:
            return params

        # Pydantic Model Validation
        if _PYDANTIC_INSTALLED and isinstance(params_annotation, type) and issubclass(params_annotation, BaseModel):
            try:
                return params_annotation.model_validate(params)
            except ValidationError as e:
                raise ParamValidationError(str(e)) from e

        # Dataclass Instantiation
        if isinstance(params_annotation, type) and is_dataclass(params_annotation):
            try:
                # Filter unknown fields to prevent TypeError on dataclass instantiation
                known_fields = {f.name for f in params_annotation.__dataclass_fields__.values()}
                filtered_params = {k: v for k, v in params.items() if k in known_fields}

                # Explicitly check for missing required fields
                required_fields = [
                    f.name
                    for f in params_annotation.__dataclass_fields__.values()
                    if f.default is Parameter.empty and f.default_factory is Parameter.empty
                ]

                if missing_fields := [f for f in required_fields if f not in filtered_params]:
                    raise ParamValidationError(f"Missing required fields for dataclass: {', '.join(missing_fields)}")

                return params_annotation(**filtered_params)
            except (TypeError, ValueError) as e:
                # TypeError for missing/extra args, ValueError from __post_init__
                raise ParamValidationError(str(e)) from e

        return params

    def _prepare_dependencies(self, handler: Callable, task_id: str) -> dict[str, Any]:
        """Injects dependencies based on type hints."""
        deps = {}
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        # Always create the object, but directory is lazy
        task_files = TaskFiles(task_dir)

        sig = signature(handler)
        for name, param in sig.parameters.items():
            if param.annotation is TaskFiles:
                deps[name] = task_files

        return deps

    async def _process_task(self, task_data: dict[str, Any]):
        """Executes the task logic."""
        task_id, job_id, task_name = task_data["task_id"], task_data["job_id"], task_data["type"]
        params, orchestrator = task_data.get("params", {}), task_data["orchestrator"]

        result: dict[str, Any] = {}
        handler_data = self._task_handlers.get(task_name)
        task_type_for_limit = handler_data.get("type") if handler_data else None

        result_sent = False  # Flag to track if result has been sent

        try:
            if not handler_data:
                message = f"Unsupported task: {task_name}"
                logger.warning(message)
                result = {"status": "failure", "error": {"code": PERMANENT_ERROR, "message": message}}
                payload = {"job_id": job_id, "task_id": task_id, "worker_id": self._config.WORKER_ID, "result": result}
                await self._send_result(payload, orchestrator)
                result_sent = True  # Mark result as sent
                return

            params = await self._s3_manager.process_params(params, task_id)
            validated_params = self._prepare_task_params(handler_data["func"], params)
            deps = self._prepare_dependencies(handler_data["func"], task_id)

            result = await handler_data["func"](
                validated_params,
                task_id=task_id,
                job_id=job_id,
                priority=task_data.get("priority", 0),
                send_progress=self.send_progress,
                add_to_hot_cache=self.add_to_hot_cache,
                remove_from_hot_cache=self.remove_from_hot_cache,
                **deps,
            )
            result = await self._s3_manager.process_result(result)
        except ParamValidationError as e:
            logger.error(f"Task {task_id} failed validation: {e}")
            result = {"status": "failure", "error": {"code": INVALID_INPUT_ERROR, "message": str(e)}}
        except CancelledError:
            logger.info(f"Task {task_id} was cancelled.")
            result = {"status": "cancelled"}
            # We must re-raise the exception to be handled by the outer gather
            raise
        except Exception as e:
            logger.exception(f"An unexpected error occurred while processing task {task_id}:")
            result = {"status": "failure", "error": {"code": TRANSIENT_ERROR, "message": str(e)}}
        finally:
            # Cleanup task workspace
            await self._s3_manager.cleanup(task_id)

            if not result_sent:  # Only send if not already sent
                payload = {"job_id": job_id, "task_id": task_id, "worker_id": self._config.WORKER_ID, "result": result}
                await self._send_result(payload, orchestrator)
            self._active_tasks.pop(task_id, None)

            self._current_load -= 1
            if task_type_for_limit:
                self._current_load_by_type[task_type_for_limit] -= 1
            self._schedule_heartbeat_debounce()

    async def _send_result(self, payload: dict[str, Any], orchestrator: dict[str, Any]):
        """Sends the result to a specific orchestrator."""
        url = f"{orchestrator['url']}/_worker/tasks/result"
        delay = self._config.RESULT_RETRY_INITIAL_DELAY
        headers = self._get_headers(orchestrator)
        for i in range(self._config.RESULT_MAX_RETRIES):
            try:
                if self._http_session and not self._http_session.closed:
                    async with self._http_session.post(url, json=payload, headers=headers) as resp:
                        if resp.status == 200:
                            return
            except ClientError as e:
                logger.error(f"Error sending result: {e}")
            await sleep(delay * (2**i))

    async def _manage_orchestrator_communications(self):
        """Registers the worker and sends heartbeats."""
        await self._register_with_all_orchestrators()

        self._registered_event.set()
        if self._config.ENABLE_WEBSOCKETS:
            create_task(self._start_websocket_manager())

        while not self._shutdown_event.is_set():
            await self._send_heartbeats_to_all()
            await sleep(self._config.HEARTBEAT_INTERVAL)

    async def _register_with_all_orchestrators(self):
        """Registers the worker with all orchestrators."""
        state = self._get_current_state()
        payload = {
            "worker_id": self._config.WORKER_ID,
            "worker_type": self._config.WORKER_TYPE,
            "supported_tasks": state["supported_tasks"],
            "max_concurrent_tasks": self._config.MAX_CONCURRENT_TASKS,
            "cost_per_skill": self._config.COST_PER_SKILL,
            "installed_models": self._config.INSTALLED_MODELS,
            "hostname": self._config.HOSTNAME,
            "ip_address": self._config.IP_ADDRESS,
            "resources": self._config.RESOURCES,
        }
        for orchestrator in self._config.ORCHESTRATORS:
            url = f"{orchestrator['url']}/_worker/workers/register"
            try:
                if self._http_session:
                    async with self._http_session.post(
                        url, json=payload, headers=self._get_headers(orchestrator)
                    ) as resp:
                        if resp.status >= 400:
                            logger.error(f"Error registering with {orchestrator['url']}: {resp.status}")
            except ClientError as e:
                logger.error(f"Error registering with orchestrator {orchestrator['url']}: {e}")

    async def _send_heartbeats_to_all(self):
        """Sends heartbeat messages to all orchestrators."""
        state = self._get_current_state()
        payload = {
            "load": self._current_load,
            "status": state["status"],
            "supported_tasks": state["supported_tasks"],
            "hot_cache": list(self._hot_cache),
        }

        if self._skill_dependencies:
            payload["skill_dependencies"] = self._skill_dependencies
            hot_skills = [
                skill for skill, models in self._skill_dependencies.items() if set(models).issubset(self._hot_cache)
            ]
            if hot_skills:
                payload["hot_skills"] = hot_skills

        async def _send_single(orchestrator: dict[str, Any]):
            url = f"{orchestrator['url']}/_worker/workers/{self._config.WORKER_ID}"
            headers = self._get_headers(orchestrator)
            try:
                if self._http_session and not self._http_session.closed:
                    async with self._http_session.patch(url, json=payload, headers=headers) as resp:
                        if resp.status >= 400:
                            logger.warning(f"Heartbeat to {orchestrator['url']} failed with status: {resp.status}")
            except ClientError as e:
                logger.error(f"Error sending heartbeat to orchestrator {orchestrator['url']}: {e}")

        await gather(*[_send_single(o) for o in self._config.ORCHESTRATORS])

    async def main(self):
        """The main asynchronous function."""
        self._validate_config()  # Validate config now that all tasks are registered
        if not self._http_session:
            self._http_session = ClientSession()

        comm_task = create_task(self._manage_orchestrator_communications())

        polling_task = create_task(self._start_polling())
        await self._shutdown_event.wait()

        for task in [comm_task, polling_task]:
            task.cancel()
        if self._active_tasks:
            await gather(*self._active_tasks.values(), return_exceptions=True)

        if self._ws_connection and not self._ws_connection.closed:
            await self._ws_connection.close()
        if self._http_session and not self._http_session.closed and not self._session_is_managed_externally:
            await self._http_session.close()

    def run(self):
        """Runs the worker."""
        try:
            run(self.main())
        except KeyboardInterrupt:
            self._shutdown_event.set()

    async def _run_health_check_server(self):
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

    def run_with_health_check(self):
        async def _main_wrapper():
            await gather(self._run_health_check_server(), self.main())

        try:
            run(_main_wrapper())
        except KeyboardInterrupt:
            self._shutdown_event.set()

    # WebSocket methods omitted for brevity as they are not relevant to the changes
    async def _start_websocket_manager(self):
        """Manages the WebSocket connection to the orchestrator."""
        while not self._shutdown_event.is_set():
            for orchestrator in self._config.ORCHESTRATORS:
                ws_url = orchestrator["url"].replace("http", "ws", 1) + "/_worker/ws"
                try:
                    if self._http_session:
                        async with self._http_session.ws_connect(ws_url, headers=self._get_headers(orchestrator)) as ws:
                            self._ws_connection = ws
                            logger.info(f"WebSocket connection established to {ws_url}")
                            await self._listen_for_commands()
                except (ClientError, AsyncTimeoutError) as e:
                    logger.warning(f"WebSocket connection to {ws_url} failed: {e}")
                finally:
                    self._ws_connection = None
                    logger.info(f"WebSocket connection to {ws_url} closed.")
                    await sleep(5)  # Reconnection delay
            if not self._config.ORCHESTRATORS:
                await sleep(5)

    async def _listen_for_commands(self):
        """Listens for and processes commands from the orchestrator via WebSocket."""
        if not self._ws_connection:
            return

        try:
            async for msg in self._ws_connection:
                if msg.type == WSMsgType.TEXT:
                    try:
                        command = msg.json()
                        if command.get("type") == "cancel_task":
                            task_id = command.get("task_id")
                            if task_id in self._active_tasks:
                                self._active_tasks[task_id].cancel()
                                logger.info(f"Cancelled task {task_id} by orchestrator command.")
                    except JSONDecodeError:
                        logger.warning(f"Received invalid JSON over WebSocket: {msg.data}")
                elif msg.type == WSMsgType.ERROR:
                    break
        except Exception as e:
            logger.error(f"Error in WebSocket listener: {e}")

    async def send_progress(self, task_id: str, job_id: str, progress: float, message: str = ""):
        """Sends a progress update to the orchestrator via WebSocket."""
        if self._ws_connection and not self._ws_connection.closed:
            try:
                payload = {
                    "type": "progress_update",
                    "task_id": task_id,
                    "job_id": job_id,
                    "progress": progress,
                    "message": message,
                }
                await self._ws_connection.send_json(payload)
            except Exception as e:
                logger.warning(f"Could not send progress update for task {task_id}: {e}")
