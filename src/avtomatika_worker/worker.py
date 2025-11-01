from asyncio import CancelledError, Event, Task, create_task, gather, run, sleep
from asyncio import TimeoutError as AsyncTimeoutError
from json import JSONDecodeError
from logging import getLogger
from typing import Any, Callable

from aiohttp import ClientError, ClientSession, ClientTimeout, ClientWebSocketResponse, WSMsgType, web

from .config import WorkerConfig

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
    ):
        self._config = WorkerConfig()
        self._config.worker_type = worker_type  # Allow overriding worker_type
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
        self._headers = {"X-Worker-Token": self._config.worker_token}
        self._shutdown_event = Event()
        self._registered_event = Event()
        self._round_robin_index = 0
        self._debounce_task: Task | None = None

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
        if self._current_load >= self._config.max_concurrent_tasks:
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

    async def _debounced_heartbeat_sender(self):
        """Waits for the debounce delay then sends a heartbeat."""
        await sleep(self._config.heartbeat_debounce_delay)
        await self._send_heartbeats_to_all()

    def _schedule_heartbeat_debounce(self):
        """Schedules a debounced heartbeat, cancelling any pending one."""
        # Cancel the previously scheduled task, if it exists and is not done.
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        # Schedule the new debounced call.
        self._debounce_task = create_task(self._debounced_heartbeat_sender())

    async def _poll_for_tasks(self, orchestrator_url: str):
        """Polls a specific Orchestrator for new tasks."""
        url = f"{orchestrator_url}/_worker/workers/{self._config.worker_id}/tasks/next"
        try:
            if not self._http_session:
                return
            timeout = ClientTimeout(total=self._config.task_poll_timeout + 5)
            async with self._http_session.get(url, headers=self._headers, timeout=timeout) as resp:
                if resp.status == 200:
                    task_data = await resp.json()
                    task_data["orchestrator_url"] = orchestrator_url

                    self._current_load += 1
                    task_handler_info = self._task_handlers.get(task_data["type"])
                    if task_handler_info:
                        task_type_for_limit = task_handler_info.get("type")
                        if task_type_for_limit:
                            self._current_load_by_type[task_type_for_limit] += 1
                    self._schedule_heartbeat_debounce()

                    task = create_task(self._process_task(task_data))
                    self._active_tasks[task_data["task_id"]] = task
                elif resp.status != 204:
                    await sleep(self._config.task_poll_error_delay)
        except (AsyncTimeoutError, ClientError) as e:
            logger.error(f"Error polling for tasks: {e}")
            await sleep(self._config.task_poll_error_delay)

    async def _start_polling(self):
        print("Waiting for registration")
        """The main loop for polling tasks."""
        await self._registered_event.wait()
        print("Polling started")
        while not self._shutdown_event.is_set():
            if self._get_current_state()["status"] == "busy":
                await sleep(self._config.idle_poll_delay)
                continue

            if self._config.multi_orchestrator_mode == "ROUND_ROBIN":
                orchestrator = self._config.orchestrators[self._round_robin_index]
                await self._poll_for_tasks(orchestrator["url"])
                self._round_robin_index = (self._round_robin_index + 1) % len(self._config.orchestrators)
            else:
                for orchestrator in self._config.orchestrators:
                    if self._get_current_state()["status"] == "busy":
                        break
                    await self._poll_for_tasks(orchestrator["url"])

            if self._current_load == 0:
                await sleep(self._config.idle_poll_delay)

    async def _process_task(self, task_data: dict[str, Any]):
        """Executes the task logic."""
        task_id, job_id, task_name = task_data["task_id"], task_data["job_id"], task_data["type"]
        params, orchestrator_url = task_data.get("params", {}), task_data["orchestrator_url"]

        result: dict[str, Any] = {}
        handler_data = self._task_handlers.get(task_name)
        task_type_for_limit = handler_data.get("type") if handler_data else None

        try:
            if handler_data:
                result = await handler_data["func"](
                    params,
                    task_id=task_id,
                    job_id=job_id,
                    priority=task_data.get("priority", 0),
                    send_progress=self.send_progress,
                    add_to_hot_cache=self.add_to_hot_cache,
                    remove_from_hot_cache=self.remove_from_hot_cache,
                )
            else:
                result = {"status": "failure", "error_message": f"Unsupported task: {task_name}"}
        except CancelledError:
            result = {"status": "cancelled"}
        except Exception as e:
            result = {"status": "failure", "error": {"code": "TRANSIENT_ERROR", "message": str(e)}}
        finally:
            payload = {"job_id": job_id, "task_id": task_id, "worker_id": self._config.worker_id, "result": result}
            await self._send_result(payload, orchestrator_url)
            self._active_tasks.pop(task_id, None)

            self._current_load -= 1
            if task_type_for_limit:
                self._current_load_by_type[task_type_for_limit] -= 1
            self._schedule_heartbeat_debounce()

    async def _send_result(self, payload: dict[str, Any], orchestrator_url: str):
        """Sends the result to a specific orchestrator."""
        url = f"{orchestrator_url}/_worker/tasks/result"
        delay = self._config.result_retry_initial_delay
        for i in range(self._config.result_max_retries):
            try:
                if self._http_session and not self._http_session.closed:
                    async with self._http_session.post(url, json=payload, headers=self._headers) as resp:
                        if resp.status == 200:
                            return
            except ClientError as e:
                logger.error(f"Error sending result: {e}")
            await sleep(delay * (2**i))

    async def _manage_orchestrator_communications(self):
        print("Registering worker")
        """Registers the worker and sends heartbeats."""
        await self._register_with_all_orchestrators()
        print("Worker registered")
        self._registered_event.set()
        if self._config.enable_websockets:
            create_task(self._start_websocket_manager())

        while not self._shutdown_event.is_set():
            await self._send_heartbeats_to_all()
            await sleep(self._config.heartbeat_interval)

    async def _register_with_all_orchestrators(self):
        """Registers the worker with all orchestrators."""
        state = self._get_current_state()
        payload = {
            "worker_id": self._config.worker_id,
            "worker_type": self._config.worker_type,
            "supported_tasks": state["supported_tasks"],
            "max_concurrent_tasks": self._config.max_concurrent_tasks,
            "installed_models": self._config.installed_models,
            "hostname": self._config.hostname,
            "ip_address": self._config.ip_address,
            "resources": self._config.resources,
        }
        for orchestrator in self._config.orchestrators:
            url = f"{orchestrator['url']}/_worker/workers/register"
            try:
                if self._http_session:
                    async with self._http_session.post(url, json=payload, headers=self._headers) as resp:
                        if resp.status >= 400:
                            logger.error(f"Error registering with {orchestrator['url']}: {resp.status}")
            except ClientError as e:
                logger.error(f"Error registering with orchestrator {orchestrator['url']}: {e}")

    async def _send_heartbeats_to_all(self):
        print("Sending heartbeats")
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

        async def _send_single(orchestrator_url: str):
            url = f"{orchestrator_url}/_worker/workers/{self._config.worker_id}"
            try:
                if self._http_session and not self._http_session.closed:
                    async with self._http_session.patch(url, json=payload, headers=self._headers) as resp:
                        if resp.status >= 400:
                            logger.warning(f"Heartbeat to {orchestrator_url} failed with status: {resp.status}")
            except ClientError as e:
                logger.error(f"Error sending heartbeat to orchestrator {orchestrator_url}: {e}")

        await gather(*[_send_single(o["url"]) for o in self._config.orchestrators])

    async def main(self):
        print("Main started")
        """The main asynchronous function."""
        self._validate_config()  # Validate config now that all tasks are registered
        if not self._http_session:
            self._http_session = ClientSession()
        print("Starting comm task")
        comm_task = create_task(self._manage_orchestrator_communications())
        print("Starting polling task")
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
            run(sleep(1.5))

    async def _run_health_check_server(self):
        app = web.Application()
        app.router.add_get("/health", lambda r: web.Response(text="OK"))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self._config.worker_port)
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
            run(sleep(1.5))

    # WebSocket methods omitted for brevity as they are not relevant to the changes
    async def _start_websocket_manager(self):
        """Manages the WebSocket connection to the orchestrator."""
        while not self._shutdown_event.is_set():
            for orchestrator in self._config.orchestrators:
                ws_url = orchestrator["url"].replace("http", "ws", 1) + "/_worker/ws"
                try:
                    if self._http_session:
                        async with self._http_session.ws_connect(ws_url, headers=self._headers) as ws:
                            self._ws_connection = ws
                            logger.info(f"WebSocket connection established to {ws_url}")
                            await self._listen_for_commands()
                except (ClientError, AsyncTimeoutError) as e:
                    logger.warning(f"WebSocket connection to {ws_url} failed: {e}")
                finally:
                    self._ws_connection = None
                    logger.info(f"WebSocket connection to {ws_url} closed.")
                    await sleep(5)  # Reconnection delay
            if not self._config.orchestrators:
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
