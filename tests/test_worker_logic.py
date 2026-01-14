import asyncio
from unittest.mock import MagicMock

import pytest

from avtomatika_worker.client import OrchestratorClient
from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_validate_config_warns_on_unused_task_type_limits(mocker):
    """
    Tests that _validate_config warns when a task type limit is defined
    but no task with that type is registered.
    """
    logger_mock = mocker.patch("avtomatika_worker.worker.logger.warning")
    worker = Worker(task_type_limits={"gpu": 1})

    @worker.task("cpu_task")
    def cpu_task(params: dict):
        pass

    worker._validate_task_types()

    logger_mock.assert_called_once_with(
        "Configuration warning: A limit is defined for task type 'gpu', but no tasks are registered with this type."
    )


@pytest.mark.asyncio
async def test_debounced_heartbeat_sender(mocker):
    """
    Tests that _debounced_heartbeat_sender waits for the correct delay
    and then sends a heartbeat.
    """
    worker = Worker()
    worker._config.HEARTBEAT_DEBOUNCE_DELAY = 0.01
    mock_send_heartbeats = mocker.patch.object(worker, "_send_heartbeats_to_all", new_callable=mocker.AsyncMock)
    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)

    await worker._debounced_heartbeat_sender()

    mock_sleep.assert_called_once_with(0.01)
    mock_send_heartbeats.assert_called_once()


@pytest.mark.asyncio
async def test_schedule_heartbeat_debounce(mocker):
    """
    Tests that _schedule_heartbeat_debounce cancels a pending task and schedules a new one.
    """
    worker = Worker()
    mock_create_task = mocker.patch("avtomatika_worker.worker.create_task")

    # First call, no pending task
    worker._schedule_heartbeat_debounce()
    mock_create_task.assert_called_once()
    mock_create_task.call_args[0][0].close()  # Avoid RuntimeWarning

    # Second call, with a pending task
    pending_task = MagicMock()
    pending_task.done.return_value = False
    worker._debounce_task = pending_task
    worker._schedule_heartbeat_debounce()

    pending_task.cancel.assert_called_once()
    assert mock_create_task.call_count == 2
    mock_create_task.call_args[0][0].close()  # Avoid RuntimeWarning


@pytest.mark.asyncio
async def test_poll_for_tasks_receives_task(mocker):
    """Tests that _poll_for_tasks correctly processes a received task."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    task_payload = {
        "job_id": "job-123",
        "task_id": "task-456",
        "type": "successful_task",
        "params": {"input": "test"},
    }
    client.poll_task.return_value = task_payload

    worker = Worker()
    mocker.patch.object(worker, "_schedule_heartbeat_debounce")
    worker._process_task = mocker.AsyncMock()

    await worker._poll_for_tasks(client)

    client.poll_task.assert_called_once_with(timeout=worker._config.TASK_POLL_TIMEOUT)
    worker._process_task.assert_called_once()
    # verify client was injected
    assert worker._process_task.call_args[0][0]["client"] == client


@pytest.mark.asyncio
async def test_poll_for_tasks_no_task(mocker):
    """Tests that _poll_for_tasks handles no task correctly."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    client.poll_task.return_value = None

    worker = Worker()
    mocker.patch.object(worker, "_schedule_heartbeat_debounce")
    worker._process_task = mocker.AsyncMock()

    await worker._poll_for_tasks(client)

    client.poll_task.assert_called_once()
    worker._process_task.assert_not_called()


@pytest.mark.asyncio
async def test_start_polling_busy(mocker):
    """Tests that _start_polling waits when the worker is busy."""
    worker = Worker()
    worker._get_current_state = mocker.MagicMock(return_value={"status": "busy"})
    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)
    mock_sleep.side_effect = asyncio.CancelledError  # To break the loop
    worker._registered_event.set()

    with pytest.raises(asyncio.CancelledError):
        await worker._start_polling()

    mock_sleep.assert_called_once_with(worker._config.IDLE_POLL_DELAY)


@pytest.mark.asyncio
async def test_start_polling_round_robin(mocker):
    """Tests that _start_polling uses round-robin to select orchestrators."""
    worker = Worker()
    worker._config.MULTI_ORCHESTRATOR_MODE = "ROUND_ROBIN"

    # Setup mock clients
    client1 = mocker.AsyncMock(spec=OrchestratorClient)
    client2 = mocker.AsyncMock(spec=OrchestratorClient)

    worker._clients = [
        ({"url": "http://test-1", "weight": 1, "current_weight": 0}, client1),
        ({"url": "http://test-2", "weight": 1, "current_weight": 0}, client2),
    ]
    worker._total_orchestrator_weight = 2
    worker._get_current_state = mocker.MagicMock(return_value={"status": "idle"})

    # We only want to test one iteration of the polling loop
    async def poll_side_effect(*args, **kwargs):
        worker._shutdown_event.set()

    worker._poll_for_tasks = mocker.AsyncMock(side_effect=poll_side_effect)
    worker._registered_event.set()

    await worker._start_polling()

    # With equal weights, the first one should be chosen
    worker._poll_for_tasks.assert_called_once_with(client1)


@pytest.mark.asyncio
async def test_process_task_exception(mocker):
    """Tests that _process_task handles exceptions in the task handler."""
    worker = Worker()

    client = mocker.AsyncMock(spec=OrchestratorClient)

    @worker.task("failing_task")
    def failing_task(params: dict, **kwargs):
        raise ValueError("Task failed")

    task_data = {
        "job_id": "job-1",
        "task_id": "task-1",
        "type": "failing_task",
        "params": {},
        "client": client,
        "orchestrator": {"url": "http://test-orchestrator"},
    }

    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    payload = client.send_result.call_args.args[0]
    assert payload["result"]["status"] == "failure"
    assert payload["result"]["error"]["code"] == "TRANSIENT_ERROR"
    assert payload["result"]["error"]["message"] == "Task failed"


@pytest.mark.asyncio
async def test_process_unsupported_task(mocker):
    """Tests that _process_task handles unsupported tasks correctly."""
    worker = Worker()
    client = mocker.AsyncMock(spec=OrchestratorClient)

    task_data = {
        "job_id": "job-1",
        "task_id": "task-1",
        "type": "unsupported_task",
        "params": {},
        "client": client,
        "orchestrator": {"url": "http://test-orchestrator"},
    }

    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    payload = client.send_result.call_args.args[0]
    assert payload["result"]["status"] == "failure"
    assert payload["result"]["error"]["message"] == "Unsupported task: unsupported_task"


@pytest.mark.asyncio
async def test_process_task_cancelled(mocker):
    """Tests that _process_task handles cancelled tasks correctly."""
    worker = Worker()
    client = mocker.AsyncMock(spec=OrchestratorClient)

    @worker.task("cancellable_task")
    async def cancellable_task(params: dict, **kwargs):
        raise asyncio.CancelledError

    task_data = {
        "job_id": "job-1",
        "task_id": "task-1",
        "type": "cancellable_task",
        "params": {},
        "client": client,
        "orchestrator": {"url": "http://test-orchestrator"},
    }

    with pytest.raises(asyncio.CancelledError):
        await worker._process_task(task_data)

    client.send_result.assert_called_once()
    payload = client.send_result.call_args.args[0]
    assert payload["result"]["status"] == "cancelled"


@pytest.mark.asyncio
async def test_run_with_health_check(mocker):
    """
    Tests that run_with_health_check runs the health check server and the main worker loop.
    """
    worker = Worker()
    mock_run = mocker.patch("avtomatika_worker.worker.run")
    mock_gather = mocker.patch("avtomatika_worker.worker.gather", new_callable=mocker.AsyncMock)
    worker._run_health_check_server = mocker.AsyncMock()
    worker.main = mocker.AsyncMock()

    # Create a separate async function to call the method
    async def run_test():
        worker.run_with_health_check()

    # Run the test function
    await run_test()

    # Check that asyncio.run was called
    mock_run.assert_called_once()

    # To check the inner calls, we need to get the coroutine passed to asyncio.run
    # This is a bit tricky, but we can inspect the call arguments
    wrapper_coro = mock_run.call_args[0][0]
    await wrapper_coro

    # Now check that gather was called with the correct coroutines
    mock_gather.assert_called_once()
    worker._run_health_check_server.assert_called_once()
    worker.main.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore:coroutine 'AsyncMockMixin._execute_mock_call' was never awaited")
async def test_run_health_check_server(mocker):
    """
    Tests that _run_health_check_server starts and stops the aiohttp server.
    """
    worker = Worker()

    # Explicitly mock classes and their return values for better control
    mock_app_instance = mocker.MagicMock()
    mock_app_class = mocker.patch("aiohttp.web.Application", return_value=mock_app_instance)

    mock_runner_instance = mocker.MagicMock()
    mock_runner_instance.setup = mocker.AsyncMock()
    mock_runner_instance.cleanup = mocker.AsyncMock()
    mock_runner_class = mocker.patch("aiohttp.web.AppRunner", return_value=mock_runner_instance)

    mock_site_instance = mocker.MagicMock()
    mock_site_instance.start = mocker.AsyncMock()
    mock_site_class = mocker.patch("aiohttp.web.TCPSite", return_value=mock_site_instance)

    # Set the shutdown event to stop the server
    async def wait_side_effect(*args, **kwargs):
        worker._shutdown_event.set()

    worker._shutdown_event.wait = mocker.AsyncMock(side_effect=wait_side_effect)

    await worker._run_health_check_server()

    mock_app_class.assert_called_once_with()  # Application()
    mock_app_instance.router.add_get.assert_called_once_with(
        "/health", mocker.ANY
    )  # Assuming internal call to app.router.add_get

    mock_runner_class.assert_called_once_with(mock_app_instance)  # AppRunner(app)
    mock_runner_instance.setup.assert_called_once()
    mock_runner_instance.cleanup.assert_called_once()

    mock_site_class.assert_called_once_with(
        mock_runner_instance, "0.0.0.0", worker._config.WORKER_PORT
    )  # TCPSite(runner, ...)
    mock_site_instance.start.assert_called_once()
