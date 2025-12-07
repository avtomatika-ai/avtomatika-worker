import asyncio
import contextlib

import aiohttp
import pytest

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


def test_worker_config_loads_from_env(monkeypatch):
    """Tests that WorkerConfig correctly loads values from environment variables."""
    monkeypatch.setenv("WORKER_ID", "test-worker-from-env")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator-from-env")
    monkeypatch.setenv("MAX_CONCURRENT_TASKS", "5")

    config = WorkerConfig()

    assert config.WORKER_ID == "test-worker-from-env"
    assert config.ORCHESTRATORS[0]["url"] == "http://test-orchestrator-from-env"
    assert config.MAX_CONCURRENT_TASKS == 5


def test_task_registration():
    """Tests that the @worker.task decorator correctly registers a task handler."""
    worker = Worker(worker_type="test-worker")

    @worker.task("my_test_task")
    def my_handler(params: dict):
        return {"status": "success"}

    assert "my_test_task" in worker._task_handlers
    assert worker._task_handlers["my_test_task"]["func"] == my_handler
    assert worker._task_handlers["my_test_task"]["type"] is None


# --- Integration Tests ---


@pytest.fixture
def test_worker():
    """Provides a Worker instance for integration tests."""
    worker = Worker(worker_type="integration-test-worker")

    @worker.task("successful_task")
    async def successful_handler(params: dict):
        await asyncio.sleep(0.01)
        return {"status": "success", "data": {"result": "it worked"}}

    return worker


@pytest.mark.asyncio
async def test_worker_polls_executes_and_sends_result(mocker, monkeypatch):
    """Tests the full PULL cycle using the correct pytest-mock pattern for aiohttp."""
    monkeypatch.setenv("MAX_CONCURRENT_TASKS", "1")
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "10")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator")
    monkeypatch.setenv("COST_PER_SKILL", '{"successful_task": 0.5}')

    task_payload = {
        "job_id": "job-123",
        "task_id": "task-456",
        "type": "successful_task",
        "params": {"input": "test"},
    }

    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    # Mock for GET (polling for tasks)
    get_response_success = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    get_response_success.status = 200
    get_response_success.json = mocker.AsyncMock(return_value=task_payload)
    get_response_success.__aenter__.return_value = get_response_success

    get_response_no_task = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    get_response_no_task.status = 204
    get_response_no_task.__aenter__.return_value = get_response_no_task

    session.get = mocker.MagicMock(
        side_effect=[get_response_success, get_response_no_task]  # First call gets a task, subsequent get no task
    )

    # Mock for POST (registration and result sending)
    post_response_success = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    post_response_success.status = 200
    post_response_success.__aenter__.return_value = post_response_success

    # Set side_effect for session.post to return different responses
    # First POST is registration, second is task result
    session.post = mocker.MagicMock(side_effect=[post_response_success, post_response_success])

    # Mock for PATCH (heartbeat)
    patch_response = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    patch_response.status = 200
    patch_response.__aenter__.return_value = patch_response
    session.patch = mocker.MagicMock(return_value=patch_response)

    worker = Worker(worker_type="integration-test-worker", http_session=session)

    @worker.task("successful_task")
    async def successful_handler(params: dict, **kwargs):
        return {"status": "success"}

    worker_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.5)
    worker._shutdown_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(worker_task, timeout=1.0)

    # --- Assertions ---
    # Assert registration, polling, and result sending were called
    assert session.get.call_count > 0

    # Check that POST was called for registration and for sending result
    # The order can vary, so we check using 'any'
    assert any("register" in call.args[0] for call in session.post.call_args_list)
    assert any("result" in call.args[0] for call in session.post.call_args_list)

    # Find the registration call and check the payload
    for call in session.post.call_args_list:
        if "register" in call.args[0]:
            assert call.kwargs["json"]["cost_per_skill"] == {"successful_task": 0.5}
            break
    else:
        pytest.fail("Registration call not found")


@pytest.mark.asyncio
async def test_listen_for_commands_cancels_task(mocker):
    """Tests that _listen_for_commands correctly cancels a task."""
    worker = Worker()
    task_id = "task-to-cancel"
    mock_task = mocker.MagicMock()
    worker._active_tasks[task_id] = mock_task

    mock_ws = mocker.AsyncMock()
    cancel_message = mocker.MagicMock()
    cancel_message.type = aiohttp.WSMsgType.TEXT
    cancel_message.json.return_value = {"type": "cancel_task", "task_id": task_id}
    mock_ws.__aiter__.return_value = [cancel_message]
    worker._ws_connection = mock_ws

    await worker._listen_for_commands()

    mock_task.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_send_progress(mocker):
    """Tests that send_progress sends a progress update via WebSocket."""
    worker = Worker()
    mock_ws = mocker.AsyncMock()
    mock_ws.closed = False
    mock_ws.send_json = mocker.AsyncMock()
    worker._ws_connection = mock_ws

    task_id = "task-123"
    job_id = "job-456"
    await worker.send_progress(task_id, job_id, 0.5, "in progress")

    mock_ws.send_json.assert_called_once_with(
        {
            "type": "progress_update",
            "task_id": task_id,
            "job_id": job_id,
            "progress": 0.5,
            "message": "in progress",
        }
    )


@pytest.mark.asyncio
async def test_hot_cache_update_and_heartbeat(mocker, monkeypatch):
    """Tests that the hot_cache is correctly updated and sent in the heartbeat."""
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "0.01")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator")

    # Correctly mock the session and its patch method
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False
    patch_response = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    patch_response.status = 200
    patch_response.__aenter__.return_value = patch_response
    session.patch = mocker.MagicMock(return_value=patch_response)

    worker = Worker(http_session=session)

    worker.add_to_hot_cache("model-1")
    worker.add_to_hot_cache("model-2")
    worker.remove_from_hot_cache("model-1")

    await worker._send_heartbeats_to_all()

    # Assert that the patch method was called with the correct hot_cache
    session.patch.assert_called_once()
    payload = session.patch.call_args.kwargs["json"]
    assert sorted(payload["hot_cache"]) == ["model-2"]


@pytest.mark.asyncio
async def test_heartbeat_sends_skill_dependencies_and_hot_skills(mocker, monkeypatch):
    """
    Tests that the heartbeat correctly sends skill_dependencies and dynamically
    calculates and sends hot_skills based on the current hot_cache.
    """
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "0.01")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator")

    skill_deps = {
        "image_generation": ["sd_v1.5", "vae"],
        "upscaling": ["realesrgan"],
    }

    # Correctly mock the session and its patch method
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False
    patch_response = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    patch_response.status = 200
    patch_response.__aenter__.return_value = patch_response
    session.patch = mocker.MagicMock(return_value=patch_response)

    worker = Worker(skill_dependencies=skill_deps, http_session=session)

    # Case 1: One skill fully loaded
    worker.add_to_hot_cache("sd_v1.5")
    worker.add_to_hot_cache("vae")
    await worker._send_heartbeats_to_all()

    session.patch.assert_called_once()
    payload = session.patch.call_args.kwargs["json"]
    assert payload["skill_dependencies"] == skill_deps
    assert sorted(payload["hot_skills"]) == ["image_generation"]
    session.patch.reset_mock()

    # Case 2: A model is removed, making the skill "cold"
    worker.remove_from_hot_cache("sd_v1.5")
    await worker._send_heartbeats_to_all()

    session.patch.assert_called_once()
    payload = session.patch.call_args.kwargs["json"]
    assert "hot_skills" not in payload


@pytest.mark.asyncio
async def test_get_hot_cache():
    """Tests that get_hot_cache returns the current hot cache."""
    worker = Worker()
    worker.add_to_hot_cache("model-1")
    assert worker.get_hot_cache() == {"model-1"}
    # Clean up the debounced task
    if worker._debounce_task:
        worker._debounce_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker._debounce_task


def test_get_current_state_busy():
    """Tests that _get_current_state returns 'busy' when the worker is at max capacity."""
    worker = Worker()
    worker._current_load = 10
    worker._config.MAX_CONCURRENT_TASKS = 10
    state = worker._get_current_state()
    assert state["status"] == "busy"
    assert state["supported_tasks"] == []


def test_get_current_state_idle():
    """Tests that _get_current_state returns 'idle' and a list of tasks when not busy."""
    worker = Worker()

    @worker.task("task-1")
    def task_1(params: dict):
        pass

    @worker.task("task-2")
    def task_2(params: dict):
        pass

    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert sorted(state["supported_tasks"]) == ["task-1", "task-2"]


def test_get_current_state_with_task_type_limits():
    """Tests that _get_current_state correctly filters tasks based on type limits."""
    worker = Worker(task_type_limits={"gpu": 1})

    @worker.task("gpu_task_1", task_type="gpu")
    def gpu_task_1(params: dict):
        pass

    @worker.task("gpu_task_2", task_type="gpu")
    def gpu_task_2(params: dict):
        pass

    @worker.task("cpu_task")
    def cpu_task(params: dict):
        pass

    # No GPU tasks running, so all tasks are available
    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert sorted(state["supported_tasks"]) == ["cpu_task", "gpu_task_1", "gpu_task_2"]

    # One GPU task is running, so no more GPU tasks can be started
    worker._current_load_by_type["gpu"] = 1
    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert state["supported_tasks"] == ["cpu_task"]


@pytest.mark.asyncio
async def test_run_and_shutdown(mocker):
    """Tests that the worker can start, run, and shut down gracefully."""
    # This test is more about lifecycle than network calls, but we need
    # to provide valid mocks for the calls that happen on startup.
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    # Setup for POST (register)
    post_cm = mocker.AsyncMock()
    post_cm.__aenter__.return_value.status = 200
    session.post.return_value = post_cm

    # Setup for GET (poll)
    get_cm = mocker.AsyncMock()
    get_cm.__aenter__.return_value.status = 204  # No tasks
    session.get.return_value = get_cm

    # Setup for PATCH (heartbeat)
    patch_cm = mocker.AsyncMock()
    patch_cm.__aenter__.return_value.status = 200
    session.patch.return_value = patch_cm

    worker = Worker(http_session=session)
    run_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)  # Give worker time to start up
    worker._shutdown_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(run_task, timeout=1.0)


@pytest.mark.asyncio
async def test_send_result_success(mocker):
    """Tests that _send_result sends a successful result to the orchestrator."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    post_cm = mocker.AsyncMock()
    post_cm.__aenter__.return_value.status = 200
    session.post.return_value = post_cm

    worker = Worker(http_session=session)
    payload = {
        "job_id": "job-1",
        "task_id": "task-1",
        "worker_id": worker._config.WORKER_ID,
        "result": {"status": "success"},
    }
    await worker._send_result(payload, {"url": "http://test-orchestrator"})

    session.post.assert_called_once()
    sent_payload = session.post.call_args.kwargs["json"]
    assert sent_payload["result"]["status"] == "success"


@pytest.mark.asyncio
async def test_send_result_failure(mocker):
    """Tests that _send_result retries on failure and then raises an exception."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    # side_effect function to produce a new context manager mock on each call
    def mock_failed_post(*args, **kwargs):
        cm = mocker.AsyncMock()
        cm.__aenter__.return_value.status = 500
        return cm

    session.post.side_effect = mock_failed_post

    worker = Worker(http_session=session)
    worker._config.RESULT_MAX_RETRIES = 2
    worker._config.RESULT_RETRY_INITIAL_DELAY = 0.01

    payload = {
        "job_id": "job-1",
        "task_id": "task-1",
        "worker_id": worker._config.WORKER_ID,
        "result": {"status": "failed"},
    }
    await worker._send_result(payload, {"url": "http://test-orchestrator"})

    assert session.post.call_count == 2
