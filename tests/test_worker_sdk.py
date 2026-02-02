import asyncio
import contextlib

import pytest
from rxon import WorkerCommand
from rxon.models import TaskPayload, TaskResult
from rxon.testing import MockTransport

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


# --- Logical Integration Tests using MockTransport ---


@pytest.mark.asyncio
async def test_worker_polls_executes_and_sends_result(monkeypatch):
    """Tests the full PULL cycle using MockTransport."""
    monkeypatch.setenv("MAX_CONCURRENT_TASKS", "1")
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "10")
    monkeypatch.setenv("IDLE_POLL_DELAY", "0.01")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator")
    monkeypatch.setenv("COST_PER_SKILL", '{"successful_task": 0.5}')

    transport = MockTransport(worker_id="test-worker")

    # Pre-inject a task into the mock transport
    transport.push_task(
        TaskPayload(
            job_id="job-123", task_id="task-456", type="successful_task", params={"input": "test"}, tracing_context={}
        )
    )

    worker = Worker(
        worker_type="integration-test-worker", clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)]
    )

    @worker.task("successful_task")
    async def successful_handler(params: dict, **kwargs):
        return {"status": "success", "data": {"output": "ok"}}

    # Run the worker main loop in background
    worker_task = asyncio.create_task(worker.main())

    # Wait for task to be processed
    for _ in range(20):
        if transport.results:
            break
        await asyncio.sleep(0.05)

    worker._shutdown_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(worker_task, timeout=1.0)

    # --- Assertions ---
    assert len(transport.registered) > 0
    assert transport.registered[0].capabilities.cost_per_skill == {"successful_task": 0.5}

    assert len(transport.results) == 1
    result: TaskResult = transport.results[0]
    assert result.task_id == "task-456"
    assert result.status == "success"
    assert result.data == {"output": "ok"}


@pytest.mark.asyncio
async def test_listen_for_commands_cancels_task():
    """Tests that commands from transport correctly cancel a task."""
    transport = MockTransport(worker_id="test-worker")
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    task_id = "task-to-cancel"
    mock_task = asyncio.create_task(asyncio.sleep(10))
    worker._active_tasks[task_id] = mock_task

    # Start the command listener
    await transport.connect()
    worker._shutdown_event = asyncio.Event()  # Ensure event is clean
    listener_task = asyncio.create_task(worker._listen_to_single_transport(transport))

    # Inject cancellation command
    transport.push_command(WorkerCommand(command="cancel_task", task_id=task_id, job_id="job-123"))

    await asyncio.sleep(0.1)

    assert mock_task.cancelled()

    worker._shutdown_event.set()
    listener_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await listener_task


@pytest.mark.asyncio
async def test_send_progress():
    """Tests that send_progress sends a progress update via Transport."""
    transport = MockTransport()
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    @worker.task("progress_task")
    async def progress_task(params, send_progress, task_id, job_id, **kwargs):
        await send_progress(task_id, job_id, 0.5, "halfway")

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "progress_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    await worker._process_task(task_data)

    assert len(transport.progress_updates) == 1
    update = transport.progress_updates[0]
    assert update.progress == 0.5
    assert update.message == "halfway"


@pytest.mark.asyncio
async def test_hot_cache_update_and_heartbeat():
    """Tests that the hot_cache is correctly updated and sent in the heartbeat."""
    transport = MockTransport()
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    worker.add_to_hot_cache("model-1")
    worker.add_to_hot_cache("model-2")
    worker.remove_from_hot_cache("model-1")

    await worker._send_heartbeats_to_all()

    assert len(transport.heartbeats) == 1
    heartbeat = transport.heartbeats[0]
    assert sorted(heartbeat.hot_cache) == ["model-2"]


@pytest.mark.asyncio
async def test_heartbeat_sends_skill_dependencies_and_hot_skills():
    """
    Tests that the heartbeat correctly sends skill_dependencies and hot_skills.
    """
    skill_deps = {
        "image_generation": ["sd_v1.5", "vae"],
        "upscaling": ["realesrgan"],
    }
    transport = MockTransport()
    worker = Worker(
        skill_dependencies=skill_deps, clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)]
    )

    # Case 1: One skill fully loaded
    worker.add_to_hot_cache("sd_v1.5")
    worker.add_to_hot_cache("vae")
    await worker._send_heartbeats_to_all()

    heartbeat = transport.heartbeats[0]
    assert heartbeat.skill_dependencies == skill_deps
    assert sorted(heartbeat.hot_skills) == ["image_generation"]

    transport.heartbeats.clear()

    # Case 2: A model is removed, making the skill "cold"
    worker.remove_from_hot_cache("sd_v1.5")
    await worker._send_heartbeats_to_all()

    heartbeat = transport.heartbeats[0]
    assert heartbeat.hot_skills is None


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
async def test_run_and_shutdown():
    """Tests that the worker can start, run, and shut down gracefully."""
    transport = MockTransport()
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    run_task = asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)

    assert transport.connected
    assert len(transport.registered) == 1

    worker._shutdown_event.set()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(run_task, timeout=1.0)

    assert not transport.connected
