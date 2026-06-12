# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio
import contextlib

import pytest
from rxon import WorkerCommand
from rxon.models import SkillInfo, TaskPayload, TaskResult
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
    """Tests that the @worker.skill decorator correctly registers a task handler."""
    worker = Worker(worker_type="test-worker")

    @worker.skill("my_test_task")
    def my_handler(params: dict):
        return {"status": "success"}

    assert "my_test_task" in worker._skill_handlers
    assert worker._skill_handlers["my_test_task"]["func"] == my_handler
    assert worker._skill_handlers["my_test_task"]["type"] == "my_test_task"


# --- Logical Integration Tests using MockTransport ---


@pytest.mark.asyncio
async def test_worker_polls_executes_and_sends_result(monkeypatch):
    """Tests the full PULL cycle using MockTransport."""
    monkeypatch.setenv("MAX_CONCURRENT_TASKS", "1")
    monkeypatch.setenv("HEARTBEAT_INTERVAL", "10")
    monkeypatch.setenv("IDLE_POLL_DELAY", "0.01")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://test-orchestrator")
    monkeypatch.setenv("COST_PER_SKILL", '{"successful_task": 0.5}')
    monkeypatch.setenv("RAM_GB", "16.0")

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

    @worker.skill("successful_task")
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
    registration = transport.registered[0]
    assert registration.capabilities.cost_per_skill == {"successful_task": 0.5}
    assert registration.resources.properties["ram_gb"] == 16.0

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

    @worker.skill("progress_task")
    async def progress_task(params, send_progress, task_id, job_id, **kwargs):
        await send_progress(task_id, job_id, 0.5, "halfway")
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "progress_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    await worker._process_task(task_data)

    assert len(transport.emitted_events) == 1
    event = transport.emitted_events[0]
    assert event.event_type == "progress"
    assert event.payload["progress"] == 0.5
    assert event.payload["message"] == "halfway"


@pytest.mark.asyncio
async def test_hot_skills_update_and_heartbeat():
    """Tests that the hot_skills state is correctly updated and sent in the heartbeat."""
    transport = MockTransport()
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    worker.add_to_hot_skills("model-1")
    worker.add_to_hot_skills("model-2")
    worker.remove_from_hot_skills("model-1")

    # Simulate Heartbeat
    for _, client in worker._clients:
        await worker._send_single_heartbeat(client)

    assert len(transport.heartbeats) == 1
    heartbeat = transport.heartbeats[0]
    # Internal hot_cache is NOT sent anymore
    assert not hasattr(heartbeat, "hot_cache")


@pytest.mark.asyncio
async def test_heartbeat_sends_hot_skills_by_names():
    """
    Tests that the heartbeat correctly sends hot_skills as names.
    """
    skill_deps = {
        "image_generation": ["sd_v1.5", "vae"],
        "upscaling": ["realesrgan"],
    }
    transport = MockTransport()
    worker = Worker(
        skill_dependencies=skill_deps, clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)]
    )
    worker._heartbeat_cooldown = 0

    @worker.skill("image_generation")
    async def h1(params: dict):
        pass

    # Case 1: One skill fully loaded
    worker.add_to_hot_skills("sd_v1.5")
    worker.add_to_hot_skills("vae")
    # Simulate Heartbeat
    for _, client in worker._clients:
        await worker._send_single_heartbeat(client)

    heartbeat = transport.heartbeats[0]
    assert heartbeat.hot_skills == ["image_generation"]
    # Internal detail is NOT in the heartbeat
    assert not hasattr(heartbeat, "skill_dependencies")

    transport.heartbeats.clear()

    # Case 2: A model is removed, making the skill "cold"
    worker.remove_from_hot_skills("sd_v1.5")
    # Simulate Heartbeat
    for _, client in worker._clients:
        await worker._send_single_heartbeat(client)

    heartbeat = transport.heartbeats[0]
    assert heartbeat.hot_skills is None


@pytest.mark.asyncio
async def test_get_hot_skills_state():
    """Tests that get_hot_skills_state returns the current hot skills state."""
    worker = Worker()
    worker.add_to_hot_skills("model-1")
    assert worker.get_hot_skills_state() == {"model-1"}


@pytest.mark.asyncio
async def test_skill_dependencies_complex():
    """Tests complex skill dependencies with shared resources."""
    skill_deps = {
        "text_to_image": ["base_model", "refiner"],
        "image_upscale": ["refiner"],
    }
    worker = Worker(skill_dependencies=skill_deps)

    @worker.skill("text_to_image")
    async def s1(params):
        pass

    @worker.skill("image_upscale")
    async def s2(params):
        pass

    # No resources -> both cold
    state = worker._get_current_state()
    assert not any(s.name in ["text_to_image", "image_upscale"] for s in state["hot_skills"])

    # Load 'refiner' -> upscale becomes hot, t2i stays cold
    worker.add_to_hot_skills("refiner")
    state = worker._get_current_state()
    hot_names = {s.name for s in state["hot_skills"]}
    assert "image_upscale" in hot_names
    assert "text_to_image" not in hot_names

    # Load 'base_model' -> both become hot
    worker.add_to_hot_skills("base_model")
    state = worker._get_current_state()
    hot_names = {s.name for s in state["hot_skills"]}
    assert "image_upscale" in hot_names
    assert "text_to_image" in hot_names

    # Remove 'refiner' -> both become cold (cascading)
    worker.remove_from_hot_skills("refiner")
    state = worker._get_current_state()
    hot_names = {s.name for s in state["hot_skills"]}
    assert not any(n in hot_names for n in ["text_to_image", "image_upscale"])


def test_get_current_state_full():
    """Tests that _get_current_state returns 'full' when the worker is at max capacity."""
    worker = Worker()
    worker._current_load = 10
    worker._config.MAX_CONCURRENT_TASKS = 10
    state = worker._get_current_state()
    assert state["status"] == "full"
    assert state["available_skills"] == []


def test_get_current_state_idle():
    """Tests that _get_current_state returns 'idle' and a list of tasks when not busy."""
    worker = Worker()

    @worker.skill("task-1")
    def task_1(params: dict):
        pass

    @worker.skill("task-2")
    def task_2(params: dict):
        pass

    state = worker._get_current_state()
    assert state["status"] == "idle"
    # supported_skills is the full catalog
    assert sorted([s.name for s in state["supported_skills"]]) == ["task-1", "task-2"]
    # available_skills is what can be done now
    assert sorted([s.name for s in state["available_skills"]]) == ["task-1", "task-2"]


def test_get_current_state_with_skill_type_limits():
    """Tests that _get_current_state correctly filters available tasks based on type limits."""
    worker = Worker(skill_type_limits={"gpu": 1})

    @worker.skill("gpu_task_1", type="gpu")
    def gpu_task_1(params: dict):
        pass

    @worker.skill("gpu_task_2", type="gpu")
    def gpu_task_2(params: dict):
        pass

    @worker.skill("cpu_task")
    def cpu_task(params: dict):
        pass

    # No GPU tasks running, so all tasks are available
    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert sorted([s.name for s in state["available_skills"]]) == ["cpu_task", "gpu_task_1", "gpu_task_2"]

    # One GPU task is running, so no more GPU tasks can be started
    worker._current_load_by_type["gpu"] = 1
    state = worker._get_current_state()
    # supported_skills still has all tasks (it's a static catalog)
    assert sorted([s.name for s in state["supported_skills"]]) == ["cpu_task", "gpu_task_1", "gpu_task_2"]
    # available_skills only has the CPU task
    assert sorted([s.name for s in state["available_skills"]]) == ["cpu_task"]


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


def test_worker_contract_hash_consistency():
    """
    Tests that the worker's contract hash correctly changes when skills,
    costs, or extra capabilities change.
    """
    worker = Worker()

    # 1. Initial Hash
    initial_skills = [SkillInfo(name="task1")]
    hash1 = worker._calculate_contract_hash(initial_skills)

    # 2. Same input should produce same hash
    assert worker._calculate_contract_hash(initial_skills) == hash1

    # 3. Change skills -> Hash should change
    new_skills = [SkillInfo(name="task1"), SkillInfo(name="task2")]
    hash2 = worker._calculate_contract_hash(new_skills)
    assert hash1 != hash2

    # 4. Change COST_PER_SKILL -> Hash should change
    worker._config.COST_PER_SKILL = {"task1": 0.5}
    hash3 = worker._calculate_contract_hash(new_skills)
    assert hash2 != hash3

    # 5. Change EXTRA_CAPABILITIES -> Hash should change
    worker._config.EXTRA_CAPABILITIES = {"region": "us-east-1"}
    hash4 = worker._calculate_contract_hash(new_skills)
    assert hash3 != hash4

    # 6. Reverting change should restore hash (determinism)
    worker._config.EXTRA_CAPABILITIES = {}
    assert worker._calculate_contract_hash(new_skills) == hash3


@pytest.mark.asyncio
async def test_worker_custom_usage_checker():
    """Tests that a custom usage checker is correctly used in heartbeats."""
    from rxon.models import ResourcesUsage

    transport = MockTransport()
    worker = Worker(clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    mock_usage = ResourcesUsage(cpu_load_percent=88.5, ram_used_gb=12.2, devices_usage=None)

    def my_checker():
        return mock_usage

    worker.set_usage_checker(my_checker)
    # Simulate Heartbeat
    for _, client in worker._clients:
        await worker._send_single_heartbeat(client)

    assert len(transport.heartbeats) == 1
    hb = transport.heartbeats[0]
    assert hb.usage.cpu_load_percent == 88.5
    assert hb.usage.ram_used_gb == 12.2


def test_worker_hash_sync_between_registration_and_heartbeat():
    """
    Ensures that registration and heartbeat use the same hash for the same state.
    """
    worker = Worker()

    @worker.skill("test_skill")
    def handler(params):
        return {"status": "success"}

    state = worker._get_current_state()
    current_skills = state["supported_skills"]

    # Hash calculation used internally
    expected_hash = worker._calculate_contract_hash(current_skills)

    # Simulation: Registration logic
    # (Inside _register_with_all_orchestrators, it calls _calculate_contract_hash)
    reg_hash = worker._calculate_contract_hash(current_skills)

    # Simulation: Heartbeat logic
    # (It calls _calculate_contract_hash inside _send_single_heartbeat)
    hb_hash = worker._calculate_contract_hash(current_skills)

    assert reg_hash == hb_hash == expected_hash
