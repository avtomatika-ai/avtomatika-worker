# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio
import sys
from unittest.mock import AsyncMock, patch

import pytest
from rxon.models import SkillInfo, WorkerCommand
from rxon.testing import MockTransport

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_skill_docstring_and_version_extraction():
    """Checks that description and version are extracted from docstring and module."""
    worker = Worker()
    from types import ModuleType

    mock_mod = ModuleType("mock_mod")
    mock_mod.__version__ = "1.2.3"
    sys.modules["mock_mod"] = mock_mod

    @worker.skill("test_doc_skill")
    async def my_handler(params: dict):
        """
        This is a test skill description.
        It should end up in SkillInfo.
        """
        return {"status": "success"}

    my_handler.__module__ = "mock_mod"
    decorator = worker.skill("test_doc_skill")
    decorator(my_handler)

    skill_info = worker._skill_handlers["test_doc_skill"]["info"]
    assert "This is a test skill description" in skill_info.description
    assert skill_info.version == "1.2.3"
    del sys.modules["mock_mod"]


@pytest.mark.asyncio
async def test_three_tier_skill_lists_in_heartbeat():
    """Checks that correct skill list types are sent in heartbeat."""
    worker = Worker()
    worker.add_to_hot_skills("hot_task")

    @worker.skill("hot_task")
    async def hot_task(params: dict):
        pass

    @worker.skill("cold_task")
    async def cold_task(params: dict):
        pass

    payload = worker._create_heartbeat_payload()
    assert isinstance(payload.supported_skills[0], SkillInfo)
    assert "hot_task" in payload.available_skills
    assert payload.hot_skills == ["hot_task"]


@pytest.mark.asyncio
async def test_contract_hash_stability_under_load():
    """Checks that the catalog hash doesn't change when current load changes."""
    worker = Worker()

    @worker.skill("task1")
    async def h1(p):
        pass

    initial_payload = worker._create_heartbeat_payload()
    initial_hash = initial_payload.skills_hash
    assert initial_payload.supported_skills is not None  # First time we send everything

    # Simulate load
    worker._current_load = 100
    worker._last_synced_skills_hash = initial_hash

    load_payload = worker._create_heartbeat_payload()
    assert load_payload.skills_hash == initial_hash  # Hash must be the SAME
    assert load_payload.supported_skills is None  # Full list should NOT be re-sent
    assert load_payload.available_skills is None  # None returned for bandwidth optimization when empty


@pytest.mark.asyncio
async def test_task_result_holarchy_provenance():
    """Checks that task result preserves and transmits origin_worker_id."""
    transport = MockTransport()
    transport.connected = True
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    @worker.skill("test_skill")
    async def handler(params: dict):
        return {"status": "success", "data": {"ok": True}}

    # Task came from an external source
    task_payload = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "test_skill",
        "params": {},
        "origin_worker_id": "real-initiator-node",
        "client": transport,
    }
    await worker._process_task(task_payload)

    result = transport.results[0]
    assert result.origin_worker_id == "real-initiator-node"
    assert result.worker_id == worker._config.WORKER_ID


@pytest.mark.asyncio
async def test_s3_cleanup_on_task_completion():
    """Checks that worker calls S3 cleanup after task completion."""
    transport = MockTransport()
    transport.connected = True
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    @worker.skill("s3_skill")
    async def handler(params: dict):
        return {"status": "success"}

    with patch.object(worker._s3_manager, "cleanup", new_callable=AsyncMock) as mock_cleanup:
        await worker._process_task(
            {"job_id": "j1", "task_id": "task-abc", "type": "s3_skill", "params": {}, "client": transport}
        )
        mock_cleanup.assert_awaited_once_with("task-abc")


@pytest.mark.asyncio
async def test_custom_command_routing():
    """Checks custom command routing."""
    transport = MockTransport()
    transport.connected = True
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    received_params = None

    @worker.on_command("reboot")
    async def handle_reboot(command):
        nonlocal received_params
        received_params = command.params

    listener_task = asyncio.create_task(worker._listen_to_single_transport(transport))
    transport.push_command(WorkerCommand(command="reboot", params={"force": True}))

    for _ in range(10):
        if received_params:
            break
        await asyncio.sleep(0.1)

    assert received_params == {"force": True}
    listener_task.cancel()


@pytest.mark.asyncio
async def test_event_origin_relay():
    """Checks origin_worker_id relay in events."""
    transport = MockTransport()
    transport.connected = True
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    @worker.skill("relay")
    async def relay_handler(send_event, **kwargs):
        await send_event("data", {"v": 1}, origin_worker_id="source-node")
        return {"status": "success"}

    await worker._process_task({"job_id": "j1", "task_id": "t1", "type": "relay", "params": {}, "client": transport})
    assert transport.emitted_events[0].origin_worker_id == "source-node"
