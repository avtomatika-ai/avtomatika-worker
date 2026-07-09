# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from rxon.models import ResourcesUsage
from rxon.testing import MockTransport

from avtomatika_worker.s3 import S3Manager
from avtomatika_worker.worker import OrchestratorClient, Worker


@pytest.mark.asyncio
async def test_telemetry_throttling():
    """Tests that resources usage is throttled using deadband logic."""
    worker = Worker()
    worker._telemetry_deadband = 5.0  # 5%
    worker._telemetry_force_interval = 60.0

    # Mock resource retrieval
    usage1 = ResourcesUsage(cpu_load_percent=10.0, ram_used_gb=1.0)
    usage2 = ResourcesUsage(cpu_load_percent=12.0, ram_used_gb=1.02)  # Diff is 2% CPU and 2% RAM
    usage3 = ResourcesUsage(cpu_load_percent=20.0, ram_used_gb=1.0)  # Diff is 8% CPU

    with patch.object(worker, "_get_resources_usage", side_effect=[usage1, usage2, usage3]):
        # 1. First heartbeat - should include usage
        hb1 = worker._create_heartbeat_payload()
        assert hb1.usage == usage1
        assert worker._last_sent_usage == usage1

        # 2. Second heartbeat (small change) - usage should be None (throttled)
        hb2 = worker._create_heartbeat_payload()
        assert hb2.usage is None
        assert worker._last_sent_usage == usage1

        # 3. Third heartbeat (large CPU change > 5%) - should include usage
        hb3 = worker._create_heartbeat_payload()
        assert hb3.usage == usage3
        assert worker._last_sent_usage == usage3


@pytest.mark.asyncio
async def test_etag_blob_caching(tmp_path):
    """Tests that S3 downloads are cached locally based on ETag."""
    from avtomatika_worker.config import WorkerConfig

    config = WorkerConfig()
    config.S3_ENDPOINT_URL = "http://mock-s3"
    config.WORKER_BLOB_CACHE_DIR = str(tmp_path / "cache")
    config.TASK_FILES_DIR = str(tmp_path / "payloads")

    s3_manager = S3Manager(config)

    # Mock provider functions
    s3_manager._provider.get_metadata = AsyncMock(return_value={"etag": "etag-12345"})

    async def side_effect(uri, local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w") as f:
            f.write("cached_data")

    s3_manager._provider.download = AsyncMock(side_effect=side_effect)

    # Call process_params (first time - cache miss)
    params = {"file": "s3://my-bucket/model.bin"}
    processed1 = await s3_manager.process_params(params, task_id="t1")

    local_path = processed1["file"]
    # Check that provider.download was called with cache path, and symlink was created
    s3_manager._provider.download.assert_called_once_with(
        "s3://my-bucket/model.bin", os.path.join(config.WORKER_BLOB_CACHE_DIR, "etag-12345")
    )
    assert os.path.islink(local_path)
    assert os.readlink(local_path) == os.path.join(config.WORKER_BLOB_CACHE_DIR, "etag-12345")

    # Reset download mock
    s3_manager._provider.download.reset_mock()

    # Call process_params again (second time - cache hit)
    processed2 = await s3_manager.process_params(params, task_id="t2")
    local_path2 = processed2["file"]

    # Download should NOT be called again, but symlink should be created
    s3_manager._provider.download.assert_not_called()
    assert os.path.islink(local_path2)
    assert os.readlink(local_path2) == os.path.join(config.WORKER_BLOB_CACHE_DIR, "etag-12345")


@pytest.mark.asyncio
async def test_result_queue_uploader():
    """Tests that task results are uploaded asynchronously via asyncio.Queue."""
    worker = Worker()
    transport = AsyncMock(spec=MockTransport)

    # Process task and queue the result

    @worker.skill("test-skill")
    async def handler(params):
        return {"data": "test_ok"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "test-skill",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    # Start result uploader worker task
    uploader_task = asyncio.create_task(worker._result_queue_worker())

    await worker._process_task(task_data)

    # Task should be processed immediately and queued
    assert worker._result_queue.qsize() == 1 or transport.send_result.called

    # Let the uploader process the queue
    await asyncio.sleep(0.1)

    transport.send_result.assert_called_once()
    uploader_task.cancel()


@pytest.mark.asyncio
async def test_orchestrator_client_injection():
    """Tests that OrchestratorClient is injected successfully and can be invoked."""
    worker = Worker()
    from rxon.transports.http import HttpTransport

    class DummyHttpTransport(HttpTransport):
        def __init__(self):
            self.base_url = "http://mock-orchestrator"
            self._headers = {}
            self._session = MagicMock()
            self.result_retries = 3
            self.result_retry_delay = 0.1

    transport = DummyHttpTransport()

    injected_client = None

    @worker.skill("ai_agent_skill")
    async def ai_handler(params, orchestrator_client: OrchestratorClient, **kwargs):
        nonlocal injected_client
        injected_client = orchestrator_client
        # Call subtask
        await orchestrator_client.call_skill("web_search", {"query": "test"})
        return {"status": "ok"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "ai_agent_skill",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    # Mock http post for subtask call
    response_mock = MagicMock()
    response_mock.status = 200
    response_mock.json = AsyncMock(return_value={"result": "found"})

    cm_mock = MagicMock()
    cm_mock.__aenter__.return_value = response_mock
    transport._session.post = MagicMock(return_value=cm_mock)

    await worker._process_task(task_data)

    assert injected_client is not None
    assert isinstance(injected_client, OrchestratorClient)
    transport._session.post.assert_called_once()


@pytest.mark.asyncio
async def test_telemetry_throttling_edges():
    """Tests boundary values, force timeout, and zero RAM scenarios in telemetry throttling."""
    worker = Worker()
    worker._telemetry_deadband = 5.0
    worker._telemetry_force_interval = 2.0  # 2 seconds timeout

    # 1. Zero RAM boundary scenario (relative diff should not fail with division by zero)
    usage_zero = ResourcesUsage(cpu_load_percent=10.0, ram_used_gb=0.0)
    usage_next = ResourcesUsage(cpu_load_percent=10.0, ram_used_gb=1.0)
    with patch.object(worker, "_get_resources_usage", side_effect=[usage_zero, usage_next]):
        hb1 = worker._create_heartbeat_payload()
        assert hb1.usage == usage_zero

        hb2 = worker._create_heartbeat_payload()
        assert hb2.usage == usage_next  # Should trigger update since division by zero was avoided and last_ram was 0

    # Reset worker
    worker = Worker()
    worker._telemetry_deadband = 5.0
    worker._telemetry_force_interval = 0.5  # 0.5s force interval

    # 2. Force interval timeout scenario
    usage1 = ResourcesUsage(cpu_load_percent=10.0, ram_used_gb=1.0)
    with patch.object(worker, "_get_resources_usage", return_value=usage1):
        hb1 = worker._create_heartbeat_payload()
        assert hb1.usage == usage1

        # Second payload immediately (no change, no timeout) -> usage should be None
        hb2 = worker._create_heartbeat_payload()
        assert hb2.usage is None

        # Wait for force interval timeout (0.5s)
        await asyncio.sleep(0.6)

        # Third payload (no change, but timeout expired) -> usage should be populated
        hb3 = worker._create_heartbeat_payload()
        assert hb3.usage == usage1


@pytest.mark.asyncio
async def test_etag_blob_caching_edge_cases(tmp_path):
    """Tests missing ETag and pre-existing local file conflicts during blob caching."""
    from avtomatika_worker.config import WorkerConfig

    config = WorkerConfig()
    config.S3_ENDPOINT_URL = "http://mock-s3"
    config.WORKER_BLOB_CACHE_DIR = str(tmp_path / "cache")
    config.TASK_FILES_DIR = str(tmp_path / "payloads")

    s3_manager = S3Manager(config)

    # 1. Missing ETag Scenario
    s3_manager._provider.get_metadata = AsyncMock(return_value=None)  # No ETag returned
    s3_manager._provider.download = AsyncMock()

    params = {"file": "s3://my-bucket/no-etag.bin"}
    processed = await s3_manager.process_params(params, task_id="t_no_etag")
    local_path = processed["file"]

    # Download should be called directly to target local_path, no symlink
    s3_manager._provider.download.assert_called_once_with("s3://my-bucket/no-etag.bin", local_path)
    assert not os.path.islink(local_path)

    # 2. Existing File/Symlink Conflict Scenario
    s3_manager._provider.get_metadata = AsyncMock(return_value={"etag": "conflict-etag"})

    async def side_effect(uri, local_path):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w") as f:
            f.write("data")

    s3_manager._provider.download = AsyncMock(side_effect=side_effect)

    # Create a pre-existing file at the target local_path to simulate a conflict
    local_root = os.path.join(config.TASK_FILES_DIR, "t_conflict")
    os.makedirs(local_root, exist_ok=True)
    conflict_file = os.path.join(local_root, "conflict.bin")
    with open(conflict_file, "w") as f:
        f.write("old_conflict_data")

    params_conflict = {"file": "s3://my-bucket/conflict.bin"}
    processed_conflict = await s3_manager.process_params(params_conflict, task_id="t_conflict")
    local_path_conflict = processed_conflict["file"]

    # Symlink should successfully overwrite the pre-existing file without errors
    assert os.path.islink(local_path_conflict)
    assert os.readlink(local_path_conflict) == os.path.join(config.WORKER_BLOB_CACHE_DIR, "conflict-etag")


@pytest.mark.asyncio
async def test_result_uploader_error_handling():
    """Tests 429 Rate Limit Retry-After and network failures in the result uploader."""
    worker = Worker()
    worker._config.RESULT_MAX_RETRIES = 2
    worker._config.RESULT_RETRY_INITIAL_DELAY = 0.01

    from rxon.exceptions import RxonRateLimitError

    transport = AsyncMock(spec=MockTransport)

    # Mock rate limit on first call, success on second
    transport.send_result.side_effect = [RxonRateLimitError("Rate limit", details={"retry_after": 0.05}), True]

    from rxon.models import TaskResult

    result = TaskResult(job_id="j1", task_id="t1", worker_id="w1", status="success", timestamp=123)

    uploader_task = asyncio.create_task(worker._result_queue_worker())
    await worker._result_queue.put((transport, result))

    # Let the uploader process the queue with rate limit delay
    await asyncio.sleep(0.15)

    assert transport.send_result.call_count == 2
    uploader_task.cancel()


@pytest.mark.asyncio
async def test_orchestrator_client_error_handling():
    """Tests error response handling and mock fallbacks in OrchestratorClient."""
    # 1. Non-HttpTransport Fallback Test
    from rxon.transports.base import Transport

    mock_transport = AsyncMock(spec=Transport)
    worker = Worker()

    client = OrchestratorClient(mock_transport, worker)
    res = await client.call_skill("some_skill", {"x": 1})
    assert res["status"] == "success"
    assert res["data"]["mocked"] is True

    # 2. HTTP Error Response Test (>= 400 status code)
    from rxon.transports.http import HttpTransport

    class DummyHttpTransport(HttpTransport):
        def __init__(self):
            self.base_url = "http://mock-orchestrator"
            self._headers = {}
            self._session = MagicMock()
            self.result_retries = 3
            self.result_retry_delay = 0.1

    transport = DummyHttpTransport()

    response_mock = MagicMock()
    response_mock.status = 500
    response_mock.text = AsyncMock(return_value="Internal Server Error")

    cm_mock = MagicMock()
    cm_mock.__aenter__.return_value = response_mock
    transport._session.post = MagicMock(return_value=cm_mock)

    client = OrchestratorClient(transport, worker)
    with pytest.raises(RuntimeError, match="Orchestrator returned HTTP 500: Internal Server Error"):
        await client.call_skill("failing_skill", {})
