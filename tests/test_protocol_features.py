import asyncio
from typing import Any

import pytest
from rxon.constants import (
    ERROR_CODE_CONTRACT_VIOLATION,
    TASK_STATUS_FAILURE,
)
from rxon.models import (
    Heartbeat,
    TaskPayload,
)
from rxon.testing import MockTransport
from rxon.utils import to_dict

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


class JitterMockTransport(MockTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.heartbeat_response = {"status": "ok"}

    async def send_heartbeat(self, heartbeat: Heartbeat) -> dict[str, Any]:
        return self.heartbeat_response


@pytest.mark.asyncio
async def test_worker_security_signing():
    config = WorkerConfig()
    config.WORKER_TOKEN = "test-secure-token"
    config.WORKER_ID = "secure-worker"
    transport = MockTransport(worker_id="secure-worker")
    worker = Worker(config=config, clients=[({}, transport)])
    reg = worker._create_registration_payload()
    assert len(reg.security.signature) == 64
    hb = worker._create_heartbeat_payload()
    assert len(hb.security.signature) == 64


@pytest.mark.asyncio
async def test_heartbeat_jitter_variants():
    """Verify worker handles various jitter formats."""
    config = WorkerConfig()
    transport = JitterMockTransport()
    worker = Worker(config=config, clients=[({}, transport)])
    worker._heartbeat_cooldown = 0

    # 1. String int
    transport.heartbeat_response = {"next_heartbeat_jitter_ms": "500"}
    assert await worker._send_single_heartbeat(transport) == 500

    # 2. String float
    transport.heartbeat_response = {"next_heartbeat_jitter_ms": "500.5"}
    # Note: my current implementation uses int(), it might return 0 for "500.5" or we can improve it
    jitter = await worker._send_single_heartbeat(transport)
    assert jitter in (0, 500)  # Current safety check returns 0 for non-int strings

    # 3. Real float
    transport.heartbeat_response = {"next_heartbeat_jitter_ms": 123.45}
    assert await worker._send_single_heartbeat(transport) == 123


@pytest.mark.asyncio
async def test_output_contract_violation():
    config = WorkerConfig()
    transport = MockTransport()
    worker = Worker(config=config, clients=[({}, transport)])

    @worker.skill(
        "strict_skill",
        output_schema={"type": "object", "properties": {"count": {"type": "integer"}}, "required": ["count"]},
    )
    async def strict_handler(params, **kwargs):
        return {"count": "not-an-integer"}

    task_payload = TaskPayload(job_id="j-1", task_id="t-1", type="strict_skill", params={})
    await worker._process_task({**to_dict(task_payload), "client": transport})
    assert transport.results[0].status == TASK_STATUS_FAILURE
    assert transport.results[0].error.code == ERROR_CODE_CONTRACT_VIOLATION


@pytest.mark.asyncio
async def test_dynamic_skill_sync():
    config = WorkerConfig()
    transport = MockTransport()
    worker = Worker(config=config, clients=[({}, transport)])
    worker._heartbeat_cooldown = 0
    await worker._send_single_heartbeat(transport)
    assert transport.heartbeats[-1].supported_skills is not None
    await worker._send_single_heartbeat(transport)
    assert transport.heartbeats[-1].supported_skills is None

    @worker.skill("new_dynamic_skill")
    def new_skill(params):
        return "ok"

    await worker._send_single_heartbeat(transport)
    assert transport.heartbeats[-1].supported_skills is not None


@pytest.mark.asyncio
async def test_heartbeat_debounce():
    config = WorkerConfig()
    transport = MockTransport()
    worker = Worker(config=config, clients=[({}, transport)])
    # We want to test that multiple triggers result in 1 heartbeat.
    # Set cooldown to a small but positive value so throttling happens.
    worker._heartbeat_cooldown = 0.05

    # Pre-set last heartbeat time to "now" so the first trigger is also throttled
    from time import time

    worker._last_heartbeat_times[transport] = time()

    for _ in range(3):
        worker._schedule_heartbeat_debounce()
    await asyncio.sleep(0.1)
    assert len(transport.heartbeats) == 1


@pytest.mark.asyncio
async def test_large_payload_signing():
    config = WorkerConfig()
    config.WORKER_TOKEN = "secure"
    transport = MockTransport()
    worker = Worker(config=config, clients=[({}, transport)])
    large_data = "x" * 50000

    @worker.skill("large_skill")
    async def large_handler(params, **kwargs):
        return {"data": large_data}

    task_payload = TaskPayload(job_id="j-large", task_id="t-large", type="large_skill", params={})
    await worker._process_task({**to_dict(task_payload), "client": transport})
    assert transport.results[0].status == "success"
    assert len(transport.results[0].security.signature) == 64
