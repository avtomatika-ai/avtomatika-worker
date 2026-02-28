# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import pytest

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


def test_config_validation_invalid_worker_id(monkeypatch):
    """Test that WorkerConfig raises ValueError when WORKER_ID contains invalid characters."""
    monkeypatch.setenv("WORKER_ID", "invalid/id")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    # Config init reads env vars immediately
    config = WorkerConfig()

    with pytest.raises(ValueError, match="Invalid WORKER_ID"):
        config.validate()


def test_config_validation_valid_worker_id(monkeypatch):
    """Test that WorkerConfig passes validation with a valid WORKER_ID."""
    monkeypatch.setenv("WORKER_ID", "valid-worker_123")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    config = WorkerConfig()
    # Should not raise
    config.validate()


def test_task_decorator_validation_invalid_name(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    with pytest.raises(ValueError, match="Invalid skill name"):

        @worker.skill(name="invalid/task name")
        async def my_task(params, **kwargs):
            pass


def test_task_decorator_validation_invalid_type(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    with pytest.raises(ValueError, match="Invalid skill type"):

        @worker.skill(name="valid_task", type="invalid type!")
        async def my_task(params, **kwargs):
            pass


def test_task_decorator_validation_success(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    @worker.skill(name="valid_task", type="valid_type")
    async def my_task(params, **kwargs):
        pass

    assert "valid_task" in worker._skill_handlers


@pytest.mark.asyncio
async def test_fail_fast_result_validation(monkeypatch, mocker):
    """Tests that task results are validated locally against the output schema."""
    from rxon.constants import ERROR_CODE_CONTRACT_VIOLATION
    from rxon.testing import MockTransport

    worker = Worker()
    transport = MockTransport()

    @worker.skill(output_schema={"type": "object", "properties": {"score": {"type": "number"}}})
    async def scored_task(params, **kwargs):
        # Invalid result (score is a string)
        return {"status": "success", "data": {"score": "high"}}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "scored_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    await worker._process_task(task_data)

    assert len(transport.results) == 1
    result = transport.results[0]
    assert result.status == "failure"
    assert result.error.code == ERROR_CODE_CONTRACT_VIOLATION
    assert "contract violation" in result.error.message.lower()


@pytest.mark.asyncio
async def test_fail_fast_event_validation(monkeypatch, mocker):
    """Tests that emitted events are validated locally against the events schema."""
    from rxon.testing import MockTransport

    worker = Worker()
    transport = MockTransport()

    @worker.skill(events_schema={"alert": {"type": "object", "properties": {"level": {"type": "integer"}}}})
    async def alerting_task(params, send_event, **kwargs):
        # Invalid event payload (level is a string)
        await send_event("alert", {"level": "critical"})
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "alerting_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    await worker._process_task(task_data)

    # Event should be blocked and not sent to transport
    assert len(transport.emitted_events) == 0
