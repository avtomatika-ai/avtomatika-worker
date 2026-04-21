# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import AsyncMock, patch

import pytest
from rxon.models import TaskResult
from rxon.testing import MockTransport

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


@pytest.fixture
def worker():
    config = WorkerConfig()
    config.WORKER_ID = "test-worker-id"
    config.STRICT_EVENT_VALIDATION = True
    return Worker(config=config)


@pytest.mark.asyncio
async def test_correct_event_emission(worker):
    """Tests that a correctly formatted event is successfully emitted and contains a timestamp."""
    transport = MockTransport()

    @worker.skill(events_schema={"status_update": {"type": "object", "properties": {"status": {"type": "string"}}}})
    async def my_task(params, send_event, **kwargs):
        await send_event("status_update", {"status": "processing"})
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "my_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    # Process task
    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    assert len(transport.emitted_events) == 1
    event = transport.emitted_events[0]
    assert event.event_type == "status_update"
    assert event.payload == {"status": "processing"}
    assert event.timestamp is not None
    assert event.timestamp > 0


@pytest.mark.asyncio
async def test_incorrect_event_emission_blocked(worker, caplog):
    """Tests that an incorrectly formatted event is blocked and an error is logged."""
    transport = MockTransport()

    @worker.skill(events_schema={"status_update": {"type": "object", "properties": {"status": {"type": "string"}}}})
    async def my_task(params, send_event, **kwargs):
        # Invalid payload (number instead of string)
        await send_event("status_update", {"status": 123})
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "my_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    # Event should be blocked
    assert len(transport.emitted_events) == 0
    assert "Local contract violation for event 'status_update'" in caplog.text


@pytest.mark.asyncio
async def test_undeclared_event_blocked_in_strict_mode(worker, caplog):
    """Tests that undeclared events are blocked if STRICT_EVENT_VALIDATION is True."""
    transport = MockTransport()
    worker._config.STRICT_EVENT_VALIDATION = True

    @worker.skill(events_schema={"status_update": {}})
    async def my_task(params, send_event, **kwargs):
        # Undeclared event
        await send_event("undeclared_event", {"some": "data"})
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "my_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    assert len(transport.emitted_events) == 0
    assert "Contract violation: Emitting undeclared event type 'undeclared_event'. Blocked." in caplog.text


@pytest.mark.asyncio
async def test_undeclared_event_allowed_in_non_strict_mode(worker, caplog):
    """Tests that undeclared events are allowed if STRICT_EVENT_VALIDATION is False."""
    transport = MockTransport()
    worker._config.STRICT_EVENT_VALIDATION = False

    @worker.skill(events_schema={"status_update": {}})
    async def my_task(params, send_event, **kwargs):
        # Undeclared event
        await send_event("undeclared_event", {"some": "data"})
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "my_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    assert len(transport.emitted_events) == 1
    assert "Emitting undeclared event type 'undeclared_event'. Allowed by config." in caplog.text


@pytest.mark.asyncio
async def test_task_result_holarchy_fields(worker):
    """Tests that TaskResult properly includes worker_id and origin_worker_id."""
    transport = MockTransport()

    @worker.skill()
    async def my_task(params, **kwargs):
        return {"data": {"foo": "bar"}}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "my_task",
        "params": {},
        "tracing_context": {},
        "origin_worker_id": "original-source",
        "client": transport,
    }

    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({"foo": "bar"}, {})),
    ):
        await worker._process_task(task_data)

    assert len(transport.results) == 1
    result = transport.results[0]

    assert isinstance(result, TaskResult)
    # worker_id is mandatory now
    assert result.worker_id == worker._config.WORKER_ID
    # origin_worker_id should be preserved
    assert result.origin_worker_id == "original-source"
    # status should default to "success"
    assert result.status == "success"
    # timestamp must be present
    assert result.timestamp is not None
