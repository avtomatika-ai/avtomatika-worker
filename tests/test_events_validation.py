# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import AsyncMock, patch

import pytest
from rxon.testing import MockTransport

from avtomatika_worker.worker import Worker


@pytest.fixture
def worker():
    return Worker()


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

    # Verify emitted event
    assert len(transport.emitted_events) == 1
    event = transport.emitted_events[0]
    assert event.event_type == "status_update"
    assert event.payload == {"status": "processing"}
    assert event.timestamp is not None


@pytest.mark.asyncio
async def test_send_event_no_otel_safety(mocker):
    """
    Tests that emitting an event works safely even if 'opentelemetry' is not installed.
    Verifies that the original trace_context is preserved.
    """
    from rxon.testing import MockTransport

    transport = MockTransport()
    worker = Worker()

    # Simulate missing OTel by patching import in worker.py
    # We need to patch it where it is used in send_event_wrapper
    mocker.patch("avtomatika_worker.worker.propagate", None, create=True)

    @worker.skill("no_otel_task")
    async def no_otel_task(params, send_event, **kwargs):
        await send_event("test_event", {"val": 42})
        return "ok"

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "no_otel_task",
        "params": {},
        "tracing_context": {"traceparent": "original-trace-id"},
        "client": transport,
    }

    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    assert len(transport.emitted_events) == 1
    event = transport.emitted_events[0]
    assert event.event_type == "test_event"
    # Crucial: original context must be preserved
    assert event.trace_context == {"traceparent": "original-trace-id"}
    assert event.timestamp is not None


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

    # Process task
    with (
        patch.object(worker._s3_manager, "process_params", new_callable=AsyncMock, return_value={}),
        patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock, return_value=({}, {})),
    ):
        await worker._process_task(task_data)

    # Verify event was NOT emitted
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
    assert transport.emitted_events[0].event_type == "undeclared_event"
    assert "Emitting undeclared event type 'undeclared_event'. Allowed by config." in caplog.text


@pytest.mark.asyncio
async def test_progress_event_always_allowed(worker, caplog):
    """Tests that 'progress' events are always allowed regardless of schema."""
    transport = MockTransport()
    worker._config.STRICT_EVENT_VALIDATION = True

    @worker.skill(events_schema={})
    async def my_task(params, send_event, **kwargs):
        # progress is a special built-in event
        await send_event("progress", {"progress": 0.5})
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
    assert transport.emitted_events[0].event_type == "progress"
