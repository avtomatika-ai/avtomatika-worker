# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio

import pytest
from rxon.models import TaskPayload
from rxon.testing import MockTransport

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_graceful_shutdown_drains_tasks(mocker):
    transport = MockTransport(worker_id="test-worker")
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])
    worker._config.SHUTDOWN_TIMEOUT = 1.0

    task_completed = False

    @worker.skill("long_task")
    async def long_handler(params, **kwargs):
        nonlocal task_completed
        await asyncio.sleep(0.5)
        task_completed = True
        return {"status": "success"}

    # Pre-inject a task
    transport.push_task(TaskPayload(job_id="j1", task_id="t1", type="long_task", params={}, tracing_context={}))

    # Run worker in background
    worker_task = asyncio.create_task(worker.main())

    # Wait for task to start
    while worker._current_load == 0:
        await asyncio.sleep(0.01)

    assert "t1" in worker._active_tasks

    # Simulate shutdown signal
    worker._shutdown_event.set()

    # Wait for main loop to finish
    await asyncio.wait_for(worker_task, timeout=2.0)

    # Verify task finished before exit
    assert task_completed is True
    assert len(transport.results) == 1
    assert transport.results[0].status == "success"


@pytest.mark.asyncio
async def test_shutdown_timeout(mocker):
    transport = MockTransport(worker_id="test-worker")
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])
    worker._config.SHUTDOWN_TIMEOUT = 0.2  # Short timeout

    task_interrupted = False

    @worker.skill("infinite_task")
    async def infinite_handler(params, **kwargs):
        nonlocal task_interrupted
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            task_interrupted = True
            raise

    transport.push_task(TaskPayload(job_id="j1", task_id="t1", type="infinite_task", params={}, tracing_context={}))

    worker_task = asyncio.create_task(worker.main())

    while worker._current_load == 0:
        await asyncio.sleep(0.01)

    worker._shutdown_event.set()

    # Wait for exit (should happen due to timeout)
    await asyncio.wait_for(worker_task, timeout=1.0)

    # Check that task was cancelled when wait_for timed out
    assert task_interrupted is True
