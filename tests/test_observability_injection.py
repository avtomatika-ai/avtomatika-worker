# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import AsyncMock

import pytest
import rxon
from rxon.models import TaskPayload

from avtomatika_worker.observability import ObservabilityManager
from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_observability_dependency_injection():
    """Tests that ObservabilityManager can be injected into a skill handler."""
    worker = Worker(worker_type="test-worker")

    injected_obs = None

    @worker.skill("test-skill")
    async def test_handler(params, obs: ObservabilityManager, **kwargs):
        nonlocal injected_obs
        injected_obs = obs
        return {"status": "success"}

    # Mock client and payload
    client = AsyncMock()
    payload = TaskPayload(job_id="j1", task_id="t1", type="test-skill", params={}, tracing_context={})

    task_data = rxon.to_dict(payload)
    task_data["client"] = client

    # Process task
    await worker._process_task(task_data)

    # Verify injection
    assert injected_obs is not None
    assert isinstance(injected_obs, ObservabilityManager)
    assert injected_obs == worker._observability


@pytest.mark.asyncio
async def test_observability_middleware_context():
    """Tests that ObservabilityManager is present in middleware context."""
    worker = Worker(worker_type="test-worker")

    context_obs = None

    async def obs_middleware(ctx, next_handler):
        nonlocal context_obs
        context_obs = ctx.get("observability")
        return await next_handler()

    worker.add_middleware(obs_middleware)

    @worker.skill("test-skill")
    async def test_handler(params):
        return {"status": "success"}

    client = AsyncMock()
    payload = TaskPayload(job_id="j1", task_id="t1", type="test-skill", params={}, tracing_context={})
    task_data = rxon.to_dict(payload)
    task_data["client"] = client

    await worker._process_task(task_data)

    assert context_obs is not None
    assert context_obs == worker._observability
