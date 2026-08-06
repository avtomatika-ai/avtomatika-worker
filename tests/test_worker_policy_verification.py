# Copyright (c) 2025-2026 Dmitrii Gagarin aka madgagarin
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_worker_verifies_orchestrator_signature():
    config = MagicMock()
    config.WORKER_ID = "worker-1"
    config.GLOBAL_WORKER_TOKEN = "token"

    worker = Worker(config)
    worker._skill_handlers = {}

    secret_key = "secret_orchestrator_key"
    os.environ["ORCHESTRATOR_SECRET_KEY"] = secret_key

    # Task payload signed with invalid signature
    raw_payload_invalid = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "some_skill",
        "sig": "invalid_sig",
        "client": AsyncMock(),
    }
    with pytest.raises(PermissionError) as exc:
        await worker._process_task(raw_payload_invalid)
    assert "orchestrator signature mismatch" in str(exc.value)


@pytest.mark.asyncio
async def test_worker_policy_allowed_skills_enforced():
    config = MagicMock()
    config.WORKER_ID = "worker-1"

    worker = Worker(config)

    # Dictionary policy
    raw_payload_dict = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "unallowed_skill",
        "policy": {"allowed_skills": ["allowed_skill_only"]},
        "client": AsyncMock(),
    }
    with pytest.raises(PermissionError) as exc:
        await worker._process_task(raw_payload_dict)
    assert "not permitted by task policy" in str(exc.value)


@pytest.mark.asyncio
async def test_worker_provenance_propagation():
    worker = Worker()

    @worker.skill("test_provenance")
    async def handler(params):
        return {"data": "ok"}

    obs_mock = MagicMock()
    worker._observability = obs_mock

    raw_payload = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "test_provenance",
        "step": 2,
        "depth": 3,
        "parent_hash": "parent_123",
        "client": AsyncMock(),
    }

    await worker._process_task(raw_payload)
    obs_mock.start_task_span.assert_called_once_with(
        "test_provenance", "t1", "j1", parent_context=None, step=2, depth=3, parent_hash="parent_123"
    )
