# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import AsyncMock, patch

import pytest
from rxon.exceptions import RxonNetworkError, RxonRateLimitError
from rxon.models import TaskPayload

from avtomatika_worker.worker import Worker


@pytest.fixture
def worker():
    w = Worker(worker_type="test-worker")

    @w.skill("test_skill")
    async def handler(params):
        return {"status": "success"}

    return w


@pytest.mark.asyncio
async def test_exponential_backoff_progression(worker, mocker):
    client = mocker.AsyncMock()
    client.poll_task.side_effect = RxonRateLimitError("Too many requests")

    await worker._poll_for_tasks_with_status(client)
    assert worker._poll_backoffs[client] == 30.0

    worker._next_poll_time[client] = 0
    await worker._poll_for_tasks_with_status(client)
    assert worker._poll_backoffs[client] == 60.0


@pytest.mark.asyncio
async def test_backoff_max_limit(worker, mocker):
    worker._config.POLL_BACKOFF_MAX = 45.0
    client = mocker.AsyncMock()
    client.poll_task.side_effect = RxonRateLimitError("429")

    worker._poll_backoffs[client] = 40.0
    worker._next_poll_time[client] = 0

    await worker._poll_for_tasks_with_status(client)
    assert worker._poll_backoffs[client] == 45.0


@pytest.mark.asyncio
async def test_backoff_reset_on_success(worker, mocker):
    client = mocker.AsyncMock()

    client.poll_task.side_effect = RxonRateLimitError("429")
    await worker._poll_for_tasks_with_status(client)
    assert client in worker._poll_backoffs

    client.poll_task.side_effect = None
    client.poll_task.return_value = None
    worker._next_poll_time[client] = 0

    await worker._poll_for_tasks_with_status(client)

    assert client not in worker._poll_backoffs
    assert client not in worker._next_poll_time


@pytest.mark.asyncio
async def test_waterfall_skips_blocked_orchestrator(worker, mocker):
    worker._config.MULTI_ORCHESTRATOR_MODE = "WATERFALL"

    c1 = mocker.AsyncMock(name="VIP")
    c2 = mocker.AsyncMock(name="STD")

    worker._clients = [({"url": "vip", "priority": 1}, c1), ({"url": "std", "priority": 2}, c2)]

    worker._next_poll_time[c1] = 10**10
    c2.poll_task.return_value = None

    await worker._poll_for_tasks_with_status(c1)
    await worker._poll_for_tasks_with_status(c2)

    assert not c1.poll_task.called
    assert c2.poll_task.called


@pytest.mark.asyncio
async def test_network_error_triggers_backoff(worker, mocker):
    client = mocker.AsyncMock()
    client.poll_task.side_effect = RxonNetworkError("Connection refused")

    await worker._poll_for_tasks_with_status(client)
    assert worker._poll_backoffs[client] == worker._config.POLL_BACKOFF_INITIAL


@pytest.mark.asyncio
async def test_backoff_with_actual_task(worker, mocker):
    client = mocker.AsyncMock()
    worker._poll_backoffs[client] = 10.0
    worker._next_poll_time[client] = 0

    client.poll_task.return_value = TaskPayload(job_id="j1", task_id="t1", type="test_skill", params={})

    with patch.object(worker, "_process_task", return_value=AsyncMock()):
        await worker._poll_for_tasks_with_status(client)

    assert client not in worker._poll_backoffs
    assert client not in worker._next_poll_time


@pytest.mark.asyncio
async def test_heartbeat_is_not_blocked_by_polling_backoff(worker, mocker):
    client = mocker.AsyncMock()
    client.poll_task.side_effect = RxonRateLimitError("429")
    await worker._poll_for_tasks_with_status(client)
    assert worker._next_poll_time[client] > 0

    client.send_heartbeat.return_value = {"status": "ok"}

    await worker._send_single_heartbeat(client)
    assert client.send_heartbeat.called


def test_rate_limit_error_contains_correct_code():
    from rxon.constants import ERROR_CODE_LIMIT_EXCEEDED

    exc = RxonRateLimitError("Rate limited")
    assert exc.details.get("code") == ERROR_CODE_LIMIT_EXCEEDED


@pytest.mark.asyncio
async def test_backoff_respects_retry_after_header(worker, mocker):
    client = mocker.AsyncMock()
    client.poll_task.side_effect = RxonRateLimitError("429", details={"retry_after": 300.0})

    await worker._poll_for_tasks_with_status(client)

    assert worker._poll_backoffs[client] == 300.0
    from time import time

    assert abs(worker._next_poll_time[client] - (time() + 300.0)) < 1.0
