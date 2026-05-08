# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import asyncio

import pytest
from rxon.exceptions import RxonRateLimitError

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_heartbeat_loop_rate_limit_backoff(mocker):
    transport = mocker.AsyncMock()
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    transport.send_heartbeat.side_effect = [RxonRateLimitError("429", details={"retry_after": "0.1"}), {"status": "ok"}]

    mock_wait_for = mocker.patch("avtomatika_worker.worker.wait_for")

    async def mock_wait_for_func(coro, timeout=None):
        try:
            await asyncio.sleep(0.01)
        finally:
            coro.close()
        return

    mock_wait_for.side_effect = mock_wait_for_func

    mocker.patch.object(worker, "_register_client_with_retry", new_callable=mocker.AsyncMock)

    task = asyncio.create_task(worker._manage_single_orchestrator(transport))

    found = False
    for _ in range(50):
        await asyncio.sleep(0.01)
        backoff_calls = [call for call in mock_wait_for.call_args_list if call.kwargs.get("timeout") == 0.1]
        if len(backoff_calls) > 0:
            found = True
            break

    assert found
    worker._shutdown_event.set()
    await task
