# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import asyncio

import pytest
from rxon.exceptions import RxonRateLimitError

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_multi_orchestrator_backoff_isolation(mocker):
    worker = Worker(worker_type="test")

    @worker.skill("dummy")
    def dummy(params):
        pass

    class MockTransport:
        def __init__(self, name):
            self.name = name
            self.poll_task = mocker.AsyncMock()

        def __repr__(self):
            return f"MockTransport({self.name})"

    c1 = MockTransport("C1")
    c2 = MockTransport("C2")

    worker._clients = [({"url": "http://c1"}, c1), ({"url": "http://c2"}, c2)]

    c1.poll_task.side_effect = RxonRateLimitError("429")
    c2.poll_task.return_value = None

    await worker._poll_for_tasks_with_status(c1)
    assert worker._poll_backoffs[c1] == 30.0

    await worker._poll_for_tasks_with_status(c2)
    assert c2.poll_task.called
    assert c2 not in worker._poll_backoffs

    assert worker._poll_backoffs[c1] == 30.0

    from time import time

    assert worker._next_poll_time[c1] > time()


@pytest.mark.asyncio
async def test_heartbeat_isolation(mocker):
    c1 = mocker.AsyncMock(name="Client1")
    c2 = mocker.AsyncMock(name="Client2")

    worker = Worker(clients=[({"url": "http://c1"}, c1), ({"url": "http://c2"}, c2)])

    await worker._send_single_heartbeat(c1)
    last_c1 = worker._last_heartbeat_times[c1]

    assert c2 not in worker._last_heartbeat_times

    await worker._send_single_heartbeat(c2)
    assert c2 in worker._last_heartbeat_times
    assert worker._last_heartbeat_times[c2] != last_c1


@pytest.mark.asyncio
async def test_registration_isolation(mocker):
    c1 = mocker.AsyncMock(name="FailingClient")
    c2 = mocker.AsyncMock(name="SucceedingClient")

    c1.register.side_effect = Exception("Network Down")
    c2.register.return_value = {"status": "ok"}

    worker = Worker(worker_type="test")
    worker._clients = [({"url": "http://c1"}, c1), ({"url": "http://c2"}, c2)]

    async def smart_wait_for(coro, timeout=None):
        try:
            if worker._shutdown_event.is_set():
                return
            await asyncio.sleep(0.01)
            if worker._shutdown_event.is_set():
                return
            raise TimeoutError()
        finally:
            coro.close()

    mocker.patch("avtomatika_worker.worker.wait_for", side_effect=smart_wait_for)

    t1 = asyncio.create_task(worker._manage_single_orchestrator(c1))
    t2 = asyncio.create_task(worker._manage_single_orchestrator(c2))

    found = False
    for _ in range(50):
        if c2.register.called:
            found = True
            break
        await asyncio.sleep(0.01)

    assert found
    assert worker._registered_event.is_set()

    worker._shutdown_event.set()
    await asyncio.sleep(0.05)

    assert c1.register.called

    t1.cancel()
    t2.cancel()
    await asyncio.gather(t1, t2, return_exceptions=True)
