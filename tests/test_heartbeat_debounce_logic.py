# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import pytest

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_heartbeat_debounce_event_not_lost(mocker):
    transport = mocker.AsyncMock()
    worker = Worker(clients=[({"url": "http://test"}, transport)])
    worker._heartbeat_cooldown = 0.5

    await worker._send_single_heartbeat(transport)
    assert transport.send_heartbeat.call_count == 1

    mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)
    mock_create_task = mocker.patch("avtomatika_worker.worker.create_task")

    await worker._send_single_heartbeat(transport)
    assert transport.send_heartbeat.call_count == 1
    assert worker._debouncing_flags[transport] is True

    if mock_create_task.called:
        mock_create_task.call_args[0][0].close()

    worker._last_heartbeat_times[transport] -= 1.0
    worker._debouncing_flags[transport] = False
    await worker._send_single_heartbeat(transport)
    assert transport.send_heartbeat.call_count == 2
    assert worker._debouncing_flags[transport] is False


@pytest.mark.asyncio
async def test_debounce_task_uniqueness(mocker):
    transport = mocker.AsyncMock()
    worker = Worker(clients=[({"url": "http://test"}, transport)])
    worker._heartbeat_cooldown = 10.0

    await worker._send_single_heartbeat(transport)

    mock_create_task = mocker.patch("avtomatika_worker.worker.create_task")
    await worker._send_single_heartbeat(transport)
    await worker._send_single_heartbeat(transport)
    await worker._send_single_heartbeat(transport)

    assert mock_create_task.call_count == 1
    mock_create_task.call_args[0][0].close()
