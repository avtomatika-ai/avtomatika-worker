# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio

import pytest
from rxon.exceptions import RxonError
from rxon.testing import MockTransport

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


class FailingTransport(MockTransport):
    def __init__(self, fail_count=2, **kwargs):
        super().__init__(**kwargs)
        self.fail_count = fail_count
        self.call_count = 0

    async def register(self, registration):
        self.call_count += 1
        if self.call_count <= self.fail_count:
            raise RxonError("Simulated registration failure")
        return await super().register(registration)


@pytest.mark.asyncio
async def test_registration_infinite_retry():
    """Tests that the worker retries registration with backoff until success."""
    config = WorkerConfig()
    config.REGISTRATION_RETRY_INITIAL_DELAY = 0.1
    config.REGISTRATION_RETRY_MAX_DELAY = 0.4

    transport = FailingTransport(fail_count=3)
    worker = Worker(config=config, clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    # Run registration task for this client
    reg_task = asyncio.create_task(worker._register_client_with_retry(transport))

    # Wait for success (initial + 3 retries = 4 calls total)
    try:
        await asyncio.wait_for(reg_task, timeout=2.0)
    except TimeoutError:
        pytest.fail("Registration did not succeed within timeout")

    assert transport.call_count == 4
    assert len(transport.registered) == 1
    assert worker._registered_event.is_set()


@pytest.mark.asyncio
async def test_registration_retry_shutdown():
    """Tests that registration retries are interrupted by shutdown signal."""
    config = WorkerConfig()
    config.REGISTRATION_RETRY_INITIAL_DELAY = 10.0  # Long delay

    transport = FailingTransport(fail_count=1)
    worker = Worker(config=config, clients=[({"url": "http://test-orchestrator", "weight": 1}, transport)])

    # Start registration
    reg_task = asyncio.create_task(worker._register_client_with_retry(transport))

    await asyncio.sleep(0.1)  # Let it fail once
    assert transport.call_count == 1

    # Signal shutdown
    worker._shutdown_event.set()

    # Task should finish quickly
    await asyncio.wait_for(reg_task, timeout=1.0)
    assert transport.call_count == 1  # No second call because of shutdown
