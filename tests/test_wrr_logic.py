# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import AsyncMock, patch

import pytest
from rxon.models import TaskPayload

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_waterfall_priority_logic():
    """
    Tests that WATERFALL mode correctly prioritizes orchestrators.
    """
    # 1. Setup two orchestrators in config
    orchestrators = [
        {"url": "http://vip-orchestrator", "priority": 1, "weight": 1},
        {"url": "http://std-orchestrator", "priority": 2, "weight": 1},
    ]

    with patch.dict("os.environ", {"ORCHESTRATORS_CONFIG": str(orchestrators).replace("'", '"')}):
        # Create worker
        worker = Worker(worker_type="test-worker")

        # Register a skill so the worker is not "busy"
        @worker.skill("test-skill")
        async def test_handler(params):
            return {"status": "success"}

        # Mock transports
        vip_client = AsyncMock()
        std_client = AsyncMock()

        # Overwrite clients with mocks (manually to ensure order)
        worker._clients = [
            (worker._config.ORCHESTRATORS[0], vip_client),
            (worker._config.ORCHESTRATORS[1], std_client),
        ]

        # Scenario A: VIP has a task. STD should NOT be polled.
        vip_client.poll_task.return_value = TaskPayload(
            task_id="task-vip", job_id="job-1", type="test-skill", params={}, tracing_context={}
        )

        # Trigger one polling cycle
        # We don't want to run the full _start_polling because it's an infinite loop.
        # Instead, we test the logic that would be inside the loop.

        # We need to mock _process_task to avoid actual execution
        with patch.object(worker, "_process_task", return_value=AsyncMock()):
            # Simulate the loop logic for WATERFALL
            for _, client in worker._clients:
                task_found = await worker._poll_for_tasks_with_status(client)
                if task_found:
                    break

            assert vip_client.poll_task.called
            assert not std_client.poll_task.called
            assert worker._current_load == 1

        # Reset for Scenario B
        worker._current_load = 0
        vip_client.poll_task.reset_mock()
        std_client.poll_task.reset_mock()

        # Scenario B: VIP is empty. STD has a task.
        vip_client.poll_task.return_value = None
        std_client.poll_task.return_value = TaskPayload(
            task_id="task-std", job_id="job-2", type="test-skill", params={}, tracing_context={}
        )

        with patch.object(worker, "_process_task", return_value=AsyncMock()):
            for _, client in worker._clients:
                task_found = await worker._poll_for_tasks_with_status(client)
                if task_found:
                    break

            assert vip_client.poll_task.called
            assert std_client.poll_task.called
            assert worker._current_load == 1


@pytest.mark.asyncio
async def test_waterfall_priority_cycle():
    """Tests that WATERFALL always returns to highest priority after any task."""
    orchestrators = [
        {"url": "http://vip", "priority": 1, "weight": 1},
        {"url": "http://std", "priority": 2, "weight": 1},
    ]
    with patch.dict("os.environ", {"ORCHESTRATORS_CONFIG": str(orchestrators).replace("'", '"')}):
        worker = Worker(worker_type="test-worker")

        @worker.skill("test-skill")
        async def test_handler(params):
            pass

        vip_client = AsyncMock()
        std_client = AsyncMock()
        worker._clients = [
            (worker._config.ORCHESTRATORS[0], vip_client),
            (worker._config.ORCHESTRATORS[1], std_client),
        ]

        # 1. VIP empty, STD has task
        vip_client.poll_task.return_value = None
        std_client.poll_task.return_value = TaskPayload(
            task_id="t1", job_id="j1", type="test-skill", params={}, tracing_context={}
        )

        with patch.object(worker, "_process_task", return_value=AsyncMock()):
            # Simulate first cycle
            for _, client in worker._clients:
                if await worker._poll_for_tasks_with_status(client):
                    break

            assert vip_client.poll_task.called
            assert std_client.poll_task.called

            # 2. RESET and simulate second cycle. VIP MUST be polled first again.
            vip_client.poll_task.reset_mock()
            std_client.poll_task.reset_mock()
            # VIP now has a task
            vip_client.poll_task.return_value = TaskPayload(
                task_id="t2", job_id="j2", type="test-skill", params={}, tracing_context={}
            )

            for _, client in worker._clients:
                if await worker._poll_for_tasks_with_status(client):
                    break

            assert vip_client.poll_task.called
            assert not std_client.poll_task.called


@pytest.mark.asyncio
async def test_round_robin_selection_logic():
    """Tests that ROUND_ROBIN correctly cycles through clients."""
    orchestrators = [
        {"url": "http://c1", "priority": 1, "weight": 1},
        {"url": "http://c2", "priority": 1, "weight": 1},
    ]
    with patch.dict("os.environ", {"ORCHESTRATORS_CONFIG": str(orchestrators).replace("'", '"')}):
        worker = Worker(worker_type="test-worker")
        worker._config.MULTI_ORCHESTRATOR_MODE = "ROUND_ROBIN"

        c1_client = AsyncMock()
        c2_client = AsyncMock()
        worker._clients = [
            (worker._config.ORCHESTRATORS[0], c1_client),
            (worker._config.ORCHESTRATORS[1], c2_client),
        ]
        worker._total_orchestrator_weight = 2

        # Cycle 1 -> Client 1
        sel1 = worker._get_next_client()
        assert sel1 == c1_client

        # Cycle 2 -> Client 2
        sel2 = worker._get_next_client()
        assert sel2 == c2_client

        # Cycle 3 -> Client 1 (Loop back)
        sel3 = worker._get_next_client()
        assert sel3 == c1_client


@pytest.mark.asyncio
async def test_failover_default_logic():
    """Tests that FAILOVER (default) polls next client if previous is empty in the SAME cycle."""
    orchestrators = [
        {"url": "http://c1", "priority": 1, "weight": 1},
        {"url": "http://c2", "priority": 1, "weight": 1},
    ]
    with patch.dict("os.environ", {"ORCHESTRATORS_CONFIG": str(orchestrators).replace("'", '"')}):
        worker = Worker(worker_type="test-worker")
        worker._config.MULTI_ORCHESTRATOR_MODE = "FAILOVER"

        @worker.skill("test-skill")
        async def test_handler(params):
            pass

        c1_client = AsyncMock()
        c2_client = AsyncMock()
        worker._clients = [
            (worker._config.ORCHESTRATORS[0], c1_client),
            (worker._config.ORCHESTRATORS[1], c2_client),
        ]

        # Scenario: C1 empty, C2 has task. FAILOVER polls both in one loop.
        c1_client.poll_task.return_value = None
        c2_client.poll_task.return_value = TaskPayload(
            task_id="t1", job_id="j1", type="test-skill", params={}, tracing_context={}
        )

        with patch.object(worker, "_process_task", return_value=AsyncMock()):
            # Simulating FAILOVER/Default loop logic: polls all until busy or end of list
            for _, client in worker._clients:
                if worker._get_current_state()["status"] == "busy":
                    break
                await worker._poll_for_tasks_with_status(client)

            assert c1_client.poll_task.called
            assert c2_client.poll_task.called
            assert worker._current_load == 1
