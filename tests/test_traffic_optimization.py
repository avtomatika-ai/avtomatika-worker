# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin


import pytest

from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_skills_hashing_traffic_optimization():
    """
    Tests that the worker only sends full supported_skills when they change.
    This is the core of HLN traffic optimization.
    """
    worker = Worker(worker_type="hashing-test")

    # 1. Register a skill
    @worker.skill("skill-1")
    async def h1(params, **kwargs):
        pass

    # 2. First heartbeat -> Should contain full skills
    hb1 = worker._create_heartbeat_payload()
    assert hb1.supported_skills is not None
    assert len(hb1.supported_skills) == 1
    assert hb1.supported_skills[0].name == "skill-1"
    assert hb1.skills_hash is not None

    # Update state: simulate successful sync
    worker._last_synced_skills_hash = hb1.skills_hash

    # 3. Second heartbeat (no changes) -> Should NOT contain skills
    hb2 = worker._create_heartbeat_payload()
    assert hb2.supported_skills is None
    assert hb2.skills_hash == hb1.skills_hash

    # 4. Add another skill -> Hash changes -> Should contain ALL skills again
    @worker.skill("skill-2")
    async def h2(params, **kwargs):
        pass

    hb3 = worker._create_heartbeat_payload()
    assert hb3.supported_skills is not None
    assert len(hb3.supported_skills) == 2
    assert hb3.skills_hash != hb1.skills_hash

    # Update state again
    worker._last_synced_skills_hash = hb3.skills_hash

    # 5. Third heartbeat (no changes) -> Should NOT contain skills
    hb4 = worker._create_heartbeat_payload()
    assert hb4.supported_skills is None
    assert hb4.skills_hash == hb3.skills_hash


@pytest.mark.asyncio
async def test_skills_hashing_persistence_lifecycle():
    """
    Tests that last_synced_skills_hash is ONLY updated after a successful
    network response from the orchestrator.
    """
    from unittest.mock import AsyncMock

    worker = Worker(worker_type="lifecycle-test")

    @worker.skill("skill-1")
    async def h1(params, **kwargs):
        pass

    client = AsyncMock()

    # Disable cooldown for sequential calls
    worker._heartbeat_cooldown = 0

    # 1. Scenario: Orchestrator returns ERROR
    client.send_heartbeat.return_value = None  # Simulating failure or empty response

    await worker._send_single_heartbeat(client)
    assert worker._last_synced_skills_hash is None  # Should NOT be updated

    # 2. Scenario: Orchestrator returns SUCCESS
    client.send_heartbeat.return_value = {"status": "accepted"}

    await worker._send_single_heartbeat(client)
    assert worker._last_synced_skills_hash is not None  # Should BE updated
    assert worker._last_synced_skills_hash == worker._calculate_contract_hash(
        worker._get_current_state()["supported_skills"]
    )
