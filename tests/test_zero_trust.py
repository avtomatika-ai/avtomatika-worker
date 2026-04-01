# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from rxon.models import Heartbeat, TaskResult, WorkerRegistration
from rxon.security import sign_payload

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


@pytest.fixture
def secure_config():
    config = WorkerConfig()
    config.WORKER_ID = "secure-worker"
    config.WORKER_TOKEN = "super-secret-worker-token"
    return config


@pytest.fixture
def worker(secure_config):
    return Worker(config=secure_config)


def test_sign_payload_if_needed_strips_none(worker, secure_config):
    """Test that _sign_payload_if_needed strips None values before signing."""
    payload = {
        "field1": "value1",
        "field2": None,  # Should be stripped
        "nested": {
            "sub1": "value2",
            "sub2": None,  # Should be stripped
        },
    }

    expected_clean_payload = {"field1": "value1", "nested": {"sub1": "value2"}}
    expected_signature = sign_payload(expected_clean_payload, secure_config.WORKER_TOKEN)

    security = worker._sign_payload_if_needed(payload)
    assert security is not None
    assert security.signature == expected_signature
    assert security.signer_id == secure_config.WORKER_ID


def test_sign_payload_if_needed_ignores_fields(worker, secure_config):
    """Test that _sign_payload_if_needed properly ignores specified fields."""
    payload = {"field1": "value1", "bubbling_chain": ["proxy-1"]}

    expected_clean_payload = {"field1": "value1"}
    expected_signature = sign_payload(expected_clean_payload, secure_config.WORKER_TOKEN)

    security = worker._sign_payload_if_needed(payload, ignore_fields=["bubbling_chain"])
    assert security is not None
    assert security.signature == expected_signature


@pytest.mark.asyncio
async def test_create_registration_payload_has_zero_trust(worker):
    """Test that WorkerRegistration payload is fully signed and timestamped."""
    worker._active_skills = []

    reg_payload = worker._create_registration_payload()

    assert isinstance(reg_payload, WorkerRegistration)
    assert reg_payload.timestamp is not None
    assert abs(time.time() - reg_payload.timestamp) < 5.0

    assert reg_payload.security is not None
    assert reg_payload.security.signer_id == worker._config.WORKER_ID
    assert reg_payload.security.signature is not None


@pytest.mark.asyncio
async def test_create_heartbeat_payload_has_zero_trust(worker):
    """Test that Heartbeat payload is fully signed and timestamped."""
    worker._active_skills = []

    hb_payload = worker._create_heartbeat_payload()

    assert isinstance(hb_payload, Heartbeat)
    assert hb_payload.timestamp is not None
    assert abs(time.time() - hb_payload.timestamp) < 5.0

    assert hb_payload.security is not None
    assert hb_payload.security.signer_id == worker._config.WORKER_ID
    assert hb_payload.security.signature is not None


@pytest.mark.asyncio
async def test_task_result_success_has_zero_trust(worker):
    """Test that a successful TaskResult is signed and timestamped."""
    mock_client = MagicMock()
    mock_client.send_result = AsyncMock(return_value=True)
    worker._clients = [mock_client]

    # Mock a successful skill handler
    worker._skill_handlers["dummy_skill"] = {
        "func": AsyncMock(return_value={"status": "success", "data": {"res": 1}}),
        "info": MagicMock(events_schema={}, output_schema=None),
    }

    task_data_raw = {"job_id": "j1", "task_id": "t1", "type": "dummy_skill", "params": {}, "client": mock_client}

    await worker._process_task(task_data_raw)

    assert mock_client.send_result.called
    result_obj = mock_client.send_result.call_args[0][0]

    assert isinstance(result_obj, TaskResult)
    assert result_obj.status == "success"
    assert result_obj.timestamp is not None
    assert result_obj.security is not None
    assert result_obj.security.signer_id == worker._config.WORKER_ID


@pytest.mark.asyncio
async def test_event_has_zero_trust(worker):
    """Test that events are signed and timestamped correctly."""
    mock_client = MagicMock()
    mock_client.emit_event = AsyncMock(return_value=True)
    mock_client.send_result = AsyncMock(return_value=True)
    worker._clients = [mock_client]

    async def dummy_handler(send_event):
        await send_event("progress", {"percent": 50})
        return "ok"

    worker._skill_handlers["dummy_skill"] = {
        "func": dummy_handler,
        "info": MagicMock(events_schema={}, output_schema=None),
    }

    task_data_raw = {"job_id": "j1", "task_id": "t1", "type": "dummy_skill", "params": {}, "client": mock_client}

    # Mock process_result to avoid S3 calls
    worker._s3_manager.process_result = AsyncMock(return_value=({"res": "ok"}, {}))

    await worker._process_task(task_data_raw)

    assert mock_client.emit_event.called
    event_obj = mock_client.emit_event.call_args[0][0]

    assert event_obj.event_type == "progress"
    assert event_obj.timestamp is not None
    assert event_obj.security is not None
    assert event_obj.security.signer_id == worker._config.WORKER_ID


def test_sign_payload_if_needed_no_token():
    """Test that signing is skipped if token is dummy or empty."""
    config = WorkerConfig()
    config.WORKER_TOKEN = "your-secret-worker-token"  # Dummy
    worker = Worker(config=config)

    payload = {"a": 1}
    assert worker._sign_payload_if_needed(payload) is None

    config.WORKER_TOKEN = ""  # Empty
    worker = Worker(config=config)
    assert worker._sign_payload_if_needed(payload) is None


@pytest.mark.asyncio
async def test_task_result_error_has_zero_trust(worker):
    # We will simulate an error during _process_task
    # Because _process_task calls client.send_result, we mock the client.
    mock_client = MagicMock()
    mock_client.send_result = AsyncMock(return_value=True)
    worker._clients = [mock_client]

    # Provide a bad payload that causes a ValueError
    task_data_raw = {"job_id": "j1", "task_id": "t1", "type": "non_existent_skill", "params": {}, "client": mock_client}

    await worker._process_task(task_data_raw)

    # Verify send_result was called
    assert mock_client.send_result.called
    result_obj = mock_client.send_result.call_args[0][0]

    assert isinstance(result_obj, TaskResult)
    assert result_obj.status == "failure"
    assert result_obj.error is not None
    assert "Unsupported skill" in result_obj.error.message

    assert result_obj.timestamp is not None
    assert result_obj.security is not None
    assert result_obj.security.signer_id == worker._config.WORKER_ID
    assert result_obj.security.signature is not None
