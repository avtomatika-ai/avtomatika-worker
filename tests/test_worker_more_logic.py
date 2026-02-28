# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import asyncio
import sys
from unittest.mock import patch

import pytest
from rxon import Transport
from rxon.constants import ERROR_CODE_INVALID_INPUT
from rxon.constants import ERROR_CODE_PERMANENT as PERMANENT_ERROR
from rxon.testing import MockTransport

from avtomatika_worker.types import ParamValidationError
from avtomatika_worker.worker import Worker


def test_pydantic_not_installed():
    """
    Tests that the worker initializes correctly when pydantic is not installed.
    """
    with patch.dict(sys.modules, {"pydantic": None}):
        from importlib import reload

        from avtomatika_worker import worker

        reload(worker)
        assert not worker._PYDANTIC_INSTALLED
    reload(worker)


@pytest.mark.filterwarnings("ignore:coroutine 'AsyncMockMixin._execute_mock_call' was never awaited:RuntimeWarning")
def test_skill_decorator_warns_on_undefined_type(mocker):
    """
    Tests that the skill decorator logs a warning if a skill_type is not in skill_type_limits.
    """
    mocker.patch("avtomatika_worker.worker.S3Manager")
    logger_mock = mocker.patch("avtomatika_worker.worker.logger")
    worker = Worker(skill_type_limits={"gpu": 1})

    @worker.skill("test_task", type="cpu")
    def my_task(params: dict):
        pass

    logger_mock.warning.assert_called_with(
        "Skill 'test_task' has a type 'cpu' which is not defined in 'skill_type_limits'. "
        "No concurrency limit will be applied for this type."
    )


@pytest.mark.asyncio
async def test_worker_registration_payload(mocker):
    """Tests that the registration payload contains all expected fields."""
    mock_transport = MockTransport(worker_id="test-worker")

    worker = Worker(worker_type="custom-type", clients=[({"url": "http://test", "weight": 1}, mock_transport)])
    worker._config.WORKER_ID = "custom-id"
    worker._config.INSTALLED_ARTIFACTS = [{"name": "model1", "version": "1.0"}]
    worker._config.COST_PER_SKILL = {"task1": 1.5}

    @worker.skill("task1")
    def task1(params: dict):
        pass

    await worker._register_with_all_orchestrators()

    assert len(mock_transport.registered) == 1
    registration = mock_transport.registered[0]

    assert registration.worker_id == "custom-id"
    assert registration.worker_type == "custom-type"
    # Skill registration returns a list of SkillInfo objects, check name
    assert any(s.name == "task1" for s in registration.supported_skills)
    assert registration.installed_artifacts[0].name == "model1"
    assert registration.capabilities.cost_per_skill == {"task1": 1.5}


@pytest.mark.asyncio
async def test_poll_for_tasks_handles_non_204_status(mocker):
    """Tests that _poll_for_tasks handles errors from client."""
    client = mocker.AsyncMock(spec=Transport)
    client.poll_task.return_value = None

    worker = Worker()

    @worker.skill("dummy_task")
    def dummy_handler(params):
        pass

    await worker._poll_for_tasks(client)
    client.poll_task.assert_called_once()


@pytest.mark.asyncio
async def test_websocket_manager_handles_connection_error(mocker):
    """
    Tests that _listen_to_single_transport handles connection errors and retries (sleeps).
    """
    worker = Worker()
    client = mocker.AsyncMock(spec=Transport)

    # Mock listen_for_commands to raise exception
    client.listen_for_commands.side_effect = Exception("Connection failed")

    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)

    # We want to run the loop once, verify sleep is called, then exit.
    # We can side_effect sleep to raise CancelledError to exit the loop.
    mock_sleep.side_effect = asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await worker._listen_to_single_transport(client)

    client.listen_for_commands.assert_called()
    mock_sleep.assert_called_with(5)


@pytest.mark.asyncio
async def test_process_task_permanent_error_on_unsupported_task(mocker):
    """
    Tests that a permanent error is returned for an unsupported task type.
    """
    client = mocker.AsyncMock(spec=Transport)
    worker = Worker()

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "unsupported_task",
        "params": {},
        "tracing_context": {},
        "client": client,
    }
    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    result = client.send_result.call_args.args[0]
    assert result.status == "failure"
    assert result.error.code == PERMANENT_ERROR


@pytest.mark.asyncio
async def test_prepare_task_params_raises_validation_error_for_dataclass():
    """
    Tests that _prepare_task_params raises ParamValidationError for a dataclass with missing fields.
    """
    worker = Worker()

    from dataclasses import dataclass

    @dataclass
    class MyDataclass:
        a: int
        b: str

    @worker.skill("test_task")
    async def my_handler(params: MyDataclass):
        pass

    with pytest.raises(ParamValidationError):
        worker._prepare_task_params(my_handler, {"a": 1})


@pytest.mark.asyncio
async def test_process_task_handles_param_validation_error(mocker):
    """
    Tests that _process_task sends an INVALID_INPUT_ERROR when ParamValidationError is raised.
    """
    client = mocker.AsyncMock(spec=Transport)
    worker = Worker()

    @worker.skill("validation_task")
    async def my_task(params: dict, **kwargs):
        raise ParamValidationError("Invalid params")

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "validation_task",
        "params": {},
        "tracing_context": {},
        "client": client,
    }

    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    result = client.send_result.call_args[0][0]
    assert result.status == "failure"
    assert result.error.code == ERROR_CODE_INVALID_INPUT


def test_run_keyboard_interrupt(mocker):
    """Tests that run() handles KeyboardInterrupt gracefully."""
    worker = Worker()
    mocker.patch.object(worker, "main", side_effect=KeyboardInterrupt)
    mock_shutdown_set = mocker.patch.object(worker._shutdown_event, "set")

    worker.run()

    mock_shutdown_set.assert_called_once()


def test_run_with_health_check_keyboard_interrupt(mocker):
    """Tests that run_with_health_check() handles KeyboardInterrupt."""
    worker = Worker()
    mocker.patch.object(worker, "main", side_effect=KeyboardInterrupt)
    mock_shutdown_set = mocker.patch.object(worker._shutdown_event, "set")

    worker.run_with_health_check()

    mock_shutdown_set.assert_called_once()
