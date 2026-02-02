import asyncio
import sys
from unittest.mock import patch

import aiohttp
import pytest
from rxon import Transport
from rxon.constants import ERROR_CODE_INVALID_INPUT
from rxon.constants import ERROR_CODE_PERMANENT as PERMANENT_ERROR

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
def test_task_decorator_warns_on_undefined_type(caplog, mocker):
    """
    Tests that the task decorator logs a warning if a task_type is not in task_type_limits.
    """
    mocker.patch("avtomatika_worker.worker.S3Manager")
    worker = Worker(task_type_limits={"gpu": 1})
    with caplog.at_level("WARNING"):

        @worker.task("test_task", task_type="cpu")
        def my_task(params: dict):
            pass

    assert "Task 'test_task' has a type 'cpu' which is not defined in 'task_type_limits'" in caplog.text


@pytest.mark.asyncio
async def test_worker_registration_payload(mocker):
    """Tests that the registration payload contains all expected fields."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    # Mock Transport.register
    # We patch create_transport to return our mock transport
    mock_transport = mocker.AsyncMock(spec=Transport)
    mocker.patch("avtomatika_worker.worker.create_transport", return_value=mock_transport)

    worker = Worker(http_session=session, worker_type="custom-type")
    worker._config.WORKER_ID = "custom-id"
    worker._config.INSTALLED_MODELS = [{"name": "model1", "version": "1.0"}]
    worker._config.COST_PER_SKILL = {"task1": 1.5}

    @worker.task("task1")
    def task1(params: dict):
        pass

    await worker._register_with_all_orchestrators()

    mock_transport.register.assert_called()
    registration = mock_transport.register.call_args.args[0]

    assert registration.worker_id == "custom-id"
    assert registration.worker_type == "custom-type"
    assert "task1" in registration.supported_tasks
    assert registration.installed_models[0].name == "model1"
    assert registration.capabilities.cost_per_skill == {"task1": 1.5}


@pytest.mark.asyncio
async def test_poll_for_tasks_handles_non_204_status(mocker):
    """Tests that _poll_for_tasks handles errors from client."""
    client = mocker.AsyncMock(spec=Transport)
    client.poll_task.return_value = None

    worker = Worker()

    @worker.task("dummy_task")
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

    @worker.task("test_task")
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

    @worker.task("validation_task")
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
