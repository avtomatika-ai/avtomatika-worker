import asyncio
import sys
from json import JSONDecodeError
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
from aiohttp import ClientError, WSMsgType

from avtomatika_worker.client import OrchestratorClient
from avtomatika_worker.types import INVALID_INPUT_ERROR, PERMANENT_ERROR
from avtomatika_worker.worker import ParamValidationError, Worker


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

    # Mock OrchestratorClient.register
    mocker.patch("avtomatika_worker.worker.OrchestratorClient.register", new_callable=mocker.AsyncMock)

    worker = Worker(http_session=session, worker_type="custom-type")
    worker._config.WORKER_ID = "custom-id"
    worker._config.INSTALLED_MODELS = [{"name": "model1"}]
    worker._config.COST_PER_SKILL = {"task1": 1.5}

    @worker.task("task1")
    def task1(params: dict):
        pass

    await worker._register_with_all_orchestrators()

    from avtomatika_worker.worker import OrchestratorClient

    OrchestratorClient.register.assert_called()
    payload = OrchestratorClient.register.call_args.args[0]

    assert payload["worker_id"] == "custom-id"
    assert payload["worker_type"] == "custom-type"
    assert "task1" in payload["supported_tasks"]
    assert payload["installed_models"] == [{"name": "model1"}]
    assert payload["cost_per_skill"] == {"task1": 1.5}


@pytest.mark.asyncio
async def test_poll_for_tasks_handles_non_204_status(mocker):
    """Tests that _poll_for_tasks handles errors from client."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    client.poll_task.return_value = None

    worker = Worker()

    await worker._poll_for_tasks(client)
    client.poll_task.assert_called_once()


@pytest.mark.asyncio
async def test_send_result_retries_on_client_error(mocker):
    """Tests that OrchestratorClient.send_result retries sending the result on ClientError."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    client = OrchestratorClient(session, "http://test", "w1", "token")

    session.post.side_effect = ClientError("Connection error")
    mocker.patch("avtomatika_worker.client.sleep", new_callable=mocker.AsyncMock)

    payload = {"result": {}}
    await client.send_result(payload, max_retries=3, initial_delay=0.01)

    assert session.post.call_count == 3


@pytest.mark.asyncio
async def test_websocket_manager_handles_connection_error(mocker):
    """
    Tests that the WebSocket manager handles connection errors and retries.
    """
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    worker = Worker(http_session=session)

    # Setup mock clients manually since we're in a test
    client = mocker.AsyncMock(spec=OrchestratorClient)
    client.connect_websocket.return_value = None
    worker._clients = [({"url": "http://test-orchestrator", "weight": 1}, client)]

    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)
    # To break the while loop
    mock_sleep.side_effect = asyncio.CancelledError
    worker._shutdown_event.clear()

    with pytest.raises(asyncio.CancelledError):
        await worker._start_websocket_manager()

    client.connect_websocket.assert_called_once()
    mock_sleep.assert_called_once()


@pytest.mark.asyncio
async def test_process_task_permanent_error_on_unsupported_task(mocker):
    """
    Tests that a permanent error is returned for an unsupported task type.
    """
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "unsupported_task",
        "params": {},
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    result_payload = client.send_result.call_args.args[0]
    assert result_payload["result"]["error"]["code"] == PERMANENT_ERROR


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
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()

    @worker.task("validation_task")
    async def my_task(params: dict, **kwargs):
        raise ParamValidationError("Invalid params")

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "validation_task",
        "params": {},
        "client": client,
        "orchestrator": {"url": "http://test"},
    }

    await worker._process_task(task_data)

    client.send_result.assert_called_once()
    result = client.send_result.call_args[0][0]["result"]
    assert result["error"]["code"] == INVALID_INPUT_ERROR
    assert "Invalid params" in result["error"]["message"]


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


@pytest.mark.asyncio
async def test_listen_for_commands_handles_invalid_json(mocker, caplog):
    """
    Tests that _listen_for_commands logs a warning on receiving invalid JSON.
    """
    worker = Worker()

    # Create a mock message
    mock_message = MagicMock()
    mock_message.type = WSMsgType.TEXT
    mock_message.json.side_effect = JSONDecodeError("Invalid JSON", doc="invalid json", pos=0)
    mock_message.data = "invalid json"

    # Define a custom async iterator class
    class MockAsyncIterator:
        def __init__(self, items):
            self.items = items

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.items:
                return self.items.pop(0)
            raise StopAsyncIteration

    # Mock the ws_connection object
    ws_connection = mocker.AsyncMock(spec=aiohttp.ClientWebSocketResponse)
    # The key fix: assign the iterator directly to the mock, so 'async for' uses it
    ws_connection.__aiter__.side_effect = lambda: MockAsyncIterator([mock_message])

    worker._ws_connection = ws_connection

    with caplog.at_level("WARNING"):
        await worker._listen_for_commands()

    assert "Received invalid JSON over WebSocket: invalid json" in caplog.text


@pytest.mark.asyncio
async def test_listen_for_commands_handles_ws_error(mocker):
    """
    Tests that _listen_for_commands breaks the loop on a WSMsgType.ERROR.
    """
    worker = Worker()
    ws_connection = mocker.AsyncMock()

    mock_error_message = MagicMock()
    mock_error_message.type = WSMsgType.ERROR

    class MockAsyncIterator:
        def __init__(self):
            self.messages = [mock_error_message]

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.messages:
                return self.messages.pop(0)
            raise StopAsyncIteration

    ws_connection.__aiter__.return_value = MockAsyncIterator()
    worker._ws_connection = ws_connection

    # This should not raise an exception
    await worker._listen_for_commands()


@pytest.mark.asyncio
async def test_send_progress_handles_exception(mocker, caplog):
    """
    Tests that send_progress logs a warning if sending the progress update fails.
    """
    worker = Worker()
    ws_connection = mocker.AsyncMock()
    ws_connection.closed = False
    ws_connection.send_json.side_effect = Exception("Connection lost")
    worker._ws_connection = ws_connection

    with caplog.at_level("WARNING"):
        await worker.send_progress("t1", "j1", 0.5)

    assert "Could not send progress update for task t1: Connection lost" in caplog.text
