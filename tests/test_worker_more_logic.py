import asyncio
import sys
from json import JSONDecodeError
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
from aiohttp import ClientError, WSMsgType

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
async def test_poll_for_tasks_handles_non_204_status(mocker):
    """Tests that _poll_for_tasks sleeps on non-204, non-200 responses."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False
    get_response = mocker.AsyncMock(spec=aiohttp.ClientResponse)
    get_response.status = 500  # Some server error
    get_response.__aenter__.return_value = get_response
    session.get = mocker.MagicMock(return_value=get_response)
    worker = Worker(http_session=session)
    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)

    await worker._poll_for_tasks({"url": "http://test-orchestrator"})

    mock_sleep.assert_called_once_with(worker._config.TASK_POLL_ERROR_DELAY)


@pytest.mark.asyncio
async def test_send_result_retries_on_client_error(mocker):
    """Tests that _send_result retries sending the result on ClientError."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False
    session.post.side_effect = ClientError("Connection error")
    worker = Worker(http_session=session)
    worker._config.RESULT_MAX_RETRIES = 3
    worker._config.RESULT_RETRY_INITIAL_DELAY = 0.01
    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)

    payload = {"job_id": "j1", "task_id": "t1", "worker_id": "w1", "result": {}}
    await worker._send_result(payload, {"url": "http://test"})

    assert session.post.call_count == 3
    assert mock_sleep.call_count == 3


@pytest.mark.asyncio
async def test_websocket_manager_handles_connection_error(mocker):
    """
    Tests that the WebSocket manager handles connection errors and retries.
    """
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.ws_connect.side_effect = ClientError("Connection refused")
    worker = Worker(http_session=session)
    worker._config.ENABLE_WEBSOCKETS = True
    worker._config.ORCHESTRATORS = [{"url": "http://test-orchestrator"}]
    mock_sleep = mocker.patch("avtomatika_worker.worker.sleep", new_callable=mocker.AsyncMock)
    # To break the while loop
    mock_sleep.side_effect = asyncio.CancelledError
    worker._shutdown_event.clear()

    with pytest.raises(asyncio.CancelledError):
        await worker._start_websocket_manager()

    session.ws_connect.assert_called_once()
    mock_sleep.assert_called_once()


@pytest.mark.asyncio
async def test_process_task_permanent_error_on_unsupported_task(mocker):
    """
    Tests that a permanent error is returned for an unsupported task type.
    """
    worker = Worker()
    worker._send_result = mocker.AsyncMock()

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "unsupported_task",
        "params": {},
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    worker._send_result.assert_called_once()
    result_payload = worker._send_result.call_args.args[0]
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
    worker = Worker()
    worker._send_result = mocker.AsyncMock()

    @worker.task("validation_task")
    async def my_task(params: dict, **kwargs):
        raise ParamValidationError("Invalid params")

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "validation_task",
        "params": {},
        "orchestrator": {"url": "http://test"},
    }

    await worker._process_task(task_data)

    worker._send_result.assert_called_once()
    result = worker._send_result.call_args[0][0]["result"]
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

        # ws_connection will directly implement the async iterator protocol
        ws_connection = mocker.AsyncMock(spec=aiohttp.ClientWebSocketResponse)

        # Create a mock message that simulates aiohttp's message object
        mock_message = MagicMock()
        mock_message.type = WSMsgType.TEXT
        mock_message.json.side_effect = JSONDecodeError("Invalid JSON", doc="invalid json", pos=0)
        mock_message.data = "invalid json"

        mock_message_queue = [mock_message]

        async def anext_side_effect():
            if mock_message_queue:
                return mock_message_queue.pop(0)
            raise StopAsyncIteration

        ws_connection.__aiter__.return_value = ws_connection
        ws_connection.__anext__.side_effect = anext_side_effect

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
