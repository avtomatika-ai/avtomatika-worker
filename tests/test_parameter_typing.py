from dataclasses import dataclass

import pytest
from pydantic import BaseModel, Field

from avtomatika_worker.types import INVALID_INPUT_ERROR
from avtomatika_worker.worker import Worker


# --- Test Setup ---
@dataclass
class SimpleDataclass:
    message: str
    count: int


@dataclass
class DataclassWithValidation:
    name: str
    age: int

    def __post_init__(self):
        if self.age < 18:
            raise ValueError("Age must be 18 or over.")


class PydanticModel(BaseModel):
    name: str
    value: float = Field(gt=0, description="Value must be positive")


# --- Tests ---


@pytest.mark.asyncio
async def test_process_task_with_default_dict(mocker):
    """Tests that a handler with a standard `dict` annotation receives the raw dict."""
    worker = Worker()
    worker._send_result = mocker.AsyncMock()

    received_params = None

    @worker.task("dict_task")
    async def my_handler(params: dict, **kwargs):
        nonlocal received_params
        received_params = params
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "dict_task",
        "params": {"key": "value"},
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert received_params == {"key": "value"}
    worker._send_result.assert_called_once()
    assert worker._send_result.call_args[0][0]["result"]["status"] == "success"


@pytest.mark.asyncio
async def test_process_task_with_simple_dataclass_success(mocker):
    """Tests successful instantiation of a simple dataclass."""
    worker = Worker()
    worker._send_result = mocker.AsyncMock()
    received_params = None

    @worker.task("dataclass_task")
    async def my_handler(params: SimpleDataclass, **kwargs):
        nonlocal received_params
        received_params = params
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "dataclass_task",
        "params": {"message": "hello", "count": 10},
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert isinstance(received_params, SimpleDataclass)
    assert received_params.message == "hello"
    assert received_params.count == 10
    worker._send_result.assert_called_once()


@pytest.mark.asyncio
async def test_process_task_with_dataclass_validation_failure(mocker):
    """Tests that a validation error in a dataclass's __post_init__ is caught."""
    worker = Worker()
    worker._send_result = mocker.AsyncMock()

    @worker.task("dataclass_validation_task")
    async def my_handler(params: DataclassWithValidation, **kwargs):
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "dataclass_validation_task",
        "params": {"name": "test", "age": 16},  # Invalid age
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    worker._send_result.assert_called_once()
    result = worker._send_result.call_args[0][0]["result"]
    assert result["status"] == "failure"
    assert result["error"]["code"] == INVALID_INPUT_ERROR
    assert "Age must be 18 or over" in result["error"]["message"]


@pytest.mark.asyncio
async def test_process_task_with_pydantic_success(mocker):
    """Tests successful validation and instantiation of a Pydantic model."""
    worker = Worker()
    worker._send_result = mocker.AsyncMock()
    received_params = None

    @worker.task("pydantic_task")
    async def my_handler(params: PydanticModel, **kwargs):
        nonlocal received_params
        received_params = params
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "pydantic_task",
        "params": {"name": "test", "value": 123.45},
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert isinstance(received_params, PydanticModel)
    assert received_params.name == "test"
    assert received_params.value == 123.45
    worker._send_result.assert_called_once()


@pytest.mark.asyncio
async def test_process_task_with_pydantic_validation_failure(mocker):
    """Tests that a Pydantic validation error is caught."""
    worker = Worker()
    worker._send_result = mocker.AsyncMock()

    @worker.task("pydantic_validation_task")
    async def my_handler(params: PydanticModel, **kwargs):
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "pydantic_validation_task",
        "params": {"name": "test", "value": -5},  # Invalid value
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    worker._send_result.assert_called_once()
    result = worker._send_result.call_args[0][0]["result"]
    assert result["status"] == "failure"
    assert result["error"]["code"] == INVALID_INPUT_ERROR
    assert "Input should be greater than 0" in result["error"]["message"]
