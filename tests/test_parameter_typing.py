from dataclasses import dataclass

import pytest
from pydantic import BaseModel, Field

from avtomatika_worker.client import OrchestratorClient
from avtomatika_worker.worker import Worker


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
            raise ValueError("Must be at least 18")


class PydanticModel(BaseModel):
    name: str
    value: float = Field(gt=0)


@pytest.mark.asyncio
async def test_process_task_with_default_dict(mocker):
    """Tests that a handler with a standard `dict` annotation receives the raw dict."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()

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
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert received_params == {"key": "value"}


@pytest.mark.asyncio
async def test_process_task_with_simple_dataclass_success(mocker):
    """Tests successful instantiation of a simple dataclass."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()
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
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert isinstance(received_params, SimpleDataclass)
    assert received_params.message == "hello"
    assert received_params.count == 10


@pytest.mark.asyncio
async def test_process_task_with_dataclass_validation_failure(mocker):
    """Tests that a validation error in a dataclass's __post_init__ is caught."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()

    @worker.task("dataclass_validation_task")
    async def my_handler(params: DataclassWithValidation, **kwargs):
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "dataclass_validation_task",
        "params": {"name": "test", "age": 16},  # Invalid age
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    # Check if failure result was sent
    client.send_result.assert_called_once()
    payload = client.send_result.call_args.args[0]
    assert payload["result"]["status"] == "failure"
    assert payload["result"]["error"]["code"] == "INVALID_INPUT_ERROR"


@pytest.mark.asyncio
async def test_process_task_with_pydantic_success(mocker):
    """Tests successful validation and instantiation of a Pydantic model."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()
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
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    assert isinstance(received_params, PydanticModel)
    assert received_params.name == "test"
    assert received_params.value == 123.45


@pytest.mark.asyncio
async def test_process_task_with_pydantic_validation_failure(mocker):
    """Tests that a Pydantic validation error is caught."""
    client = mocker.AsyncMock(spec=OrchestratorClient)
    worker = Worker()

    @worker.task("pydantic_validation_task")
    async def my_handler(params: PydanticModel, **kwargs):
        return {"status": "success"}

    task_data = {
        "job_id": "j1",
        "task_id": "t1",
        "type": "pydantic_validation_task",
        "params": {"name": "test", "value": -5},  # Invalid value
        "client": client,
        "orchestrator": {"url": "http://test"},
    }
    await worker._process_task(task_data)

    # Check if failure result was sent
    client.send_result.assert_called_once()
    payload = client.send_result.call_args.args[0]
    assert payload["result"]["status"] == "failure"
    assert "validation" in payload["result"]["error"]["message"].lower()
