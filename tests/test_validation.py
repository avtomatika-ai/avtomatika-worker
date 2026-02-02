import pytest

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


def test_config_validation_invalid_worker_id(monkeypatch):
    """Test that WorkerConfig raises ValueError when WORKER_ID contains invalid characters."""
    monkeypatch.setenv("WORKER_ID", "invalid/id")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    # Config init reads env vars immediately
    config = WorkerConfig()

    with pytest.raises(ValueError, match="Invalid WORKER_ID"):
        config.validate()


def test_config_validation_valid_worker_id(monkeypatch):
    """Test that WorkerConfig passes validation with a valid WORKER_ID."""
    monkeypatch.setenv("WORKER_ID", "valid-worker_123")
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    config = WorkerConfig()
    # Should not raise
    config.validate()


def test_task_decorator_validation_invalid_name(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    with pytest.raises(ValueError, match="Invalid task name"):

        @worker.task(name="invalid/task name")
        async def my_task(params, **kwargs):
            pass


def test_task_decorator_validation_invalid_type(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    with pytest.raises(ValueError, match="Invalid task type"):

        @worker.task(name="valid_task", task_type="invalid type!")
        async def my_task(params, **kwargs):
            pass


def test_task_decorator_validation_success(monkeypatch):
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")
    worker = Worker()

    @worker.task(name="valid_task", task_type="valid_type")
    async def my_task(params, **kwargs):
        pass

    assert "valid_task" in worker._task_handlers
