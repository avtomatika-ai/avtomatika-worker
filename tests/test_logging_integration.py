import io
import json
import logging

import pytest
from rxon.testing import MockTransport

from avtomatika_worker.logging import _context_filter, clear_context, set_context, setup_logging
from avtomatika_worker.worker import Worker


def test_setup_logging_text(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "text")
    setup_logging(worker_id="test-worker")

    root_logger = logging.getLogger()
    handler = root_logger.handlers[0]
    assert isinstance(handler.formatter, logging.Formatter)


def test_context_filter():
    _context_filter.context = {}
    set_context(test_key="test_value")

    record = logging.LogRecord("name", logging.INFO, "pathname", 10, "message", None, None)
    _context_filter.filter(record)

    assert record.test_key == "test_value"

    clear_context("test_key")
    record2 = logging.LogRecord("name", logging.INFO, "pathname", 10, "message", None, None)
    _context_filter.filter(record2)
    assert not hasattr(record2, "test_key")


@pytest.mark.asyncio
async def test_worker_logging_context(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")

    # Clear loggers before test
    root_logger = logging.getLogger()
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)

    transport = MockTransport(worker_id="worker-1")
    # Capture output via StreamHandler or check record in ContextFilter

    worker = Worker(worker_type="test", clients=[({"url": "http://test", "weight": 1}, transport)])

    # Setup log capture after worker initialization
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    # Use the same format as in setup_logging
    from pythonjsonlogger import json as jsonlogger

    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(worker_id)s %(task_id)s %(job_id)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(_context_filter)
    root_logger.addHandler(handler)

    @worker.skill("test_task")
    async def task_handler(params, **kwargs):
        logging.getLogger("test").info("Inside task")
        return {"status": "success"}

    task_data = {
        "job_id": "job-123",
        "task_id": "task-456",
        "type": "test_task",
        "params": {},
        "tracing_context": {},
        "client": transport,
    }

    await worker._process_task(task_data)

    log_output = log_capture.getvalue().splitlines()
    # Find our log "Inside task"
    task_log = next(line for line in log_output if "Inside task" in line)
    log_data = json.loads(task_log)

    assert log_data["task_id"] == "task-456"
    assert log_data["job_id"] == "job-123"
    assert log_data["message"] == "Inside task"
