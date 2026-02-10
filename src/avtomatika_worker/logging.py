from logging import Filter, Formatter, LogRecord, StreamHandler, getLogger
from os import getenv
from sys import stdout
from typing import Any

from pythonjsonlogger import json as jsonlogger


class ContextFilter(Filter):
    """
    Filter for adding context (worker_id, task_id, job_id) to every log record.
    """

    def __init__(self) -> None:
        super().__init__()
        self.context: dict[str, Any] = {}

    def filter(self, record: LogRecord) -> bool:
        for key, value in self.context.items():
            setattr(record, key, value)
        return True


# Global context filter
_context_filter = ContextFilter()


def setup_logging(worker_id: str | None = None) -> None:
    """
    Configures logging for the entire application.
    """
    log_level = getenv("LOG_LEVEL", "INFO").upper()
    log_format = getenv("LOG_FORMAT", "text").lower()

    handler = StreamHandler(stdout)

    if log_format == "json":
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(worker_id)s %(task_id)s %(job_id)s"
        )
    else:
        formatter = Formatter("[%(asctime)s] %(levelname)s in %(name)s: %(message)s")

    handler.setFormatter(formatter)
    handler.addFilter(_context_filter)

    root_logger = getLogger()

    # Clear existing handlers if any
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)

    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    if worker_id:
        set_context(worker_id=worker_id)


def set_context(**kwargs: Any) -> None:
    """
    Sets the context that will be added to all subsequent logs.
    """
    _context_filter.context.update(kwargs)


def clear_context(*keys: str) -> None:
    """
    Removes specific keys from the context. If no keys are specified, clears the entire context.
    """
    if not keys:
        _context_filter.context.clear()
    else:
        for key in keys:
            _context_filter.context.pop(key, None)
