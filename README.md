EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/README.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/README.md)

# Avtomatika Worker SDK

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

The official SDK for creating workers compatible with the **[Avtomatika Orchestrator](https://github.com/avtomatika-ai/avtomatika)**. It handles polling, heartbeats, S3 payload offloading, and graceful shutdown so you can focus on your business logic.

## Installation

```bash
pip install avtomatika-worker
```

Extras:
- `pip install "avtomatika-worker[pydantic]"` — for Pydantic-based parameter validation.
- `pip install "avtomatika-worker[dev]"` — for development features like CLI `--reload`.

## Quick Start

### Option 1: CLI Usage (Recommended)

Define your worker in a Python module (e.g., `app/main.py`):

```python
from avtomatika_worker import Worker

worker = Worker(worker_type="image-processor")

@worker.task("resize")
async def resize_image(params: dict, **kwargs):
    return {"status": "success", "data": {"result": "ok"}}
```

Run it using the built-in `worker` command:

```bash
export ORCHESTRATOR_URL="http://localhost:8080"
export WORKER_TOKEN="your-secret-token"

# Standard run
worker run --app app.main:worker

# Development mode with auto-reload
worker run --app app.main:worker --reload
```

### Option 2: Programmatic Run

```python
if __name__ == "__main__":
    worker.run_with_health_check()
```

## Key Features

### 1. Structured Logging
The SDK supports both human-readable and JSON logging.
- `LOG_FORMAT=json` — for production (ELK, Grafana Loki).
- `LOG_FORMAT=text` — for development (default).
- All logs automatically include `worker_id`, `task_id`, and `job_id` context.

### 2. Graceful Shutdown
Built-in handling of `SIGTERM` and `SIGINT`. When a signal is received, the worker:
1. Enters "Drain Mode" (stops taking new tasks).
2. Waits for active tasks to complete (configurable via `WORKER_SHUTDOWN_TIMEOUT`).
3. Sends final heartbeats and closes connections.

### 3. File System & S3 Offloading
- **TaskFiles**: Async helper for isolated task workspaces.
- **S3 Payload Offloading**: Automatic download/upload of large files via S3 URIs in task parameters.

## Configuration Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKER_ID` | Unique identifier for the worker instance. | UUID |
| `ORCHESTRATOR_URL` | Address of the orchestrator. | `http://localhost:8080` |
| `LOG_FORMAT` | Log format: `text` or `json`. | `text` |
| `LOG_LEVEL` | Minimum log level (DEBUG, INFO, etc). | `INFO` |
| `WORKER_SHUTDOWN_TIMEOUT`| Max seconds to wait for tasks during shutdown. | `30.0` |
| `WORKER_ENABLE_WEBSOCKETS`| Enable real-time commands (e.g., cancellation). | `false` |
| `TASK_FILES_DIR` | Local directory for temporary S3 payloads. | `/tmp/payloads` |

## Documentation

- [Development Guide](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/DEVELOPMENT.md) — Detailed instructions on how to create custom workers, use middlewares, and handle S3 offloading.

## Docker Usage

Use the provided `Dockerfile` for easy deployment:

```bash
docker build -t my-worker .
docker run -e ORCHESTRATOR_URL=... my-worker worker run --app app:worker
```

## Development

Install development dependencies:
```bash
pip install -e .[test,dev]
pytest
```