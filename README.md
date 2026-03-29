EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/README.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/README.md)

# Avtomatika Worker SDK

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![PyPI version](https://img.shields.io/pypi/v/avtomatika-worker.svg)](https://pypi.org/project/avtomatika-worker/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/release/python-3110/)

The official SDK for creating workers compatible with the **[Avtomatika Orchestrator](https://github.com/avtomatika-ai/avtomatika)**. It handles polling, heartbeats, S3 payload offloading, and graceful shutdown so you can focus on your business logic.

## Installation

```bash
pip install avtomatika-worker
```

Recommended for full features:
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

Extras:
- `[s3]` — for S3 payload offloading (requires `obstore`).
- `[pydantic]` — for Pydantic-based parameter validation.
- `[metrics]` — for OpenTelemetry tracing and metrics.
- `[dev]` — for development features like CLI `--reload`.

## Quick Start

### Option 1: CLI Usage (Recommended)

Define your worker in a Python module (e.g., `app/main.py`). The SDK automatically infers skill names and schemas from your code!

```python
from avtomatika_worker import Worker
from pydantic import BaseModel

worker = Worker(worker_type="image-processor")

class ResizeParams(BaseModel):
    width: int
    height: int
    url: str

# Automatic: name="resize", schema from ResizeParams
@worker.skill()
async def resize(params: ResizeParams):
    print(f"Resizing to {params.width}px")
    return {"status": "success", "data": {"result": "ok"}}
```

### Option 2: Dynamic Skill Loading

Place your skill handlers in the `skills/` directory (e.g., `skills/my_skills.py`):

```python
from avtomatika_worker import SkillBlueprint

bp = SkillBlueprint()

# Add metadata for the Marketplace (optional)
@bp.skill(price=0.5, category="AI")
async def generate_preview(params: dict):
    return {"status": "success"}
```

Run the worker, and it will automatically load all skills from the directory:

```bash
# It will look into ./skills by default
worker run --app app.main:worker
```

## Key Features

### 1. Zero-Trust Security (HLN Identity Chain)
- **Cryptographic Signatures:** When `WORKER_TOKEN` is configured, the SDK automatically signs all registrations, heartbeats, task results, and events using HMAC SHA256. This ensures payload integrity and prevents unauthorized worker spoofing.

### 2. Smart Skill Registration
- **Zero Configuration:** Names and schemas are inferred from function names and type hints.
- **Auto-Contracts:** Both `input_schema` and `output_schema` are automatically generated from Pydantic models or standard Dataclasses.
- **Generic Events:** Declare custom events via `@worker.skill(events={"alert": Schema})` and emit them using the `send_event` helper. Progress is also a system event.

### 3. Optimized Network Traffic & Performance
- **Skills Hashing:** Workers only send the full skill list when it actually changes. Periodic heartbeats use a lightweight `skills_hash`.
- **Thundering Herd Prevention:** The SDK automatically applies `next_heartbeat_jitter_ms` provided by the Orchestrator to prevent network spikes when thousands of workers restart simultaneously.
- **Self-Healing Sync:** If the orchestrator loses worker metadata, it can trigger a `Full Sync` via heartbeat response, ensuring seamless recovery.

### 4. Multi-Orchestrator Support (Waterfall Priority)
- **Waterfall Strategy:** By default, the worker polls orchestrators in order of their priority. It always returns to the highest-priority orchestrator after completing any task, ensuring VIP tasks are handled first.
- **Failover & Round Robin:** Alternative strategies for load balancing and high availability.

### 4. Observability (OpenTelemetry)
- **Distributed Tracing:** Every task execution creates an OpenTelemetry Span. S3 operations are tracked as child spans, providing full visibility in Jaeger/Tempo.
- **Metrics:** Built-in Prometheus-compatible metrics for task count, duration, and S3 performance. Available at `http://localhost:8083/metrics` (if `[metrics]` extra is installed).

### 5. Fail-Fast Validation
- **Local Enforcement:** The SDK validates task results and events against their declared schemas locally. Errors are logged immediately, preventing the transmission of "broken" data.

### 6. Structured Logging
The SDK supports both human-readable and JSON logging.
- `LOG_FORMAT=json` — for production (ELK, Grafana Loki).
- `LOG_FORMAT=text` — for development (default).
- All logs automatically include `worker_id`, `task_id`, and `job_id` context.

### 7. File System & S3 Reliability
- **TaskFiles**: Async helper for isolated task workspaces.
- **S3 SDK**: High-performance async uploads/downloads with automatic retries and **Graceful Shutdown** (waits for pending uploads before exit).

## Configuration Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKER_ID` | Unique identifier for the worker instance. | UUID |
| `ORCHESTRATOR_URL` | Address of the orchestrator. | `http://localhost:8080` |
| `LOG_FORMAT` | Log format: `text` or `json`. | `text` |
| `LOG_LEVEL` | Minimum log level (DEBUG, INFO, etc). | `INFO` |
| `WORKER_PORT` | Port for health-check server. | `8083` |
| `WORKER_SHUTDOWN_TIMEOUT`| Max seconds to wait for tasks during shutdown. | `30.0` |
| `WORKER_ENABLE_WEBSOCKETS`| Enable real-time commands (e.g., cancellation). | `false` |
| `MULTI_ORCHESTRATOR_MODE` | Polling strategy: `WATERFALL`, `ROUND_ROBIN`, `FAILOVER`. | `WATERFALL` |
| `WORKER_ENABLE_METRICS` | Enable OpenTelemetry metrics and tracing. | `false` |
| `REGISTRATION_RETRY_INITIAL_DELAY`| Initial delay for registration retries (sec). | `1.0` |
| `REGISTRATION_RETRY_MAX_DELAY`| Maximum delay for registration retries (sec). | `60.0` |
| `TASK_FILES_DIR` | Local directory for temporary S3 payloads. | `/tmp/payloads` |
| `WORKER_SKILLS_DIR` | Directory to dynamically load skills from. | `skills` |

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
pip install -e .[dev]
pytest
```