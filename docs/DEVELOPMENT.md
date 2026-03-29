EN | [ES](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/es/DEVELOPMENT.md) | [RU](https://github.com/avtomatika-ai/avtomatika-worker/blob/main/docs/ru/DEVELOPMENT.md)

# Worker Development Guide

This document describes how to create a custom Worker compatible with the Orchestrator using `avtomatika-worker`.

**Requirements:** Python 3.11 or higher.

## Core Concept

Workers created with the SDK implement a hybrid interaction model with the Orchestrator:
- **PULL Model for Task Fetching:** The worker initiates the connection to the Orchestrator and "pulls" tasks from its personal queue. This allows Workers to operate from any network, including behind NAT or corporate firewalls, without needing a public IP address.
- **WebSocket for Real-time Communication:** An optional bidirectional channel for receiving commands (e.g., task cancellation) and sending intermediate execution progress.
- **HLN Optimization:** The SDK uses the **Reverse Axon (RXON)** protocol, which reduces traffic by hashing skill lists and only sending updates when changes occur.
- **Robust Connectivity:** 
    - **Independent Orchestrators:** Each orchestrator connection is managed by a separate task. A failure of one server doesn't block communications with others.
    - **Registration Retries:** Infinite retries with exponential backoff if an orchestrator is offline.
    - **Non-blocking Startup:** The worker starts polling tasks as soon as it connects to at least one orchestrator.

## How to Create a Worker with the SDK

### Step 1: Install `avtomatika-worker`

Ensure the SDK is installed in your environment. Recommended for full features (S3 and Pydantic):
```bash
pip install "avtomatika-worker[s3,pydantic,metrics]"
```

If you are working in the main repository, you can install it in editable mode:
```bash
pip install -e .[dev]
```

### Step 2: Create a Worker File

Create a Python file (e.g., `my_worker.py`) and import the `Worker` class. The SDK uses **Automatic Inference** to reduce boilerplate code.

```python
import asyncio
from avtomatika_worker import Worker
from pydantic import BaseModel

# 1. Initialize the Worker class
worker = Worker(worker_type="my-custom-worker")

# 2. Define data models for your skills
class ReportParams(BaseModel):
    data_source: str
    format: str = "pdf"

# 3. Define skill handlers using the @worker.skill decorator
# The SDK automatically infers:
# - name: "generate_report" (from function name)
# - input_schema: generated from ReportParams
@worker.skill(description="Generates complex reports")
async def generate_report(params: ReportParams, send_progress, send_event, **kwargs) -> dict:
    """
    - `params` (ReportParams): Validated and typed parameters. 
      IMPORTANT: The argument MUST be named 'params' for automatic schema inference to work.
    - `send_progress`: Async function to send progress updates.
    - `send_event`: Async function to emit custom events.
    - `**kwargs`: Metadata: task_id, job_id, etc.
    """
    task_id = kwargs.get("task_id")

    print(f"Generating {params.format} report from {params.data_source}")

    # Send progress (standard event)
    await send_progress(progress=0.5, message="Processing data...")
    
    # Send custom event
    await send_event("milestone", {"name": "data_parsed"})

    return {
        "status": "success",
        "data": {"report_url": f"s3://bucket/reports/{task_id}.pdf"}
    }

# Dynamic Field Extension: add 'price' for the Marketplace
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Sending email: {params}")
    return {"status": "success"}

# 4. Run the worker
if __name__ == "__main__":
    worker.run()
```

### Step 3: Run the Worker

You can run the worker directly via Python or use the built-in CLI for better control:

```bash
# Recommended: runs worker and enables health-check server (default port 8083)
worker run --app my_worker:worker

# For development (auto-restarts on code changes)
worker run --app my_worker:worker --reload
```

### Step 4: Connection and Authentication Configuration

#### Option 1: Simple Connection (Single Orchestrator)

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_TOKEN=a-super-secret-token-for-this-worker
```

#### Option 2: Advanced Connection (Multiple Orchestrators)

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080", "priority": 1, "weight": 5},
    {"url": "http://backup-orchestrator:8080", "priority": 2, "weight": 1}
]'
MULTI_ORCHESTRATOR_MODE=WATERFALL  # Or ROUND_ROBIN, FAILOVER
```

- **WATERFALL (Default):** Polls orchestrators in order of priority. Always returns to the highest-priority one after any task.
- **ROUND_ROBIN:** Distributes requests based on weights.
- **FAILOVER:** Polls the next one only if the previous is empty.

### Step 5: Real-time Communication (WebSocket)

To enable this functionality, set `WORKER_ENABLE_WEBSOCKETS=true`. This allows you to:
1.  **Send Progress and Events:** Use the injected `send_progress` and `send_event` functions.
2.  **Task Cancellation:** The Orchestrator can send a command that will instantly raise an `asyncio.CancelledError` in your handler.


### Step 6: Modular Skills (SkillBlueprint)

Organize tasks into modules in the `skills/` directory.

`skills/image_skills.py`:
```python
from avtomatika_worker import SkillBlueprint
from pydantic import BaseModel

class ResizeParams(BaseModel):
    w: int
    h: int

bp = SkillBlueprint()

@bp.skill() # name="resize", schema from ResizeParams
async def resize(params: ResizeParams):
    return {"status": "success"}
```

The Worker will automatically load all skills from the directory specified in `WORKER_SKILLS_DIR`.

### Step 7: Working with Large Files (S3 Offloading)

The SDK supports **"Payload Offloading"** via S3-compatible storage using the high-performance **`obstore`** library.

1.  **Auto-Download:** If `params` contains an `s3://` URI, the SDK downloads it to a local temporary folder before calling your handler.
2.  **Auto-Upload:** If your handler returns a local path, the SDK uploads it to S3 and returns the URI to the Orchestrator.
3.  **TaskFiles:** Use the `TaskFiles` class for easy async file operations in the task's isolated directory.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' in params might be an S3 URI, now replaced with local path
    local_path = params["video_url"]
    
    # Create result file
    result_path = await files.path_to("output.mp4")
    # ... process ...
    
    return {"status": "success", "data": {"result": result_path}}
```

#### S3 Configuration
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.
- `TASK_FILES_DIR`: Local root for temporary data (default: `/tmp/payloads`).

> **Note:** The SDK automatically cleans up the entire task directory after the task completes.

### Step 8: Observability (OpenTelemetry)

The SDK provides integrated support for **distributed tracing** and **metrics** using OpenTelemetry.

1.  **Distributed Tracing:** Every task execution is wrapped in a Span (`task.{type}`). S3 operations are child spans.
2.  **Metrics:** Prometheus-compatible metrics are available at `http://localhost:8083/metrics` (requires `metrics` extra).
3.  **Dependency Injection:** You can request the `ObservabilityManager` in your skill handler to create custom spans.

```python
from avtomatika_worker import Worker, ObservabilityManager

@worker.skill()
async def monitored_task(params: dict, obs: ObservabilityManager):
    with obs.tracer.start_as_current_span("my-custom-step"):
        # ... logic ...
        pass
    return {"status": "success"}
```

Enable it via environment variable: `WORKER_ENABLE_METRICS=true`.

### Step 9: Health Checks

By default, the SDK starts a small aiohttp server on `0.0.0.0:8083`. You can check the worker status at `/health`.
This is useful for Kubernetes (Liveness/Readiness probes) or monitoring systems.
- Variable: `WORKER_PORT` (default: 8083)
- CLI flag: `--health-check` (enabled by default)
