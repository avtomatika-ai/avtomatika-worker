# Worker Development Guide

This document describes how to create a custom Worker compatible with the Avtomatika Orchestrator using the `avtomatika-worker` SDK.

**Requirements:** Python 3.11 or higher.

## Core Concept

Workers created with the SDK implement a hybrid interaction model with the Orchestrator:

- **PULL Model for Task Retrieval:** The worker initiates the connection to the Orchestrator and "pulls" tasks from its personal queue. This allows workers to operate from any network (including behind NAT or corporate firewalls) without needing a public IP address.
- **WebSocket for Real-time Communication:** An optional bidirectional channel to receive commands (e.g., task cancellation) and send intermediate execution progress.
- **HLN Optimization:** The SDK uses the **Reverse Axon (RXON)** protocol, which reduces traffic by hashing skill lists and sending updates only when changes occur.
- **Connection Resilience:**
  - **Independent Orchestrators:** Each orchestrator connection is managed by a separate background task. One server failure doesn't block communications with others.
  - **Registration Retries:** Infinite retries with exponential backoff if an orchestrator is offline.
  - **Non-blocking Startup:** The worker starts polling for tasks as soon as it successfully registers with at least one orchestrator.

## How to Build a Worker with the SDK

### Step 1: Install `avtomatika-worker`

Ensure the SDK is installed in your environment. Recommended for all features (S3 and Pydantic):

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
# - name: "generate_report" (from the function name)
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

> **Security Note (Zero Trust):** While the SDK automatically generates schemas based on your models, the **Orchestrator has the ultimate authority**. If a strict `output_schema` is defined in the blueprint, the orchestrator will filter out any fields in your result that do not comply with the "law of the blueprint." This protects the system from state injection attacks.

# Dynamic field extension: add 'price' for the Marketplace
@worker.skill(name="send_email", price=0.01)
async def send_email(params: dict, **kwargs) -> dict:
    print(f"Sending email: {params}")
    return {"status": "success"}

# 4. Run the worker
if __name__ == "__main__":
    worker.run()
```

### Step 3: Run the Worker

You can run the worker directly via Python or use the integrated CLI for better control:

```bash
# Recommended: runs the worker and enables the health check server (port 8083 by default)
worker run --app my_worker:worker

# For development (auto-reload on code changes)
worker run --app my_worker:worker --reload
```

### Step 4: Connection & Auth Configuration

#### Option 1: Simple Connection (Single Orchestrator)

```dotenv
ORCHESTRATOR_URL=http://localhost:8080
WORKER_ID=report-worker-01
WORKER_TOKEN=a-super-secret-token-for-this-worker
ORCHESTRATOR_SECRET_KEY=optional-secret-key-for-incoming-task-signature-verification
```

#### Option 2: Advanced Connection (Multiple Orchestrators)

```dotenv
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080", "priority": 1, "weight": 5},
    {"url": "http://backup-orchestrator:8080", "priority": 2, "weight": 1}
]'
MULTI_ORCHESTRATOR_MODE=WATERFALL  # Or ROUND_ROBIN, FAILOVER
```

#### Polling & Backoff Configuration (Stable Beta 15+)

To prevent "Retry Storms" during high load (429 errors) or network failures, the SDK uses an exponential backoff strategy. **Note:** The SDK honors the `Retry-After` header from the orchestrator, which takes precedence over local backoff calculations.

```dotenv
TASK_POLL_TIMEOUT=30        # Max seconds to wait for a task response
POLL_BACKOFF_INITIAL=1.0    # Initial delay (sec) after error
POLL_BACKOFF_MAX=60.0       # Maximum delay limit
POLL_BACKOFF_FACTOR=2.0     # Multiplier for each retry
```

- **WATERFALL (Default):** Polls orchestrators strictly in priority order. Always returns to the highest priority one after any task.
- **ROUND_ROBIN:** Distributes requests based on weights.
- **FAILOVER:** Polls the next one only if the previous was empty.

### Step 5: Real-time Communication (WebSocket)

To enable this feature, set `WORKER_ENABLE_WEBSOCKETS=true`. This allows you to:

1.  **Send Progress & Events:** Use the injected `send_progress` and `send_event` functions.
2.  **Task Cancellation:** The Orchestrator can send a command that will instantly trigger an `asyncio.CancelledError` in your handler.

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

The SDK supports **"Payload Offloading"** to S3-compatible storage using the high-performance **`obstore`** library.

1.  **Automatic Download (Input):** If `params` contains an `s3://` URI, the SDK downloads it to a local temporary folder before calling your handler.
2.  **Automatic Upload (Output):** If your handler returns a local path, the SDK uploads it to S3 and returns the URI to the Orchestrator.
3.  **TaskFiles:** Use the `TaskFiles` class for easy async file operations in the isolated task directory.

```python
from avtomatika_worker import Worker, TaskFiles

@worker.skill()
async def process_video(params: dict, files: TaskFiles):
    # 'video_url' in params might have been an S3 URI, now replaced with the local path
    local_path = params["video_url"]

    # Create result path
    result_path = await files.path_to("output.mp4")
    # ... process ...

    return {"status": "success", "data": {"result": result_path}}
```

#### S3 Configuration

- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`.
- `TASK_FILES_DIR`: Local root for temporary data (default: `/tmp/payloads`).

> **Note:** The SDK automatically cleans up the entire task directory once completed.

### Step 8: Observability (OpenTelemetry)

The SDK provides built-in support for **distributed tracing** and **metrics** via OpenTelemetry. This transforms the worker from a "black box" into a transparent node in the system.

1.  **End-to-End Tracing:** The worker automatically extracts the `trace_id` from incoming tasks and makes the skill execution a child span (`task.{type}`). All events and results are tied to the same trace.
2.  **Automatic Sub-spans:** S3 operations (upload/download) are automatically isolated into separate child spans.
3.  **Metrics:** Metrics are automatically exported to the OTLP collector (requires the `metrics` extra and `OTEL_EXPORTER_OTLP_ENDPOINT` variable).
4.  **Custom Spans (Internal Work):** You can request the `ObservabilityManager` in your handler to add detail to your internal logic.

```python
from avtomatika_worker import Worker, ObservabilityManager

@worker.skill()
async def monitored_task(params: dict, obs: ObservabilityManager):
    """
    Using the injected manager for deep monitoring.
    """
    # 1. Create a custom sub-span for a heavy stage
    with obs.tracer.start_as_current_span("heavy_model_inference") as span:
        span.set_attribute("model.name", "whisper-v3")
        # ... model work ...
        result = "processed data"

    # 2. Metrics and logs inside the span are tied to the Task Trace ID
    return {"status": "success", "data": result}
```

Enable via environment variable: `WORKER_ENABLE_METRICS=true`.

### Step 9: Dynamic Skill Management (Hot Skills)

In high-performance scenarios (e.g., AI model inference), you might want the Orchestrator to know which skills are "hot" (already loaded into GPU memory or cache). This allows for instant task execution without loading delays.

The SDK provides `add_to_hot_skills` and `remove_from_hot_skills` functions that are injected into your skill handlers.

```python
@worker.skill()
async def heavy_ai_task(params: dict, add_to_hot_skills, **kwargs):
    # 1. Load your model if needed
    model = await load_model("my_large_model")

    # 2. Mark this resource or skill name as 'hot'
    # This will be sent in the next heartbeat to the Orchestrator
    add_to_hot_skills("my_large_model")

    # 3. Process
    return {"status": "success"}
```

You can also use these methods on the `worker` instance directly: `worker.add_to_hot_skills("model_name")`.

### Step 10: Health Checks

By default, the SDK starts a small aiohttp server on `0.0.0.0:8083`. You can check the worker's status at `/health`.
This is useful for Kubernetes (Liveness/Readiness probes) or monitoring systems.

- Variable: `WORKER_PORT` (default: 8083)
- CLI flag: `--health-check` (enabled by default)

### Step 11: AI-Agent Tool Use (Subtask Delegation)

For AI Agents performing complex reasoning, you can request other skills (tools) from the orchestrator and await their results inline.

Request the `OrchestratorClient` dependency in your handler:

```python
from avtomatika_worker import Worker, OrchestratorClient

@worker.skill()
async def agent_reasoning(params: dict, orchestrator_client: OrchestratorClient):
    # Delegate a subtask (e.g. tool execution) to the orchestrator
    subtask_result = await orchestrator_client.call_skill(
        skill_name="web_search",
        params={"query": "latest news about AI"}
    )
    # Continue reasoning using the tool result
    return {"final_answer": f"Web search returned: {subtask_result['data']}"}
```
