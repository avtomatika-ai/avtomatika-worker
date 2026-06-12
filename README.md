# Avtomatika Worker SDK

Official SDK for building workers compatible with the **Avtomatika** orchestrator. It automates low-level tasks: polling, heartbeats, S3 payload management, and graceful shutdown.

## 🚀 Key Features

- **Language:** Python 3.11+
- **Protocol:** Based on **RXON** (Reverse Axon Protocol) for Hierarchical Logic Networks (Holarchy).
- **Communication Model:**
  - **PULL:** Workers poll tasks from orchestrators (works behind NAT/Firewall).
  - **WebSocket:** Real-time command channel (cancellation, custom commands).
- **Zero Trust Security:**
  - Mandatory HMAC SHA256 signing for all messages using `WORKER_TOKEN`.
  - Identity Chain and Origin Worker ID support for provenance tracking.
  - Replay protection with timestamp validation.
- **Traffic Optimization:**
  - **3-Tier Skills:** *Supported* (catalog), *Available* (dynamic limits), and *Hot* (cached).
  - **Stable Hashing:** Sends full skill catalog only when changed, using `skills_hash` for light heartbeats.
- **S3 Streaming:** High-performance data transfer using `obstore`. No OOM on large files.
- **Hardware Awareness:** Built-in monitoring for CPU, RAM, and NVIDIA GPUs (via `psutil` and `GPUtil`).
- **Observability:**
  - Built-in support for **OpenTelemetry** (traces and metrics).
  - Automatic **Trace Context Propagation**: Workers extract `trace_id` from tasks and inject it into events (including progress), ensuring end-to-end visibility in Jaeger/Honeycomb.
  - Automatic metrics export via OTLP (Push model) when `OTEL_EXPORTER_OTLP_ENDPOINT` is configured.

## 🛡 Resilience & Connectivity

- **Independent Managers:** Connection to each orchestrator is managed by a separate background task. One server failure or rate limit doesn't affect others.
- **Smart Backoff:** Unified exponential backoff for registration, polling, and heartbeats.
- **Rate Limit Protection:** Full support for `Retry-After` (seconds or HTTP-date). Implements a mandatory 30s safety floor for 429 errors without `Retry-After` to prevent Retry Storms.
- **Heartbeat Debouncing:** Throttles heartbeats to once every 2 seconds. Events are not lost but consolidated and sent after the cooldown period.
- **Infinite Retries:** Workers never stop trying to register with an exponential delay.
- **Graceful Shutdown:** Handles `SIGTERM` and `SIGINT` properly, waiting for active tasks to finish.

## 🛠 Installation

```bash
pip install avtomatika-worker[s3,pydantic]
```

For development:
```bash
pip install -e .[test,dev]
```

## 💻 Quick Start

```python
from avtomatika_worker import Worker, TaskFiles

worker = Worker()

@worker.skill("hello_world")
async def my_skill(params: dict, files: TaskFiles):
    """Simple skill that says hello."""
    return {"message": f"Hello, {params.get('name', 'World')}!"}

@worker.on_command("reboot")
async def handle_reboot(command):
    print("Rebooting worker...")

if __name__ == "__main__":
    worker.run()
```

## ⚙️ Configuration

Controlled via environment variables:
- `ORCHESTRATORS_CONFIG`: JSON list of orchestrator configs (URLs, priorities, weights).
- `ORCHESTRATOR_URL`: Simple fallback if only one orchestrator is used (default: `http://localhost:8080`).
- `WORKER_TOKEN`: Secret for HMAC signing (Zero Trust).
- `S3_ENDPOINT_URL`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_DEFAULT_BUCKET`: Storage settings for large payloads.
- `STRICT_EVENT_VALIDATION`: (Default: `True`) Validates events against schemas before emitting.
- `LOG_LEVEL`: Logging verbosity (DEBUG, INFO, WARNING, ERROR).
- `POLL_BACKOFF_INITIAL`: Initial delay (seconds) after a 429 error or network failure (default: `1.0`). Honors `Retry-After` header.
- `POLL_BACKOFF_MAX`: Maximum backoff delay (seconds) (default: `60.0`).
- `POLL_BACKOFF_FACTOR`: Multiplier for exponential backoff (default: `2.0`).
- `MAX_CONCURRENT_TASKS`: Global limit for concurrent task execution.

## 📜 License

Mozilla Public License v. 2.0.
