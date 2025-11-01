# Avtomatika Worker SDK

This is an SDK for creating workers compatible with the **Avtomatika** orchestrator. The SDK handles all the complexity of interacting with the orchestrator, allowing you to focus on writing your business logic.

## Installation

```bash
pip install avtomatika-worker
```

## Quick Start

Creating a worker is simple. You instantiate the `Worker` class and then register your task-handling functions using the `@worker.task` decorator.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Create a worker instance
worker = Worker(
    worker_type="image-processing",
    skill_dependencies={
        "resize_image": ["pillow"],
        "add_watermark": ["pillow", "numpy"],
    }
)

# 2. Register a task handler using the decorator
@worker.task("resize_image")
async def image_resizer(params: dict, **kwargs):
    """
    An example handler that receives task parameters,
    performs the work, and returns the result.
    """
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")

    print(f"Task {task_id} (Job: {job_id}): resizing image...")
    print(f"Parameters: {params}")

    # ... your business logic here ...
    await asyncio.sleep(1) # Simulate I/O-bound work

    # Return the result
    return {
        "status": "success",
        "data": {
            "resized_path": f"/path/to/resized_{params.get('filename')}"
        }
    }

# 3. Run the worker
if __name__ == "__main__":
    # The SDK will automatically connect to the orchestrator,
    # register itself, and start polling for tasks.
    worker.run_with_health_check()

```

## Key Features

### 1. Task Handlers

Each handler is an asynchronous function that accepts two arguments:

-   `params` (`dict`): A dictionary with the parameters that the orchestrator passed for this task.
-   `**kwargs`: Additional metadata about the task, including:
    -   `task_id` (`str`): The unique ID of the task itself.
    -   `job_id` (`str`): The ID of the parent `Job` to which the task belongs.
    -   `priority` (`int`): The execution priority of the task.

### 2. Concurrency Limiting

The worker allows you to control how many tasks are executed in parallel. This can be configured at two levels:

-   **Global Limit**: A maximum number of tasks that the worker can execute simultaneously, regardless of their type.
-   **Per-Type Limit**: A specific limit for a group of tasks that share a common resource (e.g., a GPU, a specific API).

The worker dynamically reports its available capacity to the orchestrator. When a limit is reached, the worker informs the orchestrator that it can no longer accept tasks of that type until a slot becomes free.

**Example:**

Let's configure a worker that can run up to **10 tasks in total**, but no more than **1 video processing task** and **4 audio transcription tasks** at the same time.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Configure limits during initialization
worker = Worker(
    worker_type="media-processor",
    max_concurrent_tasks=10,
    task_type_limits={
        "video_processing": 1,
        "audio_processing": 4,
    }
)

# 2. Assign a type to each task using the decorator
@worker.task("upscale_video", task_type="video_processing")
async def upscale_video(params: dict, **kwargs):
    # This task uses the 'video_processing' slot
    print("Upscaling video...")
    await asyncio.sleep(5)
    return {"status": "success"}

@worker.task("blur_video_faces", task_type="video_processing")
async def blur_video_faces(params: dict, **kwargs):
    # This task also uses the 'video_processing' slot
    print("Blurring faces in video...")
    await asyncio.sleep(5)
    return {"status": "success"}

@worker.task("transcribe_audio", task_type="audio_processing")
async def transcribe_audio(params: dict, **kwargs):
    # This task uses one of the four 'audio_processing' slots
    print("Transcribing audio...")
    await asyncio.sleep(2)
    return {"status": "success"}

@worker.task("generate_report")
async def generate_report(params: dict, **kwargs):
    # This task has no specific type and is only limited by the global limit
    print("Generating report...")
    await asyncio.sleep(1)
    return {"status": "success"}


if __name__ == "__main__":
    worker.run_with_health_check()
```
In this example, even though the global limit is 10, the orchestrator will only ever send one task (`upscale_video` or `blur_video_faces`) to this worker at a time, because they both share the single "video_processing" slot.

### 3. Returning Results and Handling Errors

The result returned by a handler directly influences the subsequent flow of the pipeline in the orchestrator.

#### Successful Execution

```python
return {
    "status": "success",
    "data": {"output": "some_value"}
}
```
- The orchestrator will receive this data and use the `"success"` key in the `transitions` dictionary to determine the next step.

#### Custom Statuses

You can return custom statuses to implement complex branching logic in the orchestrator.
```python
return {
    "status": "needs_manual_review",
    "data": {"reason": "Low confidence score"}
}
```
- The orchestrator will look for the `"needs_manual_review"` key in `transitions`.

#### Error Handling

To control the orchestrator's fault tolerance mechanism, you can return standardized error types.

-   **Transient Error (`TRANSIENT_ERROR`)**: For issues that might be resolved on a retry (e.g., a network failure).
    ```python
    from avtomatika_worker.typing import TRANSIENT_ERROR
    return {
        "status": "failure",
        "error": {
            "code": TRANSIENT_ERROR,
            "message": "External API timeout"
        }
    }
    ```
-   **Permanent Error (`PERMANENT_ERROR`)**: For unresolvable problems (e.g., an invalid file format).
    ```python
    from avtomatika_worker.typing import PERMANENT_ERROR
    return {
        "status": "failure",
        "error": {
            "code": PERMANENT_ERROR,
            "message": "Corrupted input file"
        }
    }
    ```

### 4. Failover and Load Balancing

The SDK supports connecting to multiple orchestrator instances to ensure high availability (`FAILOVER`) and load balancing (`ROUND_ROBIN`).

-   **Configuration**: Set via the `ORCHESTrators_CONFIG` environment variable, which must contain a JSON string.
-   **Mode**: Controlled by the `MULTI_ORCHESTRATOR_MODE` variable.

**Example `ORCHESTRATORS_CONFIG`:**
```json
[
    {"url": "http://orchestrator-1.my-domain.com:8080", "weight": 100},
    {"url": "http://orchestrator-2.my-domain.com:8080", "weight": 100}
]
```

-   **`FAILOVER` (default):** The worker will connect to the first orchestrator. If it becomes unavailable, it will automatically switch to the next one in the list.
-   **`ROUND_ROBIN`:** The worker will send requests to fetch tasks to each orchestrator in turn.

### 5. Handling Large Files (S3 Payload Offloading)

The SDK supports working with large files "out of the box" via S3-compatible storage.

-   **Automatic Download**: If a value in `params` is a URI of the form `s3://...`, the SDK will automatically download the file to the local disk and replace the URI in `params` with the local path.
-   **Automatic Upload**: If your handler returns a local file path in `data` (located within the `WORKER_PAYLOAD_DIR` directory), the SDK will automatically upload this file to S3 and replace the path with an `s3://` URI in the final result.

This functionality is transparent to your code and only requires configuring environment variables for S3 access.

### 6. WebSocket Support

If enabled, the SDK establishes a persistent WebSocket connection with the orchestrator to receive real-time commands, such as canceling an ongoing task.

## Advanced Features

### Reporting Skill & Model Dependencies

For more advanced scheduling, the worker can report detailed information about its skills and their dependencies on specific models. This allows the orchestrator to make smarter decisions, such as dispatching tasks to workers that already have the required models loaded in memory.

This is configured via the `skill_dependencies` argument in the `Worker` constructor.

-   **`skill_dependencies`**: A dictionary where keys are skill names (as registered with `@worker.task`) and values are lists of model names required by that skill.

Based on this configuration and the current state of the worker's `hot_cache` (the set of models currently loaded in memory), the worker will automatically include two new fields in its heartbeat messages:

-   **`skill_dependencies`**: The same dictionary provided during initialization.
-   **`hot_skills`**: A dynamically calculated list of skills that are ready for immediate execution (i.e., all of their dependent models are in the `hot_cache`).

**Example:**

Consider a worker configured like this:
```python
worker = Worker(
    worker_type="ai-processor",
    skill_dependencies={
        "image_generation": ["stable_diffusion_v1.5", "vae-ft-mse"],
        "upscale": ["realesrgan_x4"],
    }
)
```

-   Initially, `hot_cache` is empty. The worker's heartbeat will include `skill_dependencies` but not `hot_skills`.
-   A task handler calls `add_to_hot_cache("stable_diffusion_v1.5")`. The next heartbeat will still not include `hot_skills` because the `image_generation` skill is only partially loaded.
-   The handler then calls `add_to_hot_cache("vae-ft-mse")`. Now, all dependencies for `image_generation` are met. The next heartbeat will include:
    ```json
    {
      "hot_skills": ["image_generation"],
      "skill_dependencies": {
        "image_generation": ["stable_diffusion_v1.5", "vae-ft-mse"],
        "upscale": ["realesrgan_x4"]
      }
    }
    ```
This information is sent automatically. Your task handlers are only responsible for managing the `hot_cache` by calling `add_to_hot_cache()` and `remove_from_hot_cache()`, which are passed as arguments to the handler.

## Configuration

The worker is fully configured via environment variables.

| Variable | Description | Default |
| --- | --- | --- |
| `ORCHESTRATOR_URL` | The URL of a single orchestrator (used if `ORCHESTRATORS_CONFIG` is not set). | `http://localhost:8080` |
| `ORCHESTRATORS_CONFIG`| A JSON string with a list of orchestrators for `FAILOVER` or `ROUND_ROBIN` modes. | `[]` |
| `MULTI_ORCHESTRATOR_MODE` | The mode for handling multiple orchestrators. Possible values: `FAILOVER`, `ROUND_ROBIN`. | `FAILOVER` |
| `WORKER_ID` | **(Required)** A unique identifier for the worker. | - |
| `WORKER_TOKEN` | A common authentication token for all workers. | `default-token` |
| `WORKER_INDIVIDUAL_TOKEN` | An individual token for this worker (overrides `WORKER_TOKEN`). | - |
| `WORKER_ENABLE_WEBSOCKETS` | Enable (`true`) or disable (`false`) WebSocket support. | `false` |
| `WORKER_HEARTBEAT_DEBOUNCE_DELAY` | The delay in seconds for debouncing immediate heartbeats. | `0.1` |
| `WORKER_PAYLOAD_DIR` | The directory for temporarily storing files when working with S3. | `/tmp/payloads` |
| `S3_ENDPOINT_URL` | The URL of the S3-compatible storage. | - |
| `S3_ACCESS_KEY` | The access key for S3. | - |
| `S3_SECRET_KEY` | The secret key for S3. | - |
| `S3_DEFAULT_BUCKET`| The default bucket name for uploading results. | `avtomatika-payloads` |

## Development

To install the necessary dependencies for running tests, use the following command:

```bash
pip install .[test]
```
