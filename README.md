# Avtomatika Worker SDK

This is an SDK for creating workers compatible with the **Avtomatika** orchestrator. The SDK handles all the complexity of interacting with the orchestrator, allowing you to focus on writing your business logic.

## Installation

```bash
pip install avtomatika-worker
```

For advanced validation features, you can install the SDK with the `pydantic` extra:
```bash
pip install "avtomatika-worker[pydantic]"
```

## Configuration

The worker is configured entirely through environment variables. Before running your worker, you need to set a few essential variables.

-   **`WORKER_ID`**: A unique name for your worker instance. If not provided, a random UUID will be generated.
-   **`ORCHESTRATOR_URL`**: The address of the Avtomatika orchestrator.
-   **`WORKER_TOKEN`**: An authentication token to connect to the orchestrator.

Here is an example of how you might set them in your shell:
```bash
export WORKER_ID="image-processor-worker-1"
export ORCHESTRATOR_URL="http://localhost:8080"
export WORKER_TOKEN="your-secret-token"
```

A complete list of all available configuration variables can be found in the **Full Configuration Reference** section at the end of this document.

## Programmatic Configuration (Advanced)

While using environment variables is the recommended approach, you can also configure the worker programmatically. This is useful for advanced scenarios, such as dynamic configuration or integration into larger applications.

The process supports partial configuration. When you create a `WorkerConfig` instance, it **first loads all settings from environment variables**. You can then override specific values in your code before passing the completed config object to the `Worker`.

**Note:** The attributes on the `WorkerConfig` object use `UPPERCASE_SNAKE_CASE` to mirror the corresponding environment variables.

### Example of Partial Configuration

Let's assume you have an environment variable set for the orchestrator URL:
```bash
export ORCHESTRATOR_URL="http://orchestrator.from.env:8080"
```

You can then write Python code to override other settings:
```python
import asyncio
from avtomatika_worker import Worker
from avtomatika_worker.config import WorkerConfig

# 1. Create a config object. It automatically reads ORCHESTRATOR_URL
#    from the environment variables at this step.
custom_config = WorkerConfig()

# 2. Programmatically override or set other attributes.
custom_config.WORKER_ID = "programmatic-worker-1"
custom_config.WORKER_TOKEN = "super-secret-token-from-code"
custom_config.MAX_CONCURRENT_TASKS = 5

# 3. Pass the final config object to the Worker.
#    It will use the values from your code (e.g., WORKER_ID)
#    and the values from the environment (e.g., ORCHESTRATORS).
worker = Worker(
    worker_type="special-cpu-worker",
    config=custom_config
)

@worker.task("do_work")
async def do_work(params: dict, **kwargs):
    # ...
    return {"status": "success"}

if __name__ == "__main__":
    worker.run_with_health_check()
```



## Quick Start

For quick testing and visibility during startup, you can add basic logging configuration to your worker script. This ensures that informational messages, including registration with the orchestrator, are printed to the console.

You can configure your worker either via environment variables (recommended for production) or directly in your Python code for quick testing or specialized setups.

### Option 1: Configure via Environment Variables (Recommended)

Save the following code as `my_worker.py`:
```python
import asyncio
import logging # Import logging
from avtomatika_worker import Worker

# Configure basic logging to see worker messages
logging.basicConfig(level=logging.INFO)

# 1. Create a worker instance.
#    The SDK automatically reads the configuration from environment variables.
worker = Worker(worker_type="image-processing")

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

After setting the required environment variables, you can run your worker.

**Example:**
```bash
export WORKER_ID="image-processor-worker-1"
export ORCHESTRATOR_URL="http://localhost:8080"
export WORKER_TOKEN="your-secret-token"

python my_worker.py
```

### Option 2: Configure Programmatically (Alternative)

For quick testing or if you prefer to define configuration directly in code for simple examples, you can create and pass a `WorkerConfig` object.

Save the following code as `my_worker_programmatic.py`:
```python
import asyncio
import logging # Import logging
from avtomatika_worker import Worker
from avtomatika_worker.config import WorkerConfig # Import WorkerConfig

# Configure basic logging to see worker messages
logging.basicConfig(level=logging.INFO)

# 1. Create and configure a WorkerConfig object
my_config = WorkerConfig()
my_config.WORKER_ID = "image-processor-worker-1-programmatic"
my_config.ORCHESTRATOR_URL = "http://localhost:8080"
my_config.WORKER_TOKEN = "your-secret-token" # Replace with your actual token

# 2. Create a worker instance, passing the configured object
worker = Worker(worker_type="image-processing", config=my_config)

# 3. Register a task handler using the decorator
@worker.task("resize_image")
async def image_resizer(params: dict, **kwargs):
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")

    print(f"Task {task_id} (Job: {job_id}): resizing image...")
    print(f"Parameters: {params}")

    await asyncio.sleep(1)
    return {
        "status": "success",
        "data": {
            "resized_path": f"/path/to/resized_{params.get('filename')}"
        }
    }

# 4. Run the worker
if __name__ == "__main__":
    worker.run_with_health_check()
```

Run your worker:
```bash
python my_worker_programmatic.py
```

## Defining Task Parameters

The SDK offers three ways to define and validate the `params` your task handler receives, giving you the flexibility to choose the right tool for your needs.

### 1. Default: `dict`

By default, or if you type-hint `params` as a `dict`, you will receive the raw dictionary of parameters sent by the orchestrator. This is simple and requires no extra definitions.

```python
@worker.task("resize_image")
async def image_resizer(params: dict, **kwargs):
    width = params.get("width")
    height = params.get("height")
    # ...
```

### 2. Structured: `dataclasses`

For better structure and IDE autocompletion, you can use Python's built-in `dataclasses`. The SDK will automatically instantiate the dataclass from the incoming parameters. You can access parameters as class attributes.

You can also add custom validation logic using the `__post_init__` method. If validation fails, the SDK will automatically catch the `ValueError` and report an `INVALID_INPUT_ERROR` to the orchestrator.

```python
from dataclasses import dataclass

@dataclass
class ResizeParams:
    width: int
    height: int

    def __post_init__(self):
        if self.width <= 0 or self.height <= 0:
            raise ValueError("Width and height must be positive.")

@worker.task("resize_image")
async def image_resizer(params: ResizeParams, **kwargs):
    # Access params with dot notation and autocompletion
    print(f"Resizing to {params.width}x{params.height}")
    # ...
```

### 3. Validated: `pydantic`

For the most robust validation and type coercion, you can use `pydantic`. First, install the necessary extra: `pip install "avtomatika-worker[pydantic]"`.

Define a `pydantic.BaseModel` for your parameters. The SDK will automatically validate the incoming data against this model. If validation fails, the detailed error message from Pydantic will be sent to the orchestrator.

```python
from pydantic import BaseModel, Field

class ResizeParams(BaseModel):
    width: int = Field(gt=0, description="Width must be positive")
    height: int = Field(gt=0, description="Height must be positive")
    source_url: str

@worker.task("resize_image")
async def image_resizer(params: ResizeParams, **kwargs):
    # Data is guaranteed to be valid
    print(f"Resizing {params.source_url} to {params.width}x{params.height}")
    # ...
```

## Key Features

### 1. Task Handlers

Each handler is an asynchronous function that accepts two arguments:

-   `params` (`dict`, `dataclass`, or `pydantic.BaseModel`): The parameters for the task, automatically validated and instantiated based on your type hint.
-   `**kwargs`: Additional metadata about the task, including:
    -   `task_id` (`str`): The unique ID of the task itself.
    -   `job_id` (`str`): The ID of the parent `Job` to which the task belongs.
    -   `priority` (`int`): The execution priority of the task.

### 2. Concurrency Limiting

The worker allows you to control how many tasks are executed in parallel. This can be configured at two levels:

-   **Global Limit**: A maximum number of tasks that the worker can execute simultaneously, regardless of their type. This can be set with the `MAX_CONCURRENT_TASKS` environment variable or by passing `max_concurrent_tasks` to the `Worker` constructor.
-   **Per-Type Limit**: A specific limit for a group of tasks that share a common resource (e.g., a GPU, a specific API), passed via `task_type_limits` to the `Worker` constructor.

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

The SDK supports connecting to multiple orchestrator instances to ensure high availability (`FAILOVER`) and load balancing (`ROUND_ROBIN`). This is configured via the `MULTI_ORCHESTRATOR_MODE` and `ORCHESTRATORS_CONFIG` environment variables.

**If `ORCHESTRATORS_CONFIG` is not set or is invalid JSON, the SDK will fall back to using the `ORCHESTRATOR_URL`. If neither is set, it will default to a single orchestrator at `http://localhost:8080`.** If both `ORCHESTRATORS_CONFIG` and `ORCHESTRATOR_URL` are set, `ORCHESTRATORS_CONFIG` will be used.

The `ORCHESTRATORS_CONFIG` variable must contain a JSON string. Each object in the list represents one orchestrator and can have the following keys:
-   `url` (required): The URL of the orchestrator.
-   `priority` (optional, default: 10): Used in `FAILOVER` mode. A lower number means a higher priority.
-   `weight` (optional, default: 1): Used in `ROUND_ROBIN` mode to determine how frequently the orchestrator is polled.
-   `token` (optional): A specific authentication token for this orchestrator. If not provided, the global `WORKER_TOKEN` is used.

**Example `ORCHESTRATORS_CONFIG`:**
```json
[
    {"url": "http://customer-a.com", "priority": 10, "weight": 100, "token": "token-for-customer-a"},
    {"url": "http://customer-b.com", "priority": 10, "weight": 50, "token": "token-for-customer-b"},
    {"url": "http://internal-backup.com", "priority": 20, "weight": 10}
]
```

-   **`FAILOVER` (default):** The worker connects to orchestrators in the order of their `priority`. It will always try the highest-priority orchestrator first and only switch to the next one if the current one becomes unavailable. In the example above, it would try both `customer-a.com` and `customer-b.com` (which have the same priority) before trying `internal-backup.com`.
-   **`ROUND_ROBIN`:** The worker distributes its requests to fetch tasks across all configured orchestrators based on their `weight`. An orchestrator with a higher weight will be polled for tasks more frequently. In the example, `customer-a.com` would be polled twice as often as `customer-b.com`.





### 5. File System Helper (TaskFiles)

To simplify working with temporary files and paths, the SDK provides a `TaskFiles` helper class. It automatically manages directory creation within the isolated task folder and provides an asynchronous interface for file operations. Just add an argument typed as `TaskFiles` to your handler:

```python
from avtomatika_worker import Worker, TaskFiles

@worker.task("generate_report")
async def generate_report(params: dict, files: TaskFiles, **kwargs):
    # 1. Easy read/write
    await files.write("data.json", '{"status": "ok"}')
    content = await files.read("data.json")
    
    # 2. Get path (directory is created automatically)
    output_path = await files.path_to("report.pdf")
    
    # 3. Check and list files
    if await files.exists("input.jpg"):
        file_list = await files.list()
    
    return {"data": {"report": output_path}}
```

**Available Methods (all asynchronous):**
- `await path_to(name)` — returns the full path to a file (ensures the task directory exists).
- `await read(name, mode='r')` — reads the entire file.
- `await write(name, data, mode='w')` — writes data to a file.
- `await list()` — lists filenames in the task directory.
- `await exists(name)` — checks if a file exists.
- `async with open(name, mode)` — async context manager for advanced usage.

> **Note: Automatic Cleanup**
>
> The SDK automatically deletes the entire task directory (including everything created via `TaskFiles`) immediately after the task completes and the result is sent.

### 6. Handling Large Files (S3 Payload Offloading)

The SDK supports working with large files "out of the box" via S3-compatible storage.

-   **Automatic Download**: If a value in `params` is a URI of the form `s3://...`, the SDK will automatically download the file to the local disk and replace the URI in `params` with the local path. **If the URI ends with `/` (e.g., `s3://bucket/data/`), the SDK treats it as a folder prefix and recursively downloads all matching objects into a local directory.**
-   **Automatic Upload**: If your handler returns a local file path in `data` (located within the `TASK_FILES_DIR` directory), the SDK will automatically upload this file to S3 and replace the path with an `s3://` URI in the final result. **If the path is a directory, the SDK recursively uploads all files within it.**

This functionality is transparent to your code.

#### S3 Example

Suppose the orchestrator sends a task with `{"input_image": "s3://my-bucket/photo.jpg"}`:

```python
import os
from avtomatika_worker import Worker, TaskFiles

worker = Worker(worker_type="image-worker")

@worker.task("process_image")
async def handle_image(params: dict, files: TaskFiles, **kwargs):
    # SDK has already downloaded the file.
    # 'input_image' now contains a local path like '/tmp/payloads/task-id/photo.jpg'
    local_input = params["input_image"]
    local_output = await files.path_to("processed.png")

    # Your logic here (using local files)
    # ... image processing ...

    # Return the local path of the result.
    # The SDK will upload it back to S3 automatically.
    return {
        "status": "success",
        "data": {
            "output_image": local_output
        }
    }
```

This only requires configuring environment variables for S3 access (see Full Configuration Reference).

> **Important: S3 Consistency**
>
> The SDK **does not validate** that the Worker and Orchestrator share the same storage backend. You must ensure that:
> 1. The Worker can reach the `S3_ENDPOINT_URL` used by the Orchestrator.
> 2. The Worker's credentials allow reading from the buckets referenced in the incoming `s3://` URIs.
> 3. The Worker's credentials allow writing to the `S3_DEFAULT_BUCKET`.

### 7. WebSocket Support

## Advanced Features

### Reporting Skill & Model Dependencies

For more advanced scheduling, the worker can report detailed information about its skills and their dependencies on specific models. This allows the orchestrator to make smarter decisions, such as dispatching tasks to workers that already have the required models loaded in memory.

This is configured via the `skill_dependencies` argument in the `Worker` constructor.

-   **`skill_dependencies`**: A dictionary where keys are skill names (as registered with `@worker.task`) and values are.
The user wants to improve the `README.md` file. I've already read it and have a plan. I need to get the file content and then I can use the `replace` tool to update it.
I've already read the file content in the previous step. Now I will use the `replace` tool to update the file.
I have read the `README.md` file. Now I will reorder its sections to improve clarity for new users. The new order will be: Installation, Configuration, Quick Start, Key Features, Advanced Features, Full Configuration Reference, and Development.
I have read the `README.md` file. Now I will update it to document the new flexible parameter typing feature. I will add a new section called "Defining Task Parameters" and update the "Installation" section. lists of model names required by that skill.

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

## Full Configuration Reference

The worker is fully configured via environment variables.

| Variable                      | Description                                                                                             | Default                                |
| ----------------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `WORKER_ID`                   | A unique identifier for the worker.                                                                     | A random UUID, e.g., `worker-...`      |
| `WORKER_TYPE`                 | A string identifying the type of the worker.                                                            | `generic-cpu-worker`                   |
| `WORKER_PORT`                 | The port for the worker's health check server.                                                          | `8083`                                 |
| `WORKER_TOKEN`                | A common authentication token used to connect to orchestrators.                                         | `your-secret-worker-token`             |
| `WORKER_INDIVIDUAL_TOKEN`     | An individual token for this worker, which overrides `WORKER_TOKEN` if set.                               | -                                      |
| `ORCHESTRATOR_URL`            | The URL of a single orchestrator (used if `ORCHESTRATORS_CONFIG` is not set).                             | `http://localhost:8080`                |
| `ORCHESTRATORS_CONFIG`        | A JSON string with a list of orchestrators for multi-orchestrator modes.                                | `[]`                                   |
| `MULTI_ORCHESTRATOR_MODE`     | The mode for handling multiple orchestrators. Possible values: `FAILOVER`, `ROUND_ROBIN`.                  | `FAILOVER`                             |
| `MAX_CONCURRENT_TASKS`        | The maximum number of tasks the worker can execute simultaneously.                                      | `10`                                   |
| `COST_PER_SKILL`               | A JSON string mapping skill names to their cost per second.                                             | `{}`                                   |
| `CPU_CORES`                   | The number of CPU cores available to the worker.                                                        | `4`                                    |
| `GPU_MODEL`                   | The model of the GPU available to the worker (e.g., "RTX 4090").                                         | -                                      |
| `GPU_VRAM_GB`                 | The amount of VRAM in GB for the GPU.                                                                   | `0`                                    |
| `INSTALLED_SOFTWARE`          | A JSON string representing a dictionary of installed software and their versions.                         | `{"python": "3.9"}`                    |
| `INSTALLED_MODELS`            | A JSON string representing a list of dictionaries with information about installed models.              | `[]`                                   |
| `HEARTBEAT_INTERVAL`          | The interval in seconds between heartbeats to the orchestrator.                                         | `15`                                   |
| `WORKER_HEARTBEAT_DEBOUNCE_DELAY` | The delay in seconds for debouncing immediate heartbeats after a state change.                          | `0.1`                                  |
| `WORKER_ENABLE_WEBSOCKETS`    | Enable (`true`) or disable (`false`) WebSocket support for real-time commands.                            | `false`                                |
| `RESULT_MAX_RETRIES`          | The maximum number of times to retry sending a task result if it fails.                                   | `5`                                    |
| `RESULT_RETRY_INITIAL_DELAY`  | The initial delay in seconds before the first retry of sending a result.                                  | `1.0`                                  |
| `TASK_POLL_TIMEOUT`           | The timeout in seconds for polling for new tasks.                                                       | `30`                                   |
| `TASK_POLL_ERROR_DELAY`       | The delay in seconds before retrying after a polling error.                                             | `5.0`                                  |
| `IDLE_POLL_DELAY`             | The delay in seconds between polls when the worker is idle.                                             | `0.01`                                 |
| `TASK_FILES_DIR`          | The directory for temporarily storing files when working with S3.                                       | `/tmp/payloads`                        |
| `S3_ENDPOINT_URL`             | The URL of the S3-compatible storage.                                                                   | -                                      |
| `S3_ACCESS_KEY`               | The access key for S3.                                                                                  | -                                      |
| `S3_SECRET_KEY`               | The secret key for S3.                                                                                  | -                                      |
| `S3_DEFAULT_BUCKET`           | The default bucket name for uploading results.                                                          | `avtomatika-payloads`                  |

## Development

To install the necessary dependencies for running tests, use the following command:

```bash
pip install .[test]
```
