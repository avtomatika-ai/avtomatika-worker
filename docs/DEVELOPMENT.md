# Worker Development Guide

This document describes how to create a custom Worker compatible with the Orchestrator using `avtomatika-worker`.

## Core Concept

Workers created with the SDK implement a hybrid interaction model with the Orchestrator:
- **PULL Model for Task Fetching:** The worker initiates the connection to the Orchestrator and "pulls" tasks from its personal queue. This allows Workers to operate from any network, including behind NAT or corporate firewalls, without needing a public IP address.
- **WebSocket for Real-time Communication:** An optional bidirectional channel for receiving commands (e.g., task cancellation) and sending intermediate execution progress.

## How to Create a Worker with the SDK

### Step 1: Install `avtomatika-worker`

Ensure the SDK is installed in your environment. If you are working in the main repository, you can install it in editable mode:
```bash
pip install -e .
```

### Step 2: Create a Worker File

Create a Python file (e.g., `my_worker.py`) and import the `Worker` class.

```python
import asyncio
from avtomatika_worker import Worker

# 1. Initialize the Worker class
# You can specify a unique type for your worker.
worker = Worker(worker_type="my-custom-worker")

# 2. Define task handlers using the @worker.task decorator
@worker.task("generate_report")
async def generate_report_handler(params: dict, **kwargs) -> dict:
    """
    This function will be called when the Orchestrator sends
    a task of type "generate_report".

    - `params` (dict): Positional argument containing task execution parameters.
    - `**kwargs`: Keyword arguments with task metadata:
        - `task_id` (str): Unique ID of the task.
        - `job_id` (str): ID of the parent Job.
        - `priority` (float): Task priority.
    """
    task_id = kwargs.get("task_id")
    job_id = kwargs.get("job_id")
    priority = kwargs.get("priority", 0.0)

    print(f"Received parameters: {params}")

    # Simulate long work with progress reporting
    print("Starting report generation...")
    await asyncio.sleep(2)
    # Use worker.send_progress to send an update to the Orchestrator
    await worker.send_progress(task_id, job_id, progress=0.5, message="Analyzed 50% of data")
    await asyncio.sleep(2)
    print("Report generation completed.")


    # 3. Return the result
    #    - 'status' (required): "success", "failure", or a custom status.
    #    - 'data' (optional): Dictionary with data to be added to the Job context.
    #    - 'error' (optional when status="failure"): Dictionary with error details.
    #      - 'code': "TRANSIENT_ERROR", "PERMANENT_ERROR", or "INVALID_INPUT_ERROR".
    #      - 'message': Human-readable error description.
    return {
        "status": "success",
        "data": {"report_url": "/path/to/report.pdf"}
    }

    # Example of returning an error
    # return {
    #     "status": "failure",
    #     "error": {
    #         "code": "TRANSIENT_ERROR",
    #         "message": "Could not connect to external service."
    #     }
    # }

@worker.task("send_email")
async def send_email_handler(params: dict, **kwargs) -> dict:
    print(f"Sending email with parameters: {params}")
    await asyncio.sleep(1)
    return {"status": "success"}

# 4. Run the worker
if __name__ == "__main__":
    worker.run()
```

### Step 3: Connection and Authentication Configuration

#### Option 1: Simple Connection (Single Orchestrator)

This is the simplest method, suitable for most cases.

```dotenv
# Your Orchestrator's address
ORCHESTRATOR_URL=http://localhost:8080

# Recommended authentication method
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker

# (Optional) Deprecated shared token authentication method
# WORKER_TOKEN=your-secret-worker-token
```

#### Option 2: Advanced Connection (Multiple Orchestrators)

This method is used for High Availability (failover) or Load Balancing (round robin).

-   `ORCHESTRATORS_CONFIG`: Instead of `ORCHESTRATOR_URL`, this variable is used. It contains a JSON string with a list of all Orchestrators.
-   `MULTI_ORCHESTRATOR_MODE`: Defines how the Worker will interact with this list.

**Example for High Availability (Failover):**
In this mode, the Worker will work with `main-orchestrator`. If it becomes unavailable, the Worker automatically switches to `backup-orchestrator`.

```dotenv
# The worker will poll 'main-orchestrator'. If it goes down,
# the SDK automatically switches to 'backup-orchestrator'.
ORCHESTRATORS_CONFIG='[
    {"url": "http://main-orchestrator:8080"},
    {"url": "http://backup-orchestrator:8080"}
]'

# FAILOVER mode is used by default, but can be specified explicitly.
MULTI_ORCHESTRATOR_MODE=FAILOVER

# Authentication settings remain the same
WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker
```

**Example for Load Balancing (Round Robin):**
In this mode, the Worker will alternately send task fetch requests to `orchestrator-1` and `orchestrator-2`, distributing the load.

```dotenv
# The worker will alternately poll both Orchestrators.
ORCHESTRATORS_CONFIG='[
    {"url": "http://orchestrator-1:8080"},
    {"url": "http://orchestrator-2:8080"}
]'

MULTI_ORCHESTRATOR_MODE=ROUND_ROBIN

WORKER_ID=report-worker-01
WORKER_INDIVIDUAL_TOKEN=a-super-secret-token-for-this-worker
```
*Note: When using `ORCHESTRATORS_CONFIG`, the `ORCHESTRATOR_URL` variable is ignored.*

### Step 4: Real-time Communication (WebSocket)

To enable this functionality, set the environment variable `WORKER_ENABLE_WEBSOCKETS=true`. Two new capabilities will then be available:

#### Sending Progress

Inside your task handler, you can call the `worker.send_progress()` method to inform the Orchestrator about the progress of a long-running operation.

```python
await worker.send_progress(
    task_id="...",      # Current task ID
    job_id="...",       # Parent Job ID
    progress=0.75,      # Float between 0.0 and 1.0
    message="Processed 75% of video"  # Optional message
)
```
> **Important:** `task_id` and `job_id` are now always passed to your handler as keyword arguments, along with `params`. See the example in Step 2.

#### Task Cancellation

The SDK provides two task cancellation mechanisms:

1.  **WebSocket (Push Model):** If WebSocket is enabled, the Orchestrator can send an immediate cancellation command. This raises an `asyncio.CancelledError` in your handler. This method provides the fastest reaction.

2.  **Redis (Pull Model):** Even without WebSocket, you can implement "cooperative" cancellation for very long tasks. The SDK provides an async function `worker.check_for_cancellation(task_id)`. You should periodically call it inside your processing loop. If the function returns `True`, it means the Orchestrator requested cancellation. Your code should gracefully interrupt execution, perform cleanup, and return a `cancelled` status.

**Example using `check_for_cancellation`:**
```python
@worker.task("train_model")
async def train_model_handler(params: dict, task_id: str, job_id: str) -> dict:
    for epoch in range(params.get("epochs", 100)):
        # ... model training logic here ...

        # Check cancellation flag at the end of each epoch
        if await worker.check_for_cancellation(task_id):
            print("Cancellation detected. Stopping training...")
            # ... cleanup code (e.g., removing temporary files) ...
            return {"status": "cancelled", "message": "Training was cancelled by user."}

    return {"status": "success"}
```

This hybrid model ensures both fast cancellation via WebSocket and a reliable fallback mechanism via Redis that doesn't require a persistent connection.

### Step 5: Running

Simply run your Python file:
```bash
python my_worker.py
```

The worker will automatically connect to the Orchestrator, register itself, establish a WebSocket connection (if enabled), and start polling for new tasks.

---

### Step 6 (Optional): Working with Large Files via "Payload Offloading"

If your tasks require processing large volumes of data (video, HD images, large text files), passing them directly through the Orchestrator is inefficient. The SDK supports a **"Payload Offloading"** mechanism, which allows transferring "heavy" data via S3-compatible storage. It uses the high-performance **`obstore`** library (Rust-based) for these operations.

#### How It Works:

1.  **Client** uploads input files to S3 before creating a Job and passes only URIs like `s3://my-bucket/path/to/file.mp4` in the task parameters.
2.  **Worker SDK** automatically detects such URIs in task parameters.
3.  Before calling your handler, the SDK **downloads the file** from S3 to a temporary directory and replaces the `s3://` URI with the local file path.
4.  Your handler code works with the file as a regular local file.
5.  If your handler **returns a local file path** in the result, the SDK automatically **uploads this file to S3** and replaces the local path with an `s3://` URI.
6.  The SDK also **automatically cleans up** all downloaded temporary files after the task completes.

#### S3 Usage Example

If the Orchestrator sends a task with parameter `{"video_path": "s3://bucket/input.mp4"}`, your code would look like this:

```python
import os
from avtomatika_worker import Worker

worker = Worker(worker_type="video-processor")

@worker.task("resize_video")
async def resize_video(params: dict, **kwargs):
    # SDK has already downloaded the file. In params['video_path'] is now a local path
    input_file = params["video_path"]
    output_file = os.path.join(os.path.dirname(input_file), "resized.mp4")

    # You work with files as regular local data
    print(f"Processing file {input_file}...")
    # ... processing logic (e.g., calling ffmpeg) ...

    # Return the path to the created file.
    # Important: the file must be inside TASK_FILES_DIR (default /tmp/payloads)
    return {
        "status": "success",
        "data": {
            "result_url": output_file
        }
    }
```

#### Working with File System (TaskFiles)

For convenient path and temporary file management, the SDK provides the `TaskFiles` class. It allows you to ignore manual directory creation and provides an async interface for file operations. Just add an argument with type `TaskFiles` to your function:

```python
from avtomatika_worker import Worker, TaskFiles

@worker.task("generate_file")
async def generate_file(params: dict, files: TaskFiles, **kwargs):
    # 1. Fast read/write
    await files.write("report.txt", "Some data")
    content = await files.read("report.txt")
    
    # 2. Get path (directory created automatically)
    output_path = await files.path_to("result.mp4")
    
    # 3. Check and list files
    if await files.exists("input.jpg"):
        all_files = await files.list()
        
    return {"data": {"file": output_path}}
```

**Available Methods (all async):**
- `await path_to(name)` — returns full path to file (creates task folder).
- `await read(name, mode='r')` — reads entire file.
- `await write(name, data, mode='w')` — writes data to file.
- `await list()` — lists filenames in task folder.
- `await exists(name)` — checks existence.
- `async with open(name, mode)` — context manager for advanced usage.

#### Working with Folders

The SDK also supports recursive directory transfer:

1.  **Download:** If an S3 link ends with `/` (e.g., `s3://bucket/dataset/`), the SDK downloads all contents of this prefix to a local folder. The task parameters will contain the path to this folder.
2.  **Upload:** If you return a path to a local directory, the SDK recursively uploads all its contents to S3, preserving file structure. The result link will look like `s3://bucket/directory_name/`.

#### S3 Configuration

To enable this functionality, you need to configure the following environment variables:

-   `S3_ENDPOINT_URL`: URL of your S3-compatible storage (e.g., `https://s3.amazonaws.com` or `http://localhost:9000` for MinIO).
-   `S3_ACCESS_KEY`: Access key for S3.
-   `S3_SECRET_KEY`: Secret key for S3.
-   `S3_DEFAULT_BUCKET`: Bucket name where results will be uploaded.
-   `S3_REGION`: The region for S3 storage (required by some providers, e.g., `us-east-1`).
-   `TASK_FILES_DIR`: **(Important for security)** Local directory where isolated workspaces for tasks are created. The SDK uploads to S3 only those files that are inside this directory. Default: `/tmp/payloads`.

With these settings, the "Payload Offloading" mechanism will work completely automatically, requiring no changes to your handler code.

> **Important: Automatic Cleanup**
>
> The SDK automatically deletes the entire task directory (including all files downloaded and created via `TaskFiles`) immediately after processing completes and the result is sent. You don't need to worry about deleting temporary files.

> **Important: S3 Consistency**
>
> The SDK **does not automatically verify** that the Worker and Orchestrator use the same storage. You must manually ensure that:
> 1. The Worker has access to the same `S3_ENDPOINT_URL` as the Orchestrator (or has network access to it).
> 2. The Worker's credentials (`S3_ACCESS_KEY`/`S3_SECRET_KEY`) have read permissions for buckets linked by the Orchestrator.
> 3. The Worker's credentials have write permissions to `S3_DEFAULT_BUCKET` for uploading results.
