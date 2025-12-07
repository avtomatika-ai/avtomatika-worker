from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


def test_task_registration_with_type():
    """Tests that the @worker.task decorator correctly registers a task with a type."""
    worker = Worker(task_type_limits={"video": 1})

    @worker.task("process_video", task_type="video")
    async def my_handler(params: dict):
        return {"status": "success"}

    assert "process_video" in worker._task_handlers
    assert worker._task_handlers["process_video"]["type"] == "video"
    assert worker._task_handlers["process_video"]["func"] == my_handler


def test_get_current_state_initial():
    """Tests the initial state of the worker."""
    mock_config = WorkerConfig()
    mock_config.MAX_CONCURRENT_TASKS = 5  # Set initial limit
    mock_config.ORCHESTRATORS = [{"url": "http://test", "weight": 1}]  # Needs to be defined

    worker = Worker(config=mock_config, task_type_limits={"video": 1, "audio": 2})

    @worker.task("process_video", task_type="video")
    async def video_handler(params: dict): ...

    @worker.task("process_audio", task_type="audio")
    async def audio_handler(params: dict): ...

    @worker.task("unlimited_task")
    async def unlimited_handler(params: dict): ...

    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert set(state["supported_tasks"]) == {
        "process_video",
        "process_audio",
        "unlimited_task",
    }


def test_get_current_state_global_limit_reached():
    """Tests that the worker becomes busy when the global concurrency limit is reached."""
    # Create a mock config to ensure MAX_CONCURRENT_TASKS is controlled
    mock_config = WorkerConfig()
    mock_config.MAX_CONCURRENT_TASKS = 1

    worker = Worker(config=mock_config)  # Pass the mock config
    worker._current_load = 1

    @worker.task("some_task")
    async def some_task(params: dict): ...

    state = worker._get_current_state()
    assert state["status"] == "busy"
    assert state["supported_tasks"] == []


def test_get_current_state_type_limit_reached():
    """Tests that a task type is unavailable when its limit is reached."""
    worker = Worker(max_concurrent_tasks=5, task_type_limits={"video": 1})
    worker._current_load = 1
    worker._current_load_by_type["video"] = 1

    @worker.task("process_video", task_type="video")
    async def video_handler(params: dict): ...

    @worker.task("process_audio", task_type="audio")
    async def audio_handler(params: dict): ...

    state = worker._get_current_state()
    assert state["status"] == "idle"
    assert set(state["supported_tasks"]) == {"process_audio"}


def test_get_current_state_all_type_limits_reached():
    """Tests that the worker becomes busy when all type limits are reached,
    even if the global limit is not."""
    worker = Worker(max_concurrent_tasks=5, task_type_limits={"video": 1, "audio": 1})
    worker._current_load = 2
    worker._current_load_by_type["video"] = 1
    worker._current_load_by_type["audio"] = 1

    @worker.task("process_video", task_type="video")
    async def video_handler(params: dict): ...

    @worker.task("process_audio", task_type="audio")
    async def audio_handler(params: dict): ...

    state = worker._get_current_state()
    assert state["status"] == "busy"
    assert state["supported_tasks"] == []
