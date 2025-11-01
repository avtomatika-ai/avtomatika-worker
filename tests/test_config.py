import os
from unittest.mock import patch

from avtomatika_worker.config import WorkerConfig


def test_worker_config_defaults():
    """Tests that the WorkerConfig class loads default values correctly."""
    with patch.dict(os.environ, {}, clear=True):
        config = WorkerConfig()
        assert config.worker_id.startswith("worker-")
        assert config.worker_type == "generic-cpu-worker"
        assert config.worker_port == 8083
        assert config.orchestrators == [{"url": "http://localhost:8080", "priority": 1}]
        assert config.worker_token == "your-secret-worker-token"
        assert config.cost_per_second == 0.01
        assert config.max_concurrent_tasks == 10
        assert config.resources["cpu_cores"] == 4
        assert config.resources["gpu_info"] is None
        assert config.installed_software == {"python": "3.9"}
        assert config.installed_models == []
        assert config.heartbeat_interval == 15
        assert config.result_max_retries == 5
        assert config.result_retry_initial_delay == 1.0
        assert config.heartbeat_debounce_delay == 0.1
        assert config.task_poll_timeout == 30
        assert config.task_poll_error_delay == 5.0
        assert config.idle_poll_delay == 0.01
        assert not config.enable_websockets
        assert config.multi_orchestrator_mode == "FAILOVER"


def test_worker_config_custom_values():
    """Tests that the WorkerConfig class loads custom values from environment variables correctly."""
    with patch.dict(
        os.environ,
        {
            "WORKER_ID": "test-worker",
            "WORKER_TYPE": "test-worker-type",
            "WORKER_PORT": "9090",
            "ORCHESTRATORS_CONFIG": '[{"url": "http://test-orchestrator:8080", "priority": 1}]',
            "WORKER_INDIVIDUAL_TOKEN": "test-token",
            "WORKER_COST_PER_SECOND": "0.02",
            "MAX_CONCURRENT_TASKS": "20",
            "CPU_CORES": "8",
            "GPU_MODEL": "RTX 4090",
            "GPU_VRAM_GB": "24",
            "INSTALLED_SOFTWARE": '{"python": "3.10"}',
            "INSTALLED_MODELS": '[{"name": "test-model"}]',
            "HEARTBEAT_INTERVAL": "30",
            "RESULT_MAX_RETRIES": "10",
            "RESULT_RETRY_INITIAL_DELAY": "2.0",
            "WORKER_HEARTBEAT_DEBOUNCE_DELAY": "0.2",
            "TASK_POLL_TIMEOUT": "60",
            "TASK_POLL_ERROR_DELAY": "10.0",
            "IDLE_POLL_DELAY": "0.02",
            "WORKER_ENABLE_WEBSOCKETS": "true",
            "MULTI_ORCHESTRATOR_MODE": "ROUND_ROBIN",
        },
        clear=True,
    ):
        config = WorkerConfig()
        assert config.worker_id == "test-worker"
        assert config.worker_type == "test-worker-type"
        assert config.worker_port == 9090
        assert config.orchestrators == [{"url": "http://test-orchestrator:8080", "priority": 1}]
        assert config.worker_token == "test-token"
        assert config.cost_per_second == 0.02
        assert config.max_concurrent_tasks == 20
        assert config.resources["cpu_cores"] == 8
        assert config.resources["gpu_info"] == {"model": "RTX 4090", "vram_gb": 24}
        assert config.installed_software == {"python": "3.10"}
        assert config.installed_models == [{"name": "test-model"}]
        assert config.heartbeat_interval == 30
        assert config.result_max_retries == 10
        assert config.result_retry_initial_delay == 2.0
        assert config.heartbeat_debounce_delay == 0.2
        assert config.task_poll_timeout == 60
        assert config.task_poll_error_delay == 10.0
        assert config.idle_poll_delay == 0.02
        assert config.enable_websockets
        assert config.multi_orchestrator_mode == "ROUND_ROBIN"


def test_get_orchestrators_config_invalid_json():
    """Tests that _get_orchestrators_config handles invalid JSON correctly."""
    with patch.dict(os.environ, {"ORCHESTRATORS_CONFIG": "invalid-json"}, clear=True):
        config = WorkerConfig()
        assert config.orchestrators == [{"url": "http://localhost:8080", "priority": 1}]


def test_load_json_from_env_invalid_json():
    """Tests that _load_json_from_env handles invalid JSON correctly."""
    with patch.dict(os.environ, {"INSTALLED_SOFTWARE": "invalid-json"}, clear=True):
        config = WorkerConfig()
        assert config.installed_software == {"python": "3.9"}
