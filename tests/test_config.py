import os
from unittest.mock import patch

from avtomatika_worker.config import WorkerConfig


def test_worker_config_defaults():
    """Tests that the WorkerConfig class loads default values correctly."""
    with patch.dict(os.environ, {}, clear=True):
        config = WorkerConfig()
        assert config.WORKER_ID.startswith("worker-")
        assert config.WORKER_TYPE == "generic-cpu-worker"
        assert config.WORKER_PORT == 8083
        assert config.ORCHESTRATORS == [{"url": "http://localhost:8080", "priority": 1, "weight": 1}]
        assert config.WORKER_TOKEN == "your-secret-worker-token"
        assert config.COST_PER_SKILL == {}
        assert config.MAX_CONCURRENT_TASKS == 10
        assert config.RESOURCES["cpu_cores"] == 4
        assert config.RESOURCES["gpu_info"] is None
        assert config.INSTALLED_SOFTWARE == {"python": "3.9"}
        assert config.INSTALLED_MODELS == []
        assert config.TASK_FILES_DIR == "/tmp/payloads"
        assert config.HEARTBEAT_INTERVAL == 15
        assert config.RESULT_MAX_RETRIES == 5
        assert config.RESULT_RETRY_INITIAL_DELAY == 1.0
        assert config.HEARTBEAT_DEBOUNCE_DELAY == 0.1
        assert config.TASK_POLL_TIMEOUT == 30
        assert config.TASK_POLL_ERROR_DELAY == 5.0
        assert config.IDLE_POLL_DELAY == 0.01
        assert not config.ENABLE_WEBSOCKETS
        assert config.MULTI_ORCHESTRATOR_MODE == "FAILOVER"


def test_worker_config_custom_values():
    """Tests that the WorkerConfig class loads custom values from environment variables correctly."""
    with patch.dict(
        os.environ,
        {
            "WORKER_ID": "test-worker",
            "WORKER_TYPE": "test-worker-type",
            "WORKER_PORT": "9090",
            "ORCHESTRATORS_CONFIG": '[{"url": "http://test-orchestrator:8080", "priority": 1, "weight": 5}]',
            "WORKER_INDIVIDUAL_TOKEN": "test-token",
            "COST_PER_SKILL": '{"skill1": 0.5}',
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
            "TASK_FILES_DIR": "/custom/path",
        },
        clear=True,
    ):
        config = WorkerConfig()
        assert config.WORKER_ID == "test-worker"
        assert config.WORKER_TYPE == "test-worker-type"
        assert config.WORKER_PORT == 9090
        assert config.ORCHESTRATORS == [{"url": "http://test-orchestrator:8080", "priority": 1, "weight": 5}]
        assert config.WORKER_TOKEN == "test-token"
        assert config.COST_PER_SKILL == {"skill1": 0.5}
        assert config.MAX_CONCURRENT_TASKS == 20
        assert config.RESOURCES["cpu_cores"] == 8
        assert config.RESOURCES["gpu_info"] == {"model": "RTX 4090", "vram_gb": 24}
        assert config.INSTALLED_SOFTWARE == {"python": "3.10"}
        assert config.INSTALLED_MODELS == [{"name": "test-model"}]
        assert config.TASK_FILES_DIR == "/custom/path"
        assert config.HEARTBEAT_INTERVAL == 30
        assert config.RESULT_MAX_RETRIES == 10
        assert config.RESULT_RETRY_INITIAL_DELAY == 2.0
        assert config.HEARTBEAT_DEBOUNCE_DELAY == 0.2
        assert config.TASK_POLL_TIMEOUT == 60
        assert config.TASK_POLL_ERROR_DELAY == 10.0
        assert config.IDLE_POLL_DELAY == 0.02
        assert config.ENABLE_WEBSOCKETS
        assert config.MULTI_ORCHESTRATOR_MODE == "ROUND_ROBIN"


def test_get_orchestrators_config_invalid_json(capsys):
    """Tests that _get_orchestrators_config handles invalid JSON correctly and prints a warning."""
    with patch.dict(os.environ, {"ORCHESTRATORS_CONFIG": "invalid-json"}, clear=True):
        config = WorkerConfig()
        assert config.ORCHESTRATORS == [{"url": "http://localhost:8080", "priority": 1, "weight": 1}]
        captured = capsys.readouterr()
        assert "Warning: Could not decode JSON from ORCHESTRATORS_CONFIG" in captured.out


def test_load_json_from_env_invalid_json(capsys):
    """Tests that _load_json_from_env handles invalid JSON correctly and prints a warning."""
    with patch.dict(os.environ, {"INSTALLED_SOFTWARE": "invalid-json"}, clear=True):
        config = WorkerConfig()
        assert config.INSTALLED_SOFTWARE == {"python": "3.9"}
        captured = capsys.readouterr()
        assert "Warning: Could not decode JSON from environment variable INSTALLED_SOFTWARE" in captured.out


def test_orchestrator_config_precedence_message(capsys):
    """
    Tests that an info message is printed when both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set.
    """
    env_vars = {
        "ORCHESTRATORS_CONFIG": '[{"url": "http://config.com"}]',
        "ORCHESTRATOR_URL": "http://url.com",
    }
    with patch.dict(os.environ, env_vars, clear=True):
        WorkerConfig()
        captured = capsys.readouterr()
        expected_message = "Info: Both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set. Using ORCHESTRATORS_CONFIG.\n"
        assert captured.out == expected_message
