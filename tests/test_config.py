# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

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
        assert config.RESOURCES["devices"] is None
        assert config.INSTALLED_SOFTWARE == {"python": "3.11"}
        assert config.INSTALLED_ARTIFACTS == []
        assert config.TASK_FILES_DIR == "/tmp/payloads"
        assert config.HEARTBEAT_INTERVAL == 15
        assert config.RESULT_MAX_RETRIES == 5
        assert config.RESULT_RETRY_INITIAL_DELAY == 1.0
        assert config.HEARTBEAT_DEBOUNCE_DELAY == 0.1
        assert config.TASK_POLL_TIMEOUT == 30
        assert config.TASK_POLL_ERROR_DELAY == 5.0
        assert config.IDLE_POLL_DELAY == 0.01
        assert not config.ENABLE_WEBSOCKETS
        assert config.MULTI_ORCHESTRATOR_MODE == "WATERFALL"


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
            "INSTALLED_ARTIFACTS": '[{"name": "test-model"}]',
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
        assert config.RESOURCES["ram_gb"] == 0.0
        assert config.RESOURCES["devices"] == [
            {"type": "gpu", "model": "RTX 4090", "id": "0", "properties": {"memory_gb": 24}}
        ]
        assert config.INSTALLED_SOFTWARE == {"python": "3.10"}
        assert config.INSTALLED_ARTIFACTS == [{"name": "test-model"}]
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


def test_worker_config_extra_and_devices_and_ram(monkeypatch):
    """Tests loading RAM_GB, EXTRA capabilities, and generic WORKER_DEVICES."""
    monkeypatch.setenv("RAM_GB", "16.5")
    monkeypatch.setenv("WORKER_EXTRA_REGION", "us-east-1")
    monkeypatch.setenv("WORKER_EXTRA_TIER", '["standard", "pro"]')
    monkeypatch.setenv("WORKER_DEVICES", '[{"type": "npu", "model": "Coral", "id": "1"}]')
    monkeypatch.setenv("ORCHESTRATOR_URL", "http://localhost:8080")

    config = WorkerConfig()
    assert config.RESOURCES["ram_gb"] == 16.5
    assert config.EXTRA_CAPABILITIES["region"] == "us-east-1"
    assert config.EXTRA_CAPABILITIES["tier"] == ["standard", "pro"]
    assert config.RESOURCES["devices"][-1]["type"] == "npu"
    assert config.RESOURCES["devices"][-1]["model"] == "Coral"


def test_get_orchestrators_config_invalid_json(caplog):
    """Tests that _get_orchestrators_config handles invalid JSON correctly and logs a warning."""
    with patch.dict(os.environ, {"ORCHESTRATORS_CONFIG": "invalid-json"}, clear=True), caplog.at_level("WARNING"):
        config = WorkerConfig()
        assert config.ORCHESTRATORS == [{"url": "http://localhost:8080", "priority": 1, "weight": 1}]
        assert "Could not decode JSON from ORCHESTRATORS_CONFIG" in caplog.text


def test_load_json_from_env_invalid_json(caplog):
    """Tests that _load_json_from_env handles invalid JSON correctly and logs a warning."""
    with patch.dict(os.environ, {"INSTALLED_SOFTWARE": "invalid-json"}, clear=True), caplog.at_level("WARNING"):
        config = WorkerConfig()
        assert config.INSTALLED_SOFTWARE == {"python": "3.11"}
        assert "Could not decode JSON from environment variable INSTALLED_SOFTWARE" in caplog.text


def test_orchestrator_config_precedence_message(caplog):
    """
    Tests that an info message is logged when both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set.
    """
    env_vars = {
        "ORCHESTRATORS_CONFIG": '[{"url": "http://config.com"}]',
        "ORCHESTRATOR_URL": "http://url.com",
    }
    with patch.dict(os.environ, env_vars, clear=True), caplog.at_level("INFO"):
        WorkerConfig()
        assert "Both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set. Using ORCHESTRATORS_CONFIG." in caplog.text
