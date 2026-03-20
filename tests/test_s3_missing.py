# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import sys
from unittest.mock import patch

import pytest

from avtomatika_worker.config import WorkerConfig


@pytest.mark.asyncio
async def test_s3_missing_dependency():
    """
    Tests that S3Manager raises appropriate errors when 'obstore' is not installed.
    """
    # Remove the module from sys.modules to force reload
    if "avtomatika_worker.s3" in sys.modules:
        del sys.modules["avtomatika_worker.s3"]

    # Simulate import error for obstore
    with patch.dict(sys.modules, {"obstore": None}):
        # Re-import the module under test
        import avtomatika_worker.s3 as s3_module

        assert s3_module._HAS_S3 is False

        config = WorkerConfig()
        # Ensure S3 is "enabled" in config so logic tries to proceed
        config.S3_ENDPOINT_URL = "http://localhost:9000"

        manager = s3_module.S3Manager(config)

        # 1. Check explicit check
        with pytest.raises(RuntimeError) as excinfo:
            manager._check_availability()
        assert "install 'avtomatika-worker[s3]'" in str(excinfo.value)

        # 2. Check process_params (async)
        with pytest.raises(RuntimeError):
            await manager.process_params({"file": "s3://bucket/file"}, "task-1")

    # Cleanup: remove the modified module so subsequent tests import the correct one
    if "avtomatika_worker.s3" in sys.modules:
        del sys.modules["avtomatika_worker.s3"]


@pytest.mark.asyncio
async def test_metrics_missing_dependency():
    """
    Tests that ObservabilityManager enters No-op mode when 'opentelemetry' is not installed.
    """
    if "avtomatika_worker.observability" in sys.modules:
        del sys.modules["avtomatika_worker.observability"]

    # Simulate missing OTel
    with patch.dict(
        sys.modules,
        {
            "opentelemetry": None,
            "opentelemetry.metrics": None,
            "opentelemetry.trace": None,
            "opentelemetry.sdk": None,
            "opentelemetry.exporter": None,
            "opentelemetry.exporter.prometheus": None,
            "prometheus_client": None,
        },
    ):
        import avtomatika_worker.observability as obs_module

        assert obs_module._HAS_OTEL is False

        manager = obs_module.ObservabilityManager(enabled=True)
        assert manager.enabled is False

        # Verify no crashes on standard calls
        with manager.start_task_span("task", "t1", "j1") as span:
            assert span is None

        manager.record_task_finished("task", "success", 1.0)
        assert manager.generate_latest() == b""

    if "avtomatika_worker.observability" in sys.modules:
        del sys.modules["avtomatika_worker.observability"]
