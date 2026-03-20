# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from unittest.mock import MagicMock, patch

import pytest


# Ensure we can import the module even if OTel is not installed
def test_observability_manager_no_op():
    """Tests that ObservabilityManager works even if OTel is not installed."""
    # We mock the module level _HAS_OTEL
    with patch("avtomatika_worker.observability._HAS_OTEL", False):
        from avtomatika_worker.observability import ObservabilityManager

        manager = ObservabilityManager(enabled=True)
        assert manager.enabled is False

        # Calling methods should not raise errors
        with manager.start_task_span("test", "t1", "j1") as span:
            assert span is None

        manager.record_task_finished("test", "success", 1.0)
        manager.record_s3_op("upload", "success")
        assert manager.generate_latest() == b""


@pytest.mark.asyncio
async def test_observability_integration_logic():
    """Tests basic functionality of ObservabilityManager logic."""
    # Instead of patching opentelemetry, we test the manager's internal state
    # by ensuring it handles attributes correctly.
    with patch("avtomatika_worker.observability._HAS_OTEL", True):
        from avtomatika_worker.observability import ObservabilityManager

        # Mock the attributes that would be created during __init__
        manager = ObservabilityManager(enabled=False)  # Start disabled to avoid init logic

        # Manually inject mocks
        manager.enabled = True
        manager.tracer = MagicMock()
        manager.tasks_total = MagicMock()
        manager.tasks_duration = MagicMock()
        manager.s3_ops_total = MagicMock()

        # Test span starting
        with manager.start_task_span("test-task", "t1", "j1"):
            pass

        manager.tracer.start_as_current_span.assert_called_once()

        # Test metrics recording
        manager.record_task_finished("test-task", "success", 0.5)
        manager.tasks_total.add.assert_called()
        manager.tasks_duration.record.assert_called()
