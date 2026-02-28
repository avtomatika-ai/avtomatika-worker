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
