# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from rxon.models import FileMetadata

from avtomatika_worker.task_files import TaskFiles


@pytest.mark.asyncio
async def test_task_files_json_operations():
    with tempfile.TemporaryDirectory() as tmpdir:
        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1")
        data = {"hello": "world", "nested": [1, 2, 3]}

        # Test write_json
        await tf.write_json("test.json", data)
        assert os.path.exists(os.path.join(tmpdir, "test.json"))

        # Test read_json
        read_data = await tf.read_json("test.json")
        assert read_data == data


@pytest.mark.asyncio
async def test_task_files_s3_proxy_methods():
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_s3 = MagicMock()
        mock_meta = FileMetadata(uri="s3://test/file", size=100, etag="hash")
        mock_s3._upload_to_s3 = AsyncMock(return_value=mock_meta)
        mock_s3._process_s3_uri = AsyncMock(return_value="/local/path")

        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1", s3_manager=mock_s3)

        # Test upload_file
        await tf.write("hello.txt", "content")
        meta = await tf.upload_file("hello.txt")
        assert meta == mock_meta
        mock_s3._upload_to_s3.assert_called_once()

        # Test download_file
        local_path = await tf.download_file("s3://bucket/remote", "local.txt")
        assert local_path == "/local/path"
        mock_s3._process_s3_uri.assert_called_once()


@pytest.mark.asyncio
async def test_task_files_upload_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_s3 = MagicMock()
        mock_meta = FileMetadata(uri="s3://test/dir/", size=1000)
        mock_s3._upload_to_s3 = AsyncMock(return_value=mock_meta)

        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1", s3_manager=mock_s3)

        meta = await tf.upload_dir()
        assert meta == mock_meta
        # Verify it passed the task root directory and prefix
        mock_s3._upload_to_s3.assert_called_with(tmpdir, s3_prefix="job1")
