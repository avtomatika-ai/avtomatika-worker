import tempfile
from unittest.mock import AsyncMock, MagicMock

import pytest
from rxon.models import FileMetadata

from avtomatika_worker.task_files import TaskFiles


@pytest.mark.asyncio
async def test_task_files_s3_proxy_methods():
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_s3 = MagicMock()
        mock_s3._config.S3_DEFAULT_BUCKET = "test-bucket"
        mock_s3._provider = MagicMock()
        mock_s3._provider.upload = AsyncMock(return_value="hash")
        mock_s3._provider.download = AsyncMock()

        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1", s3_manager=mock_s3)
        # Test upload_file
        await tf.write("hello.txt", "content")
        meta = await tf.upload_file("hello.txt")
        assert "s3://test-bucket/job1/hello.txt" in meta.uri
        assert meta.etag == "hash"
        mock_s3._provider.upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_files_upload_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_s3 = MagicMock()
        mock_meta = FileMetadata(uri="s3://test/dir/", size=1000)
        mock_s3.process_result = AsyncMock(return_value=({}, {"root": mock_meta}))

        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1", s3_manager=mock_s3)

        meta = await tf.upload_dir()
        assert meta.uri == "s3://test/dir/"
        mock_s3.process_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_task_files_download():
    with tempfile.TemporaryDirectory() as tmpdir:
        mock_s3 = MagicMock()
        mock_s3._provider = MagicMock()
        mock_s3._provider.download = AsyncMock()

        tf = TaskFiles(tmpdir, job_id="job1", task_id="task1", s3_manager=mock_s3)

        path = await tf.download_file("s3://test/file", "local.txt")
        assert path.endswith("local.txt")
        mock_s3._provider.download.assert_awaited_once()
