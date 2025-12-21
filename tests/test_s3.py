import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.s3 import S3Manager


@pytest.fixture
def s3_manager():
    """Provides an S3Manager instance for testing with mocked aioboto3."""
    config = WorkerConfig()
    config.S3_ENDPOINT_URL = "http://localhost:5000"
    config.S3_ACCESS_KEY = "testing"
    config.S3_SECRET_KEY = "testing"
    config.S3_DEFAULT_BUCKET = "test-bucket"
    config.TASK_FILES_DIR = tempfile.gettempdir()

    manager = S3Manager(config)
    return manager


# Helper class for mocking async iterator (for paginator)
class MockAsyncIterator:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)


@pytest.mark.asyncio
async def test_process_s3_uri_file(s3_manager):
    """Tests that _process_s3_uri downloads a single file from S3."""
    mock_s3_client = AsyncMock()
    mock_s3_client.download_file = AsyncMock()

    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__.return_value = mock_s3_client

    task_id = "task-123"

    with patch.object(s3_manager._session, "client", return_value=mock_context_manager):
        local_path = await s3_manager._process_s3_uri("s3://test-bucket/test-file.txt", task_id)

    # Path should include task_id
    expected_suffix = os.path.join(task_id, "test-file.txt")
    assert local_path.endswith(expected_suffix)

    mock_s3_client.download_file.assert_awaited_once_with("test-bucket", "test-file.txt", local_path)


@pytest.mark.asyncio
async def test_process_s3_uri_folder(s3_manager):
    """Tests that _process_s3_uri downloads a folder (prefix) from S3."""
    mock_s3_client = AsyncMock()
    mock_s3_client.download_file = AsyncMock()

    # Mock paginator
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = MockAsyncIterator(
        [{"Contents": [{"Key": "data/file1.txt"}, {"Key": "data/sub/file2.txt"}]}]
    )

    # get_paginator is synchronous in aioboto3
    mock_s3_client.get_paginator = MagicMock(return_value=mock_paginator)

    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__.return_value = mock_s3_client

    task_id = "task-folder"

    with patch.object(s3_manager._session, "client", return_value=mock_context_manager):
        local_path = await s3_manager._process_s3_uri("s3://test-bucket/data/", task_id)

    assert local_path.endswith(os.path.join(task_id, "data"))

    # Verify download calls
    assert mock_s3_client.download_file.await_count == 2


@pytest.mark.asyncio
async def test_upload_to_s3_file(s3_manager):
    """Tests that _upload_to_s3 uploads a single file to S3."""
    mock_s3_client = AsyncMock()
    mock_s3_client.upload_file = AsyncMock()

    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__.return_value = mock_s3_client

    local_path = os.path.join(s3_manager._config.TASK_FILES_DIR, "test-upload.txt")
    with open(local_path, "w") as f:
        f.write("test content")

    try:
        with patch.object(s3_manager._session, "client", return_value=mock_context_manager):
            s3_uri = await s3_manager._upload_to_s3(local_path)

        assert s3_uri == "s3://test-bucket/test-upload.txt"
        mock_s3_client.upload_file.assert_awaited_once_with(local_path, "test-bucket", "test-upload.txt")
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


@pytest.mark.asyncio
async def test_upload_to_s3_folder(s3_manager):
    """Tests that _upload_to_s3 uploads a folder recursively to S3."""
    mock_s3_client = AsyncMock()
    mock_s3_client.upload_file = AsyncMock()

    mock_context_manager = MagicMock()
    mock_context_manager.__aenter__.return_value = mock_s3_client

    # Create temp directory structure
    with tempfile.TemporaryDirectory(dir=s3_manager._config.TASK_FILES_DIR) as tmpdir:
        # Create file1
        with open(os.path.join(tmpdir, "file1.txt"), "w") as f:
            f.write("content1")

        dirname = os.path.basename(tmpdir)

        with patch.object(s3_manager._session, "client", return_value=mock_context_manager):
            s3_uri = await s3_manager._upload_to_s3(tmpdir)

        assert s3_uri == f"s3://test-bucket/{dirname}/"
        assert mock_s3_client.upload_file.await_count == 1


@pytest.mark.asyncio
async def test_process_params(s3_manager):
    """Tests that process_params correctly calls _process_s3_uri with task_id."""
    with patch.object(s3_manager, "_process_s3_uri", side_effect=lambda uri, tid: f"/local/{tid}/{uri.split('/')[-1]}"):
        params = {"file": "s3://test-bucket/test-file.txt", "other": "value"}
        processed_params = await s3_manager.process_params(params, "task-1")

        assert processed_params["other"] == "value"
        assert processed_params["file"] == "/local/task-1/test-file.txt"


@pytest.mark.asyncio
async def test_process_result(s3_manager):
    """Tests that process_result correctly calls _upload_to_s3."""
    local_path = os.path.join(s3_manager._config.TASK_FILES_DIR, "output.txt")
    with open(local_path, "w") as f:
        f.write("test content")

    try:
        with patch.object(s3_manager, "_upload_to_s3", return_value="s3://bucket/output.txt"):
            result = {"data": {"output_file": local_path}}
            processed_result = await s3_manager.process_result(result)

            assert processed_result["data"]["output_file"] == "s3://bucket/output.txt"
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


@pytest.mark.asyncio
async def test_cleanup(s3_manager):
    """Tests that cleanup removes the task directory."""
    task_id = "cleanup-task"
    task_dir = os.path.join(s3_manager._config.TASK_FILES_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)
    with open(os.path.join(task_dir, "file.txt"), "w") as f:
        f.write("data")

    assert os.path.exists(task_dir)

    await s3_manager.cleanup(task_id)

    assert not os.path.exists(task_dir)
