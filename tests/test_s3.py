# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.s3 import S3Manager


@pytest.fixture
def s3_manager():
    config = WorkerConfig()
    config.S3_ENDPOINT_URL = "http://localhost:9000"
    config.S3_ACCESS_KEY = "test-key"
    config.S3_SECRET_KEY = "test-secret"
    config.S3_DEFAULT_BUCKET = "test-bucket"
    config.TASK_FILES_DIR = tempfile.mkdtemp()
    return S3Manager(config)


class MockObjectMeta:
    def __init__(self, key):
        self.key = key
        self.path = key


class MockGetResult:
    def __init__(self, data=b"test content"):
        self.data = data
        self.meta = MagicMock()
        self.meta.size = len(data)
        self.meta.e_tag = '"etag"'

    async def stream(self):
        yield self.data


class MockAsyncIterator:
    def __init__(self, items):
        self.items = items

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.items:
            raise StopAsyncIteration
        return self.items.pop(0)

    async def collect(self):
        return self.items


@pytest.mark.asyncio
async def test_get_store_caching(s3_manager):
    """Tests that S3Store instances are cached by bucket name."""
    with patch("avtomatika_worker.s3.S3Store", side_effect=lambda *args, **kwargs: MagicMock()) as mock_store:
        store1 = s3_manager._provider._get_store("bucket1")
        store2 = s3_manager._provider._get_store("bucket1")
        store3 = s3_manager._provider._get_store("bucket2")

        assert store1 is store2
        assert store1 is not store3
        assert mock_store.call_count == 2


@pytest.mark.asyncio
async def test_download_file(s3_manager):
    """Tests downloading a single file via provider."""
    local_path = os.path.join(s3_manager._config.TASK_FILES_DIR, "downloaded.txt")

    with (
        patch("avtomatika_worker.s3.obstore_get", new_callable=AsyncMock) as mock_get,
        patch.object(s3_manager._provider, "_get_store", return_value=MagicMock()),
    ):
        mock_get.return_value = MockGetResult(b"hello world")
        success = await s3_manager._provider.download("s3://test-bucket/hello.txt", local_path)

    assert success
    with open(local_path, "rb") as f:
        assert f.read() == b"hello world"


@pytest.mark.asyncio
async def test_upload_file(s3_manager):
    """Tests uploading a single file via provider."""
    local_path = os.path.join(s3_manager._config.TASK_FILES_DIR, "to_upload.txt")
    with open(local_path, "w") as f:
        f.write("upload me")

    with (
        patch("avtomatika_worker.s3.obstore_put", new_callable=AsyncMock) as mock_put,
        patch.object(s3_manager._provider, "_get_store", return_value=MagicMock()),
    ):
        mock_put.return_value = {"e_tag": '"new-etag"'}
        etag = await s3_manager._provider.upload(local_path, "s3://test-bucket/target.txt")

    assert etag == "new-etag"
    mock_put.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_params_integration(s3_manager):
    """Tests that process_params correctly triggers downloads via provider."""
    params = {"input_file": "s3://test-bucket/input.jpg"}
    task_id = "task-params"

    with patch.object(s3_manager._provider, "download", new_callable=AsyncMock) as mock_dl:
        processed = await s3_manager.process_params(params, task_id)

        assert processed["input_file"].endswith("input.jpg")
        mock_dl.assert_called_once()


@pytest.mark.asyncio
async def test_process_result_integration(s3_manager):
    """Tests that process_result correctly triggers uploads via provider."""
    local_path = os.path.join(s3_manager._config.TASK_FILES_DIR, "output.pdf")
    with open(local_path, "wb") as f:
        f.write(b"pdf data")

    result = {"report": local_path}

    with patch.object(s3_manager._provider, "upload", new_callable=AsyncMock) as mock_up:
        mock_up.return_value = "res-etag"
        updated, meta = await s3_manager.process_result(result, s3_prefix="job-1")

        assert updated["report"].startswith("s3://")
        assert "report" in meta
        assert meta["report"].etag == "res-etag"


@pytest.mark.asyncio
async def test_cleanup(s3_manager):
    """Tests that cleanup removes the task directory."""
    task_id = "to-cleanup"
    task_dir = os.path.join(s3_manager._config.TASK_FILES_DIR, task_id)
    os.makedirs(task_dir, exist_ok=True)

    with open(os.path.join(task_dir, "file.txt"), "w") as f:
        f.write("temp")

    await s3_manager.cleanup(task_id)
    assert not os.path.exists(task_dir)


@pytest.mark.asyncio
async def test_delete_methods(s3_manager):
    """Tests delete and delete_dir methods of the provider."""
    with (
        patch("avtomatika_worker.s3.obstore_delete", new_callable=AsyncMock) as mock_del,
        patch.object(s3_manager._provider, "_get_store", return_value=MagicMock()),
    ):
        # Test delete single
        await s3_manager._provider.delete("s3://b/file.txt")
        mock_del.assert_called_once()

        # Test delete dir (uses list + delete)
        mock_del.reset_mock()
        mock_list = MagicMock()
        mock_list.collect = AsyncMock(return_value=[MockObjectMeta("dir/f1"), MockObjectMeta("dir/f2")])

        with patch("avtomatika_worker.s3.obstore_list", return_value=mock_list):
            await s3_manager._provider.delete_dir("s3://b/dir/")
            assert mock_del.call_count == 2
