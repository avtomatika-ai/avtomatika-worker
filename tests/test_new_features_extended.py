import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from rxon.models import FileMetadata, WorkerCommand
from rxon.testing import MockTransport

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.task_files import TaskFiles
from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_sync_command_handler():
    """Test sync handler for a custom command (Happy path)."""
    transport = MockTransport()
    transport.connected = True
    worker = Worker(clients=[({"url": "http://test", "weight": 1}, transport)])

    received_cmd = None

    @worker.on_command("sync_cmd")
    def handle_sync(command: WorkerCommand):
        nonlocal received_cmd
        received_cmd = command.command

    listener_task = asyncio.create_task(worker._listen_to_single_transport(transport))
    transport.push_command(WorkerCommand(command="sync_cmd"))

    await asyncio.sleep(0.1)

    assert received_cmd == "sync_cmd"
    listener_task.cancel()


def test_get_resources_usage_with_libraries():
    """Test metrics collection when psutil and GPUtil are present (Happy Path)."""
    worker = Worker()

    mock_psutil = MagicMock()
    mock_psutil.cpu_percent.return_value = 12.5
    mock_psutil.virtual_memory.return_value.used = 1024**3 * 2

    mock_gputil = MagicMock()
    mock_gpu = MagicMock()
    mock_gpu.id = 0
    mock_gpu.load = 0.5
    mock_gpu.memoryUsed = 4096
    mock_gpu.temperature = 70
    mock_gputil.getGPUs.return_value = [mock_gpu]

    with patch.dict(sys.modules, {"psutil": mock_psutil, "GPUtil": mock_gputil}):
        if "psutil" in sys.modules:
            del sys.modules["psutil"]
        if "GPUtil" in sys.modules:
            del sys.modules["GPUtil"]
        with patch.dict(sys.modules, {"psutil": mock_psutil, "GPUtil": mock_gputil}):
            usage = worker._get_resources_usage()

            assert usage is not None
            assert usage.cpu_load_percent == 12.5
            assert usage.ram_used_gb == 2.0
            assert usage.devices_usage is not None
            assert usage.devices_usage[0].unit_id == "0"
            assert usage.devices_usage[0].metrics["temperature_c"] == 70


def test_get_resources_usage_without_libraries():
    """Test behavior WITHOUT libraries (Edge case)."""
    worker = Worker()
    with patch.dict(sys.modules, {"psutil": None, "GPUtil": None}):
        usage = worker._get_resources_usage()
        assert usage is not None
        assert usage.cpu_load_percent == 0.0
        assert usage.devices_usage is None


@pytest.mark.asyncio
async def test_task_files_download_integrity_ok(tmp_path):
    """Test file download via TaskFiles with successful integrity check."""
    config = WorkerConfig()
    config.TASK_FILES_DIR = str(tmp_path)
    worker = Worker(config=config)

    tf = TaskFiles(task_dir=str(tmp_path / "task1"), job_id="j1", task_id="t1", s3_manager=worker._s3_manager)
    verify_meta = FileMetadata(uri="s3://b/k", size=5)

    with patch.object(worker._s3_manager._provider, "download", new_callable=AsyncMock) as mock_dl:

        async def mock_download(uri, local_path):
            with open(local_path, "w") as f:
                f.write("12345")

        mock_dl.side_effect = mock_download

        path = await tf.download_file("s3://b/k", "test.txt", verify_meta=verify_meta)
        assert path.endswith("test.txt")


@pytest.mark.asyncio
async def test_task_files_download_integrity_fail(tmp_path):
    """Test file download via TaskFiles with integrity error."""
    config = WorkerConfig()
    config.TASK_FILES_DIR = str(tmp_path)
    worker = Worker(config=config)

    tf = TaskFiles(task_dir=str(tmp_path / "task1"), job_id="j1", task_id="t1", s3_manager=worker._s3_manager)
    verify_meta = FileMetadata(uri="s3://b/k", size=10)  # Expect 10

    with patch.object(worker._s3_manager._provider, "download", new_callable=AsyncMock) as mock_dl:

        async def mock_download(uri, local_path):
            with open(local_path, "w") as f:
                f.write("wrong")  # Give 5

        mock_dl.side_effect = mock_download

        with pytest.raises(ValueError, match="Size mismatch"):
            await tf.download_file("s3://b/k", "test.txt", verify_meta=verify_meta)


@pytest.mark.asyncio
async def test_task_files_upload_dir_new_api(tmp_path):
    """Test directory upload via new TaskFiles API."""
    config = WorkerConfig()
    config.TASK_FILES_DIR = str(tmp_path)
    worker = Worker(config=config)

    # Create test directory
    test_dir = tmp_path / "to_upload"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("content")

    tf = TaskFiles(task_dir=str(tmp_path / "task1"), job_id="j1", task_id="t1", s3_manager=worker._s3_manager)

    # Mock process_result in S3Manager
    mock_meta = FileMetadata(uri="s3://b/j1/root/", size=0)
    with patch.object(worker._s3_manager, "process_result", new_callable=AsyncMock) as mock_pr:
        mock_pr.return_value = ({}, {"root": mock_meta})

        res_meta = await tf.upload_dir(str(test_dir))
        assert res_meta.uri == "s3://b/j1/root/"
        mock_pr.assert_awaited_once()
