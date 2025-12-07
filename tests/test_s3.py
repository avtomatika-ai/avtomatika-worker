import os
import tempfile

import boto3
import pytest
from moto import mock_s3

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.s3 import S3Manager


@pytest.fixture
def s3_manager():
    """Provides an S3Manager instance with a mocked S3 client."""
    with mock_s3():
        config = WorkerConfig()
        config.S3_ENDPOINT_URL = "http://localhost:5000"
        config.S3_ACCESS_KEY = "testing"
        config.S3_SECRET_KEY = "testing"
        config.S3_DEFAULT_BUCKET = "test-bucket"
        config.WORKER_PAYLOAD_DIR = tempfile.gettempdir()
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="test-bucket")
        manager = S3Manager(config)
        manager._s3 = s3
        yield manager


@pytest.mark.asyncio
async def test_process_s3_uri(s3_manager):
    """Tests that _process_s3_uri downloads a file from S3."""
    s3_manager._s3.put_object(Bucket="test-bucket", Key="test-file.txt", Body=b"test content")

    local_path = await s3_manager._process_s3_uri("s3://test-bucket/test-file.txt")

    assert os.path.exists(local_path)
    with open(local_path, "rb") as f:
        assert f.read() == b"test content"


@pytest.mark.asyncio
async def test_upload_to_s3(s3_manager):
    """Tests that _upload_to_s3 uploads a file to S3."""
    with tempfile.NamedTemporaryFile(delete=False, dir=s3_manager._config.WORKER_PAYLOAD_DIR) as tmp:
        tmp.write(b"upload content")
        local_path = tmp.name

    s3_uri = await s3_manager._upload_to_s3(local_path)

    assert s3_uri.startswith("s3://test-bucket/")
    obj = s3_manager._s3.get_object(Bucket="test-bucket", Key=os.path.basename(local_path))
    content = obj["Body"].read()
    assert content == b"upload content"


@pytest.mark.asyncio
async def test_process_params(s3_manager):
    """Tests that process_params correctly downloads S3 files."""
    s3_manager._s3.put_object(Bucket="test-bucket", Key="test-file.txt", Body=b"test content")

    params = {"file": "s3://test-bucket/test-file.txt", "other": "value"}
    processed_params = await s3_manager.process_params(params)

    assert processed_params["other"] == "value"
    local_path = processed_params["file"]
    assert os.path.exists(local_path)
    with open(local_path, "rb") as f:
        assert f.read() == b"test content"


@pytest.mark.asyncio
async def test_process_result(s3_manager):
    """Tests that process_result correctly uploads local files."""
    with tempfile.NamedTemporaryFile(delete=False, dir=s3_manager._config.WORKER_PAYLOAD_DIR) as tmp:
        tmp.write(b"result content")
        local_path = tmp.name

    result = {"data": {"output_file": local_path}}
    processed_result = await s3_manager.process_result(result)

    s3_uri = processed_result["data"]["output_file"]
    assert s3_uri.startswith("s3://test-bucket/")
    obj = s3_manager._s3.get_object(Bucket="test-bucket", Key=os.path.basename(local_path))
    content = obj["Body"].read()
    assert content == b"result content"
