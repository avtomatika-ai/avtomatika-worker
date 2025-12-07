import asyncio
import os
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.client import Config

from .config import WorkerConfig


class S3Manager:
    """Handles S3 payload offloading."""

    def __init__(self, config: WorkerConfig):
        self._config = config
        self._s3 = boto3.client(
            "s3",
            endpoint_url=self._config.S3_ENDPOINT_URL,
            aws_access_key_id=self._config.S3_ACCESS_KEY,
            aws_secret_access_key=self._config.S3_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )

    async def _process_s3_uri(self, uri: str) -> str:
        """Downloads a file from S3 and returns the local path."""
        parsed_url = urlparse(uri)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")
        local_dir = self._config.WORKER_PAYLOAD_DIR
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, os.path.basename(object_key))

        await asyncio.to_thread(self._s3.download_file, bucket_name, object_key, local_path)
        return local_path

    async def _upload_to_s3(self, local_path: str) -> str:
        """Uploads a file to S3 and returns the S3 URI."""
        bucket_name = self._config.S3_DEFAULT_BUCKET
        object_key = os.path.basename(local_path)

        await asyncio.to_thread(self._s3.upload_file, local_path, bucket_name, object_key)
        return f"s3://{bucket_name}/{object_key}"

    async def process_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Recursively searches for S3 URIs in params and downloads the files."""
        if not self._config.S3_ENDPOINT_URL:
            return params

        async def _process(item: Any) -> Any:
            if isinstance(item, str) and item.startswith("s3://"):
                return await self._process_s3_uri(item)
            if isinstance(item, dict):
                return {k: await _process(v) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(i) for i in item]
            return item

        return await _process(params)

    async def process_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Recursively searches for local file paths in the result and uploads them to S3."""
        if not self._config.S3_ENDPOINT_URL:
            return result

        async def _process(item: Any) -> Any:
            if isinstance(item, str) and os.path.exists(item) and item.startswith(self._config.WORKER_PAYLOAD_DIR):
                return await self._upload_to_s3(item)
            if isinstance(item, dict):
                return {k: await _process(v) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(i) for i in item]
            return item

        return await _process(result)
