from asyncio import gather, to_thread
from os import walk
from os.path import basename, dirname, join, relpath
from shutil import rmtree
from typing import Any
from urllib.parse import urlparse

from aioboto3 import Session
from aiofiles.os import makedirs
from aiofiles.ospath import exists, isdir
from botocore.client import Config

from .config import WorkerConfig


class S3Manager:
    """Handles S3 payload offloading."""

    def __init__(self, config: WorkerConfig):
        self._config = config
        self._session = Session()

    def _get_client_args(self) -> dict[str, Any]:
        """Returns standard arguments for S3 client creation."""
        return {
            "service_name": "s3",
            "endpoint_url": self._config.S3_ENDPOINT_URL,
            "aws_access_key_id": self._config.S3_ACCESS_KEY,
            "aws_secret_access_key": self._config.S3_SECRET_KEY,
            "config": Config(signature_version="s3v4"),
        }

    async def cleanup(self, task_id: str):
        """Removes the task-specific payload directory."""
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        if await exists(task_dir):
            await to_thread(lambda: rmtree(task_dir, ignore_errors=True))

    async def _process_s3_uri(self, uri: str, task_id: str) -> str:
        """Downloads a file or a folder (if uri ends with /) from S3 and returns the local path."""
        parsed_url = urlparse(uri)
        bucket_name = parsed_url.netloc
        object_key = parsed_url.path.lstrip("/")

        # Use task-specific directory for isolation
        local_dir_root = join(self._config.TASK_FILES_DIR, task_id)
        await makedirs(local_dir_root, exist_ok=True)

        async with self._session.client(**self._get_client_args()) as s3:
            # Handle folder download (prefix)
            if uri.endswith("/"):
                folder_name = object_key.rstrip("/").split("/")[-1]
                local_folder_path = join(local_dir_root, folder_name)

                paginator = s3.get_paginator("list_objects_v2")
                tasks = []
                async for page in paginator.paginate(Bucket=bucket_name, Prefix=object_key):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        if key.endswith("/"):
                            continue

                        # Calculate relative path inside the folder
                        rel_path = key[len(object_key) :]
                        local_file_path = join(local_folder_path, rel_path)

                        await makedirs(dirname(local_file_path), exist_ok=True)
                        tasks.append(s3.download_file(bucket_name, key, local_file_path))

                if tasks:
                    await gather(*tasks)
                return local_folder_path

            # Handle single file download
            local_path = join(local_dir_root, basename(object_key))
            await s3.download_file(bucket_name, object_key, local_path)
            return local_path

    async def _upload_to_s3(self, local_path: str) -> str:
        """Uploads a file or a folder to S3 and returns the S3 URI."""
        bucket_name = self._config.S3_DEFAULT_BUCKET

        async with self._session.client(**self._get_client_args()) as s3:
            # Handle folder upload
            if await isdir(local_path):
                folder_name = basename(local_path.rstrip("/"))
                s3_prefix = f"{folder_name}/"
                tasks = []

                # Use to_thread to avoid blocking event loop during file walk
                def _get_files_to_upload():
                    files_to_upload = []
                    for root, _, files in walk(local_path):
                        for file in files:
                            f_path = join(root, file)
                            rel = relpath(f_path, local_path)
                            files_to_upload.append((f_path, f"{s3_prefix}{rel}"))
                    return files_to_upload

                files_list = await to_thread(_get_files_to_upload)

                for full_path, key in files_list:
                    tasks.append(s3.upload_file(full_path, bucket_name, key))

                if tasks:
                    await gather(*tasks)

                return f"s3://{bucket_name}/{s3_prefix}"

            # Handle single file upload
            object_key = basename(local_path)
            await s3.upload_file(local_path, bucket_name, object_key)
            return f"s3://{bucket_name}/{object_key}"

    async def process_params(self, params: dict[str, Any], task_id: str) -> dict[str, Any]:
        """Recursively searches for S3 URIs in params and downloads the files."""
        if not self._config.S3_ENDPOINT_URL:
            return params

        async def _process(item: Any) -> Any:
            if isinstance(item, str) and item.startswith("s3://"):
                return await self._process_s3_uri(item, task_id)
            if isinstance(item, dict):
                return {k: await _process(v) for k, v in item.items()}
            return [await _process(i) for i in item] if isinstance(item, list) else item

        return await _process(params)

    async def process_result(self, result: dict[str, Any]) -> dict[str, Any]:
        """Recursively searches for local file paths in the result and uploads them to S3."""
        if not self._config.S3_ENDPOINT_URL:
            return result

        async def _process(item: Any) -> Any:
            if isinstance(item, str) and item.startswith(self._config.TASK_FILES_DIR):
                return await self._upload_to_s3(item) if await exists(item) else item
            if isinstance(item, dict):
                return {k: await _process(v) for k, v in item.items()}
            return [await _process(i) for i in item] if isinstance(item, list) else item

        return await _process(result)
