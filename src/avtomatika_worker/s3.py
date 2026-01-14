from asyncio import Semaphore, gather, to_thread
from logging import getLogger
from os import walk
from os.path import basename, dirname, join, relpath
from shutil import rmtree
from typing import Any, cast
from urllib.parse import urlparse

import obstore
from aiofiles import open as aio_open
from aiofiles.os import makedirs
from aiofiles.ospath import exists, isdir
from obstore.store import S3Store

from .config import WorkerConfig

logger = getLogger(__name__)

# Limit concurrent S3 operations to avoid "Too many open files"
MAX_S3_CONCURRENCY = 50


class S3Manager:
    """Handles S3 payload offloading using obstore (high-performance async S3 client)."""

    def __init__(self, config: WorkerConfig):
        self._config = config
        self._stores: dict[str, S3Store] = {}
        self._semaphore = Semaphore(MAX_S3_CONCURRENCY)

    def _get_store(self, bucket_name: str) -> S3Store:
        """Creates or returns a cached S3Store for a specific bucket."""
        if bucket_name in self._stores:
            return self._stores[bucket_name]

        config_kwargs = {
            "aws_access_key_id": self._config.S3_ACCESS_KEY,
            "aws_secret_access_key": self._config.S3_SECRET_KEY,
            "region": "us-east-1",  # Default region if not specified, required by some clients
        }

        if self._config.S3_ENDPOINT_URL:
            config_kwargs["endpoint"] = self._config.S3_ENDPOINT_URL
            if self._config.S3_ENDPOINT_URL.startswith("http://"):
                config_kwargs["allow_http"] = "true"

        # Filter out None values
        config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}

        try:
            store = S3Store(bucket_name, **config_kwargs)
            self._stores[bucket_name] = store
            return store
        except Exception as e:
            logger.error(f"Failed to create S3Store for bucket {bucket_name}: {e}")
            raise

    async def cleanup(self, task_id: str) -> None:
        """Removes the task-specific payload directory."""
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        if await exists(task_dir):
            await to_thread(lambda: rmtree(task_dir, ignore_errors=True))

    async def _process_s3_uri(self, uri: str, task_id: str) -> str:
        """Downloads a file or a folder (if uri ends with /) from S3 and returns the local path."""
        try:
            parsed_url = urlparse(uri)
            bucket_name = parsed_url.netloc
            object_key = parsed_url.path.lstrip("/")
            store = self._get_store(bucket_name)

            # Use task-specific directory for isolation
            local_dir_root = join(self._config.TASK_FILES_DIR, task_id)
            await makedirs(local_dir_root, exist_ok=True)

            logger.info(f"Starting download from S3: {uri}")

            # Handle folder download (prefix)
            if uri.endswith("/"):
                folder_name = object_key.rstrip("/").split("/")[-1]
                local_folder_path = join(local_dir_root, folder_name)

                # List objects with prefix
                # obstore.list returns an async iterator of ObjectMeta
                files_to_download = []

                # Note: obstore.list returns an async iterator.
                async for obj in obstore.list(store, prefix=object_key):
                    key = obj.key

                    if key.endswith("/"):
                        continue

                    # Calculate relative path inside the folder
                    rel_path = key[len(object_key) :]
                    local_file_path = join(local_folder_path, rel_path)

                    await makedirs(dirname(local_file_path), exist_ok=True)
                    files_to_download.append((key, local_file_path))

                async def _download_file(key: str, path: str) -> None:
                    async with self._semaphore:
                        result = await obstore.get(store, key)
                        async with aio_open(path, "wb") as f:
                            async for chunk in result.stream():
                                await f.write(chunk)

                # Execute downloads in parallel
                if files_to_download:
                    await gather(*[_download_file(k, p) for k, p in files_to_download])

                logger.info(f"Successfully downloaded folder from S3: {uri} ({len(files_to_download)} files)")
                return local_folder_path

            # Handle single file download
            local_path = join(local_dir_root, basename(object_key))

            result = await obstore.get(store, object_key)
            async with aio_open(local_path, "wb") as f:
                async for chunk in result.stream():
                    await f.write(chunk)

            logger.info(f"Successfully downloaded file from S3: {uri} -> {local_path}")
            return local_path

        except Exception as e:
            # Catching generic Exception because obstore might raise different errors.
            logger.exception(f"Error during download of {uri}: {e}")
            raise

    async def _upload_to_s3(self, local_path: str) -> str:
        """Uploads a file or a folder to S3 and returns the S3 URI."""
        bucket_name = self._config.S3_DEFAULT_BUCKET
        store = self._get_store(bucket_name)

        logger.info(f"Starting upload to S3 from local path: {local_path}")

        try:
            # Handle folder upload
            if await isdir(local_path):
                folder_name = basename(local_path.rstrip("/"))
                s3_prefix = f"{folder_name}/"

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

                async def _upload_file(path: str, key: str) -> None:
                    async with self._semaphore:
                        # obstore.put accepts bytes or file-like objects.
                        # Since we are in async, reading small files is fine.
                        with open(path, "rb") as f:
                            await obstore.put(store, key, f)

                if files_list:
                    # Upload in parallel
                    await gather(*[_upload_file(f, k) for f, k in files_list])

                s3_uri = f"s3://{bucket_name}/{s3_prefix}"
                logger.info(f"Successfully uploaded folder to S3: {local_path} -> {s3_uri} ({len(files_list)} files)")
                return s3_uri

            # Handle single file upload
            object_key = basename(local_path)
            with open(local_path, "rb") as f:
                await obstore.put(store, object_key, f)

            s3_uri = f"s3://{bucket_name}/{object_key}"
            logger.info(f"Successfully uploaded file to S3: {local_path} -> {s3_uri}")
            return s3_uri

        except Exception as e:
            logger.exception(f"Error during upload of {local_path}: {e}")
            raise

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

        return cast(dict[str, Any], await _process(params))

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

        return cast(dict[str, Any], await _process(result))
