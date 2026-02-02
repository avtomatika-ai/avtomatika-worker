from asyncio import Semaphore, gather, to_thread
from logging import getLogger
from os import walk
from os.path import basename, dirname, join, relpath
from shutil import rmtree
from typing import Any, cast

from aiofiles import open as aio_open
from aiofiles.os import makedirs
from aiofiles.ospath import exists, getsize, isdir
from obstore import get as obstore_get
from obstore import list as obstore_list
from obstore import put as obstore_put
from obstore.store import S3Store
from rxon.blob import parse_uri
from rxon.exceptions import IntegrityError
from rxon.models import FileMetadata

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

    async def _process_s3_uri(self, uri: str, task_id: str, verify_meta: FileMetadata | None = None) -> str:
        """Downloads a file or a folder from S3 and returns the local path.
        If verify_meta is provided, performs integrity checks.
        """
        try:
            bucket_name, object_key, is_directory = parse_uri(uri)
            store = self._get_store(bucket_name)

            # Use task-specific directory for isolation
            local_dir_root = join(self._config.TASK_FILES_DIR, task_id)
            await makedirs(local_dir_root, exist_ok=True)

            logger.info(f"Starting download from S3: {uri}")

            # Handle folder download (prefix)
            if is_directory:
                folder_name = object_key.rstrip("/").split("/")[-1]
                local_folder_path = join(local_dir_root, folder_name)
                files_to_download = []

                async for obj in obstore_list(store, prefix=object_key):
                    key = obj.key
                    if key.endswith("/"):
                        continue
                    rel_path = key[len(object_key) :]
                    local_file_path = join(local_folder_path, rel_path)
                    await makedirs(dirname(local_file_path), exist_ok=True)
                    files_to_download.append((key, local_file_path))

                async def _download_file(key: str, path: str) -> None:
                    async with self._semaphore:
                        result = await obstore_get(store, key)
                        async with aio_open(path, "wb") as f:
                            async for chunk in result.stream():
                                await f.write(chunk)

                if files_to_download:
                    await gather(*[_download_file(k, p) for k, p in files_to_download])

                logger.info(f"Successfully downloaded folder from S3: {uri} ({len(files_to_download)} files)")
                return local_folder_path

            # Handle single file download
            local_path = join(local_dir_root, basename(object_key))

            result = await obstore_get(store, object_key)

            # Integrity check before download
            if verify_meta:
                if verify_meta.size != result.meta.size:
                    raise IntegrityError(
                        f"Size mismatch for {uri}: expected {verify_meta.size}, got {result.meta.size}"
                    )
                if verify_meta.etag and result.meta.e_tag:
                    actual_etag = result.meta.e_tag.strip('"')
                    expected_etag = verify_meta.etag.strip('"')
                    if actual_etag != expected_etag:
                        raise IntegrityError(f"ETag mismatch for {uri}: expected {expected_etag}, got {actual_etag}")

            async with aio_open(local_path, "wb") as f:
                async for chunk in result.stream():
                    await f.write(chunk)

            logger.info(f"Successfully downloaded file from S3: {uri} -> {local_path}")
            return local_path

        except Exception as e:
            # Catching generic Exception because obstore might raise different errors.
            logger.exception(f"Error during download of {uri}: {e}")
            raise

    async def _upload_to_s3(self, local_path: str) -> FileMetadata:
        """Uploads a file or a folder to S3 and returns FileMetadata."""
        bucket_name = self._config.S3_DEFAULT_BUCKET
        store = self._get_store(bucket_name)

        logger.info(f"Starting upload to S3 from local path: {local_path}")

        try:
            # Handle folder upload
            if await isdir(local_path):
                folder_name = basename(local_path.rstrip("/"))
                s3_prefix = f"{folder_name}/"

                def _get_files_to_upload():
                    from os.path import getsize as std_getsize

                    files_to_upload = []
                    total_size = 0
                    for root, _, files in walk(local_path):
                        for file in files:
                            f_path = join(root, file)
                            rel = relpath(f_path, local_path)
                            total_size += std_getsize(f_path)
                            files_to_upload.append((f_path, f"{s3_prefix}{rel}"))
                    return files_to_upload, total_size

                files_list, total_size = await to_thread(_get_files_to_upload)

                async def _upload_file(path: str, key: str) -> None:
                    async with self._semaphore:
                        with open(path, "rb") as f:
                            await obstore_put(store, key, f)

                if files_list:
                    await gather(*[_upload_file(f, k) for f, k in files_list])

                s3_uri = f"s3://{bucket_name}/{s3_prefix}"
                logger.info(f"Successfully uploaded folder to S3: {local_path} -> {s3_uri} ({len(files_list)} files)")
                return FileMetadata(uri=s3_uri, size=total_size)

            # Handle single file upload
            object_key = basename(local_path)
            file_size = await getsize(local_path)
            with open(local_path, "rb") as f:
                put_result = await obstore_put(store, object_key, f)

            s3_uri = f"s3://{bucket_name}/{object_key}"
            etag = put_result.e_tag.strip('"') if put_result.e_tag else None
            logger.info(f"Successfully uploaded file to S3: {local_path} -> {s3_uri} (ETag: {etag})")
            return FileMetadata(uri=s3_uri, size=file_size, etag=etag)

        except Exception as e:
            logger.exception(f"Error during upload of {local_path}: {e}")
            raise

    async def process_params(
        self, params: dict[str, Any], task_id: str, metadata: dict[str, FileMetadata] | None = None
    ) -> dict[str, Any]:
        """Recursively searches for S3 URIs in params and downloads the files.
        Uses metadata for integrity verification if available.
        """
        if not self._config.S3_ENDPOINT_URL:
            return params

        async def _process(item: Any, key_path: str = "") -> Any:
            if isinstance(item, str) and item.startswith("s3://"):
                verify_meta = metadata.get(key_path) if metadata else None
                return await self._process_s3_uri(item, task_id, verify_meta=verify_meta)
            if isinstance(item, dict):
                return {k: await _process(v, f"{key_path}.{k}" if key_path else k) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(v, f"{key_path}[{i}]") for i, v in enumerate(item)]
            return item

        return cast(dict[str, Any], await _process(params))

    async def process_result(self, result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, FileMetadata]]:
        """Recursively searches for local file paths in the result and uploads them to S3.
        Returns a tuple of (updated_result, metadata_map).
        """
        if not self._config.S3_ENDPOINT_URL:
            return result, {}

        metadata_map = {}

        async def _process(item: Any, key_path: str = "") -> Any:
            if isinstance(item, str) and item.startswith(self._config.TASK_FILES_DIR):
                if await exists(item):
                    meta = await self._upload_to_s3(item)
                    metadata_map[key_path] = meta
                    return meta.uri
                return item
            if isinstance(item, dict):
                return {k: await _process(v, f"{key_path}.{k}" if key_path else k) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(v, f"{key_path}[{i}]") for i, v in enumerate(item)]
            return item

        updated_result = cast(dict[str, Any], await _process(result))
        return updated_result, metadata_map
