# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from asyncio import Semaphore, gather, sleep, to_thread
from logging import getLogger
from os import walk
from os.path import basename, dirname, join, relpath
from shutil import rmtree
from typing import Any, cast

from aiofiles import open as aio_open
from aiofiles.os import makedirs
from aiofiles.ospath import exists, getsize, isdir
from rxon.blob import parse_uri
from rxon.exceptions import IntegrityError, RxonError
from rxon.models import FileMetadata

from .config import WorkerConfig
from .observability import ObservabilityManager

logger = getLogger(__name__)

try:
    from obstore import get as obstore_get
    from obstore import list as obstore_list
    from obstore import put_async as obstore_put
    from obstore.store import S3Store

    _HAS_S3 = True
except ImportError:
    _HAS_S3 = False
    S3Store = Any
    obstore_get = None
    obstore_list = None
    obstore_put = None

# Limit concurrent S3 operations to avoid "Too many open files"
MAX_S3_CONCURRENCY = 50


class S3Manager:
    """Handles S3 payload offloading using obstore (high-performance async S3 client)."""

    def __init__(self, config: WorkerConfig, observability: ObservabilityManager | None = None):
        self._config = config
        self._stores: dict[str, S3Store] = {}
        self._semaphore = Semaphore(MAX_S3_CONCURRENCY)
        self._active_ops = 0
        self._observability = observability or ObservabilityManager(enabled=False)

    async def wait_all_done(self, timeout: float = 30.0) -> None:
        """Waits for all active S3 operations to complete."""
        if self._active_ops == 0:
            return
        logger.info(f"Waiting for {self._active_ops} active S3 operations to complete (timeout: {timeout}s)...")
        sleep_delay = 0.1
        total_slept = 0.0
        while self._active_ops > 0 and total_slept < timeout:
            await sleep(sleep_delay)
            total_slept += sleep_delay
        if self._active_ops > 0:
            logger.warning(f"Graceful shutdown for S3 timed out. {self._active_ops} operations still active.")
        else:
            logger.info("All S3 operations completed successfully.")

    def _check_availability(self) -> None:
        if not _HAS_S3:
            raise RuntimeError(
                "S3 support is not installed. Please install 'avtomatika-worker[s3]' to use S3 features."
            )

    def _get_store(self, bucket_name: str) -> S3Store:
        """Creates or returns a cached S3Store for a specific bucket."""
        self._check_availability()
        if bucket_name in self._stores:
            return self._stores[bucket_name]

        config_kwargs = {
            "aws_access_key_id": self._config.S3_ACCESS_KEY,
            "aws_secret_access_key": self._config.S3_SECRET_KEY,
            "region": self._config.S3_REGION,
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
        self._check_availability()
        self._active_ops += 1
        try:
            with self._observability.start_s3_span("download", uri):
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
                    self._observability.record_s3_op("download", "success")
                    return local_folder_path

                # Handle single file download
                local_path = join(local_dir_root, basename(object_key))

                async with self._semaphore:
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
                                raise IntegrityError(
                                    f"ETag mismatch for {uri}: expected {expected_etag}, got {actual_etag}"
                                )

                    async with aio_open(local_path, "wb") as f:
                        async for chunk in result.stream():
                            await f.write(chunk)

                logger.info(f"Successfully downloaded file from S3: {uri} -> {local_path}")
                self._observability.record_s3_op("download", "success")
                return local_path

        except Exception as e:
            # Catching generic Exception because obstore might raise different errors.
            self._observability.record_s3_op("download", "error")
            logger.exception(f"Error during download of {uri}: {e}")
            raise
        finally:
            self._active_ops -= 1

    async def _upload_to_s3(self, local_path: str, s3_prefix: str = "", max_retries: int = 3) -> FileMetadata:
        """Uploads a file or a folder to S3 with retries and returns FileMetadata."""
        self._check_availability()
        self._active_ops += 1
        bucket_name = self._config.S3_DEFAULT_BUCKET
        store = self._get_store(bucket_name)

        logger.info(f"Starting upload to S3 from local path: {local_path} (prefix: {s3_prefix})")

        async def _do_upload_with_retry(path: str, key: str) -> Any:
            full_key = join(s3_prefix, key).lstrip("/")
            last_err = None
            for attempt in range(max_retries):
                try:
                    async with self._semaphore:
                        with open(path, "rb") as f:
                            return await obstore_put(store, full_key, f)
                except Exception as e:
                    last_err = e
                    # Check for permanent errors (Auth/Permissions)
                    err_str = str(e).lower()
                    if "403" in err_str or "forbidden" in err_str or "credential" in err_str:
                        logger.error(f"Permanent S3 error during upload (attempt {attempt + 1}): {e}")
                        raise RxonError(f"S3 Access Denied: {e}") from e

                    if attempt < max_retries - 1:
                        wait_delay = 0.5 * (2**attempt)
                        logger.warning(
                            f"S3 upload failed (attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {wait_delay:.1f}s: {e}"
                        )
                        await sleep(wait_delay)
                    else:
                        logger.error(f"S3 upload failed after {max_retries} attempts: {e}")

            if last_err:
                raise last_err
            raise RxonError("S3 upload failed for unknown reason")

        try:
            with self._observability.start_s3_span("upload", local_path):
                # Handle folder upload
                if await isdir(local_path):
                    folder_name = basename(local_path.rstrip("/"))
                    s3_folder_prefix = join(s3_prefix, folder_name)

                    def _get_files_to_upload():
                        from os.path import getsize as std_getsize

                        files_to_upload = []
                        total_size = 0
                        for root, _, files in walk(local_path):
                            for file in files:
                                f_path = join(root, file)
                                rel = relpath(f_path, local_path)
                                total_size += std_getsize(f_path)
                                files_to_upload.append((f_path, rel))
                        return files_to_upload, total_size

                    files_list, total_size = await to_thread(_get_files_to_upload)

                    if files_list:
                        # We pass folder_prefix to do_upload_with_retry
                        await gather(*[_do_upload_with_retry(f, join(folder_name, k)) for f, k in files_list])

                    s3_uri = f"s3://{bucket_name}/{s3_folder_prefix}/"
                    logger.info(
                        f"Successfully uploaded folder to S3: {local_path} -> {s3_uri} ({len(files_list)} files)"
                    )
                    self._observability.record_s3_op("upload_folder", "success")
                    return FileMetadata(uri=s3_uri, size=total_size)

                # Handle single file upload
                filename = basename(local_path)
                file_size = await getsize(local_path)

                put_result = await _do_upload_with_retry(local_path, filename)

                full_s3_key = join(s3_prefix, filename).lstrip("/")
                s3_uri = f"s3://{bucket_name}/{full_s3_key}"
                etag = put_result["e_tag"].strip('"') if put_result["e_tag"] else None
                logger.info(f"Successfully uploaded file to S3: {local_path} -> {s3_uri} (ETag: {etag})")
                self._observability.record_s3_op("upload_file", "success")
                return FileMetadata(uri=s3_uri, size=file_size, etag=etag)

        except Exception as e:
            self._observability.record_s3_op("upload", "error")
            logger.exception(f"Fatal error during S3 upload of {local_path}: {e}")
            raise
        finally:
            self._active_ops -= 1

    async def process_params(
        self, params: dict[str, Any], task_id: str, metadata: dict[str, FileMetadata] | None = None
    ) -> dict[str, Any]:
        """Recursively searches for S3 URIs in params and downloads the files.
        Uses metadata for integrity verification if available.
        """
        # Only check availability if we actually need to process S3 URIs
        if not self._config.S3_ENDPOINT_URL:
            return params

        self._check_availability()

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

    async def process_result(
        self, result: dict[str, Any], s3_prefix: str = ""
    ) -> tuple[dict[str, Any], dict[str, FileMetadata]]:
        """Recursively searches for local file paths in the result and uploads them to S3.
        Returns a tuple of (updated_result, metadata_map).
        """
        if not self._config.S3_ENDPOINT_URL:
            return result, {}

        self._check_availability()

        metadata_map = {}

        async def _process(item: Any, key_path: str = "") -> Any:
            if isinstance(item, str) and item.startswith(self._config.TASK_FILES_DIR):
                if await exists(item):
                    meta = await self._upload_to_s3(item, s3_prefix=s3_prefix)
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
