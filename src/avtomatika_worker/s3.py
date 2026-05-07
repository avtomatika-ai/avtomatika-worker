# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from asyncio import Semaphore, sleep, to_thread
from logging import getLogger
from os.path import basename, dirname, join, relpath
from shutil import rmtree
from typing import Any, cast

from aiofiles import open as aio_open
from aiofiles.os import makedirs
from aiofiles.ospath import exists, getsize
from rxon.blob import BlobProvider, parse_uri
from rxon.models import FileMetadata

from .config import WorkerConfig
from .observability import ObservabilityManager

logger = getLogger(__name__)

try:
    from obstore import delete as obstore_delete
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
    obstore_delete = None

# Limit concurrent S3 operations to avoid "Too many open files"
MAX_S3_CONCURRENCY = 50


class S3BlobProvider(BlobProvider):
    """
    S3 implementation of BlobProvider using obstore.
    Provides memory-efficient streaming for large files.
    """

    def __init__(self, config: WorkerConfig, semaphore: Semaphore):
        self._config = config
        self._stores: dict[str, S3Store] = {}
        self._semaphore = semaphore

    def _check_availability(self) -> None:
        if not _HAS_S3:
            raise RuntimeError(
                "S3 support is not installed. Please install 'avtomatika-worker[s3]' to use S3 features."
            )

    def _get_store(self, bucket_name: str) -> S3Store:
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

        config_kwargs = {k: v for k, v in config_kwargs.items() if v is not None}

        try:
            store = S3Store(bucket_name, **config_kwargs)
            self._stores[bucket_name] = store
            return store
        except Exception as e:
            logger.error(f"Failed to create S3Store for bucket {bucket_name}: {e}")
            raise

    async def upload(self, local_path: str, uri: str) -> str:
        self._check_availability()
        bucket, key, _ = parse_uri(uri, self._config.S3_DEFAULT_BUCKET)
        store = self._get_store(bucket)
        async with self._semaphore:
            with open(local_path, "rb") as f:
                res = await obstore_put(store, key, f)
                return res["e_tag"].strip('"') if res and res.get("e_tag") else ""

    async def download(self, uri: str, local_path: str) -> bool:
        self._check_availability()
        bucket, key, _ = parse_uri(uri, self._config.S3_DEFAULT_BUCKET)
        store = self._get_store(bucket)
        async with self._semaphore:
            result = await obstore_get(store, key)
            async with aio_open(local_path, "wb") as f:
                async for chunk in result.stream():
                    await f.write(chunk)
        return True

    async def get_metadata(self, uri: str) -> dict[str, Any] | None:
        self._check_availability()
        bucket, key, _ = parse_uri(uri, self._config.S3_DEFAULT_BUCKET)
        store = self._get_store(bucket)
        try:
            async with self._semaphore:
                result = await obstore_get(store, key)
                return {"size": result.meta.size, "etag": result.meta.e_tag.strip('"') if result.meta.e_tag else None}
        except Exception:
            return None

    async def delete(self, uri: str) -> bool:
        self._check_availability()
        bucket, key, _ = parse_uri(uri, self._config.S3_DEFAULT_BUCKET)
        store = self._get_store(bucket)
        async with self._semaphore:
            await obstore_delete(store, key)
        return True

    async def delete_dir(self, uri: str) -> bool:
        self._check_availability()
        bucket, prefix, _ = parse_uri(uri, self._config.S3_DEFAULT_BUCKET)
        if not prefix.endswith("/"):
            prefix += "/"
        store = self._get_store(bucket)
        async with self._semaphore:
            objects = await obstore_list(store, prefix=prefix).collect()
            for obj in objects:
                await obstore_delete(store, obj.path)
        return True


class S3Manager:
    """High-level manager for S3 payload offloading."""

    def __init__(self, config: WorkerConfig, observability: ObservabilityManager | None = None):
        self._config = config
        self._semaphore = Semaphore(MAX_S3_CONCURRENCY)
        self._provider = S3BlobProvider(config, self._semaphore)
        self._active_ops = 0
        self._observability = observability or ObservabilityManager(enabled=False)

    async def wait_all_done(self, timeout: float = 30.0) -> None:
        if self._active_ops == 0:
            return
        sleep_delay = 0.1
        total_slept = 0.0
        while self._active_ops > 0 and total_slept < timeout:
            await sleep(sleep_delay)
            total_slept += sleep_delay

    async def cleanup(self, task_id: str) -> None:
        task_dir = join(self._config.TASK_FILES_DIR, task_id)
        if await exists(task_dir):
            await to_thread(lambda: rmtree(task_dir, ignore_errors=True))

    async def process_params(
        self, params: dict[str, Any], task_id: str, metadata: dict[str, FileMetadata] | None = None
    ) -> dict[str, Any]:
        if not self._config.S3_ENDPOINT_URL:
            return params

        async def _process(item: Any, key_path: str = "") -> Any:
            if isinstance(item, str) and item.startswith("s3://"):
                self._active_ops += 1
                try:
                    with self._observability.start_s3_span("download", item):
                        bucket, key, is_dir = parse_uri(item)
                        local_root = join(self._config.TASK_FILES_DIR, task_id)
                        await makedirs(local_root, exist_ok=True)

                        if is_dir:
                            local_path = join(local_root, key.rstrip("/").split("/")[-1])
                            return await self._download_folder(item, local_path)

                        local_path = join(local_root, basename(key))
                        await self._provider.download(item, local_path)
                        self._observability.record_s3_op("download", "success")
                        return local_path
                finally:
                    self._active_ops -= 1
            if isinstance(item, dict):
                return {k: await _process(v, f"{key_path}.{k}" if key_path else k) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(v, f"{key_path}[{i}]") for i, v in enumerate(item)]
            return item

        return cast(dict[str, Any], await _process(params))

    async def _download_folder(self, uri: str, local_path: str) -> str:
        bucket, prefix, _ = parse_uri(uri)
        store = self._provider._get_store(bucket)
        objects = await obstore_list(store, prefix=prefix).collect()
        for obj in objects:
            if obj.path.endswith("/"):
                continue
            rel = relpath(obj.path, prefix)
            target = join(local_path, rel)
            await makedirs(dirname(target), exist_ok=True)
            await self._provider.download(f"s3://{bucket}/{obj.path}", target)
        return local_path

    async def process_result(
        self, result: dict[str, Any], s3_prefix: str = ""
    ) -> tuple[dict[str, Any], dict[str, FileMetadata]]:
        if not self._config.S3_ENDPOINT_URL:
            return result, {}

        metadata_map = {}

        async def _process(item: Any, key_path: str = "") -> Any:
            if isinstance(item, str) and item.startswith(self._config.TASK_FILES_DIR):
                if await exists(item):
                    self._active_ops += 1
                    try:
                        with self._observability.start_s3_span("upload", item):
                            bucket = self._config.S3_DEFAULT_BUCKET
                            filename = basename(item)
                            target_uri = f"s3://{bucket}/{join(s3_prefix, filename).lstrip('/')}"

                            etag = await self._provider.upload(item, target_uri)
                            size = await getsize(item)

                            meta = FileMetadata(uri=target_uri, size=size, etag=etag)
                            metadata_map[key_path] = meta
                            self._observability.record_s3_op("upload", "success")
                            return target_uri
                    finally:
                        self._active_ops -= 1
                return item
            if isinstance(item, dict):
                return {k: await _process(v, f"{key_path}.{k}" if key_path else k) for k, v in item.items()}
            if isinstance(item, list):
                return [await _process(v, f"{key_path}[{i}]") for i, v in enumerate(item)]
            return item

        updated_result = cast(dict[str, Any], await _process(result))
        return updated_result, metadata_map
