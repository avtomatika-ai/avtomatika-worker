# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from os.path import dirname, join
from typing import TYPE_CHECKING, Any, cast

from aiofiles import open as aiopen  # type: ignore
from aiofiles.os import listdir, makedirs  # type: ignore
from aiofiles.ospath import exists as aio_exists  # type: ignore
from orjson import OPT_INDENT_2, dumps, loads
from rxon.models import FileMetadata

if TYPE_CHECKING:
    from .observability import ObservabilityManager
    from .s3 import S3Manager


class TaskFiles:
    """
    A helper class for managing task-specific files.
    Provides asynchronous lazy directory creation and high-level file operations
    within an isolated workspace for each task.
    """

    def __init__(
        self,
        task_dir: str,
        job_id: str,
        task_id: str,
        s3_manager: S3Manager | None = None,
        observability: ObservabilityManager | None = None,
    ):
        self._task_dir = task_dir
        self._job_id = job_id
        self._task_id = task_id
        self._s3_manager = s3_manager
        self._observability = observability

    async def get_root(self) -> str:
        """
        Asynchronously returns the root directory for the task.
        """
        await makedirs(self._task_dir, exist_ok=True)
        return self._task_dir

    async def path_to(self, filename: str) -> str:
        """
        Asynchronously returns an absolute path for a file within the task directory.
        """
        root = await self.get_root()
        return join(root, filename)

    def get_root_sync(self) -> str:
        """
        Synchronously returns the root directory for the task.
        """
        from os import makedirs as std_makedirs

        std_makedirs(self._task_dir, exist_ok=True)
        return self._task_dir

    def path_to_sync(self, filename: str) -> str:
        """
        Synchronously returns an absolute path for a file within the task directory.
        """
        root = self.get_root_sync()
        return join(root, filename)

    @asynccontextmanager
    async def open(self, filename: str, mode: str = "r") -> AsyncGenerator:
        """
        An asynchronous context manager to open a file within the task directory.
        Automatically creates the task root and any necessary subdirectories.

        Args:
            filename: Name or relative path of the file.
            mode: File opening mode (e.g., 'r', 'w', 'a', 'rb', 'wb').
        """
        path = await self.path_to(filename)
        file_dir = dirname(path)
        if file_dir != self._task_dir:
            await makedirs(file_dir, exist_ok=True)

        async with cast(Any, aiopen)(path, mode) as f:
            yield f

    async def read(self, filename: str, mode: str = "r") -> str:
        """
        Asynchronously reads the entire content of a file.

        Args:
            filename: Name of the file to read.
            mode: Mode to open the file in (defaults to 'r').
        """
        async with self.open(filename, mode) as f:
            content = await f.read()
            return cast(str, content)

    async def write(self, filename: str, data: str | bytes, mode: str = "w") -> None:
        """
        Asynchronously writes data to a file. Creates or overwrites the file by default.

        Args:
            filename: Name of the file to write.
            data: Content to write (string or bytes).
            mode: Mode to open the file in (defaults to 'w').
        """
        async with self.open(filename, mode) as f:
            await f.write(data)

    async def write_json(self, filename: str, data: Any) -> FileMetadata | None:
        """Writes data as JSON and optionally uploads to S3 if manager is available."""
        content = dumps(data, option=OPT_INDENT_2)
        await self.write(filename, content, mode="wb")
        if self._s3_manager:
            return await self.upload_file(filename)
        return None

    async def read_json(self, filename: str) -> Any:
        """Reads a file and parses it as JSON."""
        content = await self.read(filename)
        return loads(content)

    async def upload_file(self, filename: str) -> FileMetadata:
        """Uploads a specific file to S3 and returns its metadata."""
        if not self._s3_manager:
            raise RuntimeError("S3Manager not configured for this TaskFiles instance.")

        path = await self.path_to(filename)
        from os.path import basename

        from aiofiles.ospath import getsize

        bucket = self._s3_manager._config.S3_DEFAULT_BUCKET
        target_uri = f"s3://{bucket}/{join(self._job_id, basename(path)).lstrip('/')}"

        if self._observability:
            with self._observability.start_s3_span("upload", target_uri):
                etag = await self._s3_manager._provider.upload(path, target_uri)
                size = await getsize(path)
                return FileMetadata(uri=target_uri, size=size, etag=etag)

        etag = await self._s3_manager._provider.upload(path, target_uri)
        size = await getsize(path)
        return FileMetadata(uri=target_uri, size=size, etag=etag)

    async def upload_dir(self, dirname: str = "") -> FileMetadata:
        """Uploads a directory to S3."""
        if not self._s3_manager:
            raise RuntimeError("S3Manager not configured for this TaskFiles instance.")

        path = join(self._task_dir, dirname) if dirname else self._task_dir

        # We use a fixed key 'root' to avoid path-based KeyError
        _, metadata = await self._s3_manager.process_result({"root": path}, s3_prefix=self._job_id)
        return metadata["root"]

    async def download_file(self, uri: str, filename: str, verify_meta: FileMetadata | None = None) -> str:
        """Downloads a file from S3 to the task directory."""
        if not self._s3_manager:
            raise RuntimeError("S3Manager not configured for this TaskFiles instance.")

        local_path = await self.path_to(filename)

        if self._observability:
            with self._observability.start_s3_span("download", uri):
                await self._s3_manager._provider.download(uri, local_path)
        else:
            await self._s3_manager._provider.download(uri, local_path)

        if verify_meta:
            from aiofiles.ospath import getsize

            actual_size = await getsize(local_path)
            if verify_meta.size is not None and actual_size != verify_meta.size:
                raise ValueError(f"Size mismatch for {uri}")

        return local_path

    async def list(self) -> list[str]:
        """
        Asynchronously lists all file and directory names within the task root.
        """
        root = await self.get_root()
        return cast(list[str], await listdir(root))

    async def exists(self, filename: str) -> bool:
        """
        Asynchronously checks if a specific file or directory exists in the task root.
        """
        path = join(self._task_dir, filename)
        return cast(bool, await aio_exists(path))

    def __repr__(self):
        return f"<TaskFiles root='{self._task_dir}'>"
