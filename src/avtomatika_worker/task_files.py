# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from contextlib import asynccontextmanager
from json import dumps, loads
from os.path import dirname, join
from typing import TYPE_CHECKING, Any, AsyncGenerator, cast

from aiofiles import open as aiopen
from aiofiles.os import listdir, makedirs
from aiofiles.ospath import exists as aio_exists

if TYPE_CHECKING:
    from rxon.models import FileMetadata

    from .s3 import S3Manager


class TaskFiles:
    """
    A helper class for managing task-specific files.
    Provides asynchronous lazy directory creation and high-level file operations
    within an isolated workspace for each task.
    """

    def __init__(self, task_dir: str, job_id: str, task_id: str, s3_manager: S3Manager | None = None):
        """
        Initializes TaskFiles with specific directory and identifiers.
        """
        self._task_dir = task_dir
        self._job_id = job_id
        self._task_id = task_id
        self._s3_manager = s3_manager

    async def get_root(self) -> str:
        """
        Asynchronously returns the root directory for the task.
        Creates the directory on disk if it doesn't exist.
        """
        await makedirs(self._task_dir, exist_ok=True)
        return self._task_dir

    async def path_to(self, filename: str) -> str:
        """
        Asynchronously returns an absolute path for a file within the task directory.
        Guarantees that the task root directory exists.
        """
        root = await self.get_root()
        return join(root, filename)

    def get_root_sync(self) -> str:
        """
        Synchronously returns the root directory for the task.
        Creates the directory on disk if it doesn't exist.
        """
        from os import makedirs as std_makedirs

        std_makedirs(self._task_dir, exist_ok=True)
        return self._task_dir

    def path_to_sync(self, filename: str) -> str:
        """
        Synchronously returns an absolute path for a file within the task directory.
        Guarantees that the task root directory exists.
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
        # Ensure directory for the file itself exists if filename contains subdirectories
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
        content = dumps(data, indent=2)
        await self.write(filename, content)
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
        return await self._s3_manager._upload_to_s3(path, s3_prefix=self._job_id)

    async def upload_dir(self, dirname: str = "") -> FileMetadata:
        """Uploads the entire task directory or a subdirectory to S3."""
        if not self._s3_manager:
            raise RuntimeError("S3Manager not configured for this TaskFiles instance.")
        path = join(self._task_dir, dirname) if dirname else self._task_dir
        return await self._s3_manager._upload_to_s3(path, s3_prefix=self._job_id)

    async def download_file(self, uri: str, filename: str, verify_meta: FileMetadata | None = None) -> str:
        """Downloads a file from S3 to the task directory with optional integrity check."""
        if not self._s3_manager:
            raise RuntimeError("S3Manager not configured for this TaskFiles instance.")
        return await self._s3_manager._process_s3_uri(uri, self._task_id, verify_meta=verify_meta)

    async def list(self) -> list[str]:
        """
        Asynchronously lists all file and directory names within the task root.
        """
        root = await self.get_root()
        return await listdir(root)

    async def exists(self, filename: str) -> bool:
        """
        Asynchronously checks if a specific file or directory exists in the task root.
        """
        path = join(self._task_dir, filename)
        return await aio_exists(path)

    def __repr__(self):
        return f"<TaskFiles root='{self._task_dir}'>"
