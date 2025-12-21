from contextlib import asynccontextmanager
from os.path import dirname, join
from typing import AsyncGenerator

from aiofiles import open as aiopen
from aiofiles.os import listdir, makedirs
from aiofiles.ospath import exists as aio_exists


class TaskFiles:
    """
    A helper class for managing task-specific files.
    Provides asynchronous lazy directory creation and high-level file operations
    within an isolated workspace for each task.
    """

    def __init__(self, task_dir: str):
        """
        Initializes TaskFiles with a specific task directory.
        The directory is not created until needed.
        """
        self._task_dir = task_dir

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

        async with aiopen(path, mode) as f:
            yield f

    async def read(self, filename: str, mode: str = "r") -> str | bytes:
        """
        Asynchronously reads the entire content of a file.

        Args:
            filename: Name of the file to read.
            mode: Mode to open the file in (defaults to 'r').
        """
        async with self.open(filename, mode) as f:
            return await f.read()

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
