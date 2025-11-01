import asyncio

import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def cancel_all_tasks():
    yield
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
