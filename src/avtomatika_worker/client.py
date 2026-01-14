from asyncio import sleep
from logging import getLogger
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout, ClientWebSocketResponse

from .constants import AUTH_HEADER_WORKER

logger = getLogger(__name__)


class OrchestratorClient:
    """
    Dedicated client for communicating with a single Avtomatika Orchestrator instance.
    Handles HTTP requests, retries, and authentication.
    """

    def __init__(self, session: ClientSession, base_url: str, worker_id: str, token: str):
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.worker_id = worker_id
        self.token = token
        self._headers = {AUTH_HEADER_WORKER: self.token}

    async def register(self, payload: dict[str, Any]) -> bool:
        """Registers the worker with the orchestrator."""
        url = f"{self.base_url}/_worker/workers/register"
        try:
            async with self.session.post(url, json=payload, headers=self._headers) as resp:
                if resp.status >= 400:
                    logger.error(f"Error registering with {self.base_url}: {resp.status}")
                    return False
                return True
        except ClientError as e:
            logger.error(f"Error registering with orchestrator {self.base_url}: {e}")
            return False

    async def poll_task(self, timeout: float) -> dict[str, Any] | None:
        """Polls for the next available task."""
        url = f"{self.base_url}/_worker/workers/{self.worker_id}/tasks/next"
        client_timeout = ClientTimeout(total=timeout + 5)
        try:
            async with self.session.get(url, headers=self._headers, timeout=client_timeout) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status != 204:
                    logger.warning(f"Unexpected status from {self.base_url} during poll: {resp.status}")
        except ClientError as e:
            logger.error(f"Error polling for tasks from {self.base_url}: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error polling from {self.base_url}: {e}")
        return None

    async def send_heartbeat(self, payload: dict[str, Any]) -> bool:
        """Sends a heartbeat message to update worker state."""
        url = f"{self.base_url}/_worker/workers/{self.worker_id}"
        try:
            async with self.session.patch(url, json=payload, headers=self._headers) as resp:
                if resp.status >= 400:
                    logger.warning(f"Heartbeat to {self.base_url} failed with status: {resp.status}")
                    return False
                return True
        except ClientError as e:
            logger.error(f"Error sending heartbeat to orchestrator {self.base_url}: {e}")
            return False

    async def send_result(self, payload: dict[str, Any], max_retries: int, initial_delay: float) -> bool:
        """Sends task result with retries and exponential backoff."""
        url = f"{self.base_url}/_worker/tasks/result"
        delay = initial_delay
        for i in range(max_retries):
            try:
                async with self.session.post(url, json=payload, headers=self._headers) as resp:
                    if resp.status == 200:
                        return True
                    logger.error(f"Error sending result to {self.base_url}: {resp.status}")
            except ClientError as e:
                logger.error(f"Error sending result to {self.base_url}: {e}")

            if i < max_retries - 1:
                await sleep(delay * (2**i))
        return False

    async def connect_websocket(self) -> ClientWebSocketResponse | None:
        """Establishes a WebSocket connection for real-time commands."""
        ws_url = self.base_url.replace("http", "ws", 1) + "/_worker/ws"
        try:
            ws = await self.session.ws_connect(ws_url, headers=self._headers)
            logger.info(f"WebSocket connection established to {ws_url}")
            return ws
        except Exception as e:
            logger.warning(f"WebSocket connection to {ws_url} failed: {e}")
            return None
