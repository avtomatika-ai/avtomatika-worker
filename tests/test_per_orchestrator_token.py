import re

import pytest
from aiohttp import ClientSession
from aioresponses import CallbackResult, aioresponses

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_per_orchestrator_token_usage():
    """
    Tests that the worker uses the correct token for each orchestrator
    by passing a custom config object to the Worker.
    """

    # 1. Setup mock configuration object
    mock_config = WorkerConfig()
    mock_config.ORCHESTRATORS = [
        {"url": "http://orch1.com", "token": "token-for-orch1", "weight": 1},
        {"url": "http://orch2.com", "weight": 1},
    ]
    mock_config.WORKER_TOKEN = "global-fallback-token"

    captured_headers = []

    def header_capturing_callback(url, **kwargs):
        captured_headers.append(kwargs.get("headers"))
        return CallbackResult(status=200)

    # 2. Run the test logic inside aioresponses
    with aioresponses() as m:
        # Mock endpoints for registration
        m.post("http://orch1.com/_worker/workers/register", callback=header_capturing_callback)
        m.post("http://orch2.com/_worker/workers/register", callback=header_capturing_callback)

        # Use regex for heartbeats since worker_id is dynamic
        m.patch(re.compile(r"http://orch1.com/.*"), callback=header_capturing_callback)
        m.patch(re.compile(r"http://orch2.com/.*"), callback=header_capturing_callback)

        async with ClientSession() as session:
            # Instantiate worker with the custom config and a session
            # The session is intercepted by aioresponses
            worker = Worker(config=mock_config, http_session=session)

            # --- Test registration ---
            captured_headers.clear()
            await worker._register_with_all_orchestrators()

            assert len(captured_headers) == 2
            # The order can vary, so we check for both possibilities
            tokens = {h["X-Worker-Token"] for h in captured_headers}
            assert tokens == {"token-for-orch1", "global-fallback-token"}

            # --- Test heartbeats ---
            captured_headers.clear()
            await worker._send_heartbeats_to_all()

            assert len(captured_headers) == 2
            tokens = {h["X-Worker-Token"] for h in captured_headers}
            assert tokens == {"token-for-orch1", "global-fallback-token"}
