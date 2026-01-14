import aiohttp
import pytest

from avtomatika_worker.client import OrchestratorClient


@pytest.mark.asyncio
async def test_send_result_success(mocker):
    """Tests that send_result sends a successful result to the orchestrator."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    post_cm = mocker.AsyncMock()
    post_cm.__aenter__.return_value.status = 200
    session.post.return_value = post_cm

    client = OrchestratorClient(session, "http://test-orchestrator", "worker-1", "token")
    payload = {
        "job_id": "job-1",
        "task_id": "task-1",
        "worker_id": "worker-1",
        "result": {"status": "success"},
    }
    result = await client.send_result(payload, max_retries=1, initial_delay=0.1)

    assert result is True
    session.post.assert_called_once()
    sent_payload = session.post.call_args.kwargs["json"]
    assert sent_payload["result"]["status"] == "success"


@pytest.mark.asyncio
async def test_send_result_failure_retry(mocker):
    """Tests that send_result retries on failure."""
    session = mocker.MagicMock(spec=aiohttp.ClientSession)
    session.closed = False

    # side_effect function to produce a new context manager mock on each call
    def mock_failed_post(*args, **kwargs):
        cm = mocker.AsyncMock()
        cm.__aenter__.return_value.status = 500
        return cm

    session.post.side_effect = mock_failed_post

    client = OrchestratorClient(session, "http://test-orchestrator", "worker-1", "token")

    payload = {"result": {"status": "success"}}
    result = await client.send_result(payload, max_retries=2, initial_delay=0.01)

    assert result is False
    assert session.post.call_count == 2
