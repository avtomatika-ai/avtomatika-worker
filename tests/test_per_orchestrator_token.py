import pytest
from rxon.testing import MockTransport

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


@pytest.mark.asyncio
async def test_per_orchestrator_token_usage(mocker):
    """
    Tests that the worker uses the correct token for each orchestrator
    by checking the transport configuration.
    """

    # 1. Setup mock configuration object
    mock_config = WorkerConfig()
    mock_config.ORCHESTRATORS = [
        {"url": "http://orch1.com", "token": "token-for-orch1", "weight": 1},
        {"url": "http://orch2.com", "weight": 1},
    ]
    mock_config.WORKER_TOKEN = "global-fallback-token"

    # Mock create_transport to return MockTransport with captured params
    def mock_create_transport(url, token, **kwargs):
        # We can store url in a custom attribute for verification
        client = MockTransport(token=token)
        client.url = url  # type: ignore
        return client

    mocker.patch("avtomatika_worker.worker.create_transport", side_effect=mock_create_transport)

    # 2. Instantiate worker
    worker = Worker(config=mock_config)
    worker._init_clients()

    assert len(worker._clients) == 2

    # Check tokens in transports
    client1 = next(c for o, c in worker._clients if o["url"] == "http://orch1.com")
    client2 = next(c for o, c in worker._clients if o["url"] == "http://orch2.com")

    assert client1.token == "token-for-orch1"
    assert client2.token == "global-fallback-token"
    assert client1.url == "http://orch1.com"
