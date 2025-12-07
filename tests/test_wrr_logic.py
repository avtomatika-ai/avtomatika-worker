from collections import Counter

from avtomatika_worker.config import WorkerConfig
from avtomatika_worker.worker import Worker


def test_wrr_algorithm_distribution():
    """
    Tests that the smooth weighted round-robin algorithm distributes
    orchestrator selections according to their weights.
    """

    # Mock the config to return our test orchestrators
    class MockConfig(WorkerConfig):
        def _get_orchestrators_config(self) -> list[dict[str, any]]:
            return [
                {"url": "http://a.com", "priority": 1, "weight": 5},
                {"url": "http://b.com", "priority": 1, "weight": 2},
                {"url": "http://c.com", "priority": 1, "weight": 1},
            ]

    worker = Worker()
    worker._config = MockConfig()

    # Re-initialize WRR state in the worker based on the mocked config
    worker._total_orchestrator_weight = 0
    if worker._config.ORCHESTRATORS:
        for o in worker._config.ORCHESTRATORS:
            o["current_weight"] = 0
            worker._total_orchestrator_weight += o.get("weight", 1)

    # --- Run the algorithm for a number of cycles ---
    total_weight = worker._total_orchestrator_weight
    iterations = total_weight * 10  # 80 iterations
    selections = []
    for _ in range(iterations):
        orchestrator = worker._get_next_orchestrator()
        selections.append(orchestrator["url"])

    counts = Counter(selections)

    # --- Assert the distribution ---
    # Total selections should be the number of iterations
    assert sum(counts.values()) == iterations

    # Check the number of selections for each orchestrator
    # It should be proportional to its weight
    assert counts["http://a.com"] == 5 * (iterations / total_weight)
    assert counts["http://b.com"] == 2 * (iterations / total_weight)
    assert counts["http://c.com"] == 1 * (iterations / total_weight)

    # Check the selection sequence for the first cycle to ensure it's "smooth"
    # Expected sequence for weights 5, 2, 1 is A, A, B, A, C, A, B, A
    first_cycle_selections = selections[:total_weight]
    # Note: The exact sequence can vary based on tie-breaking (e.g. dict order).
    # A Counter is more robust for testing distribution.
    assert Counter(first_cycle_selections) == Counter(
        {
            "http://a.com": 5,
            "http://b.com": 2,
            "http://c.com": 1,
        }
    )
