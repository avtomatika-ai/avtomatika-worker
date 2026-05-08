# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from avtomatika_worker.worker import Worker


@pytest.fixture
def worker():
    return Worker(worker_type="test")


def test_backoff_progression(worker):
    # Normal progression
    val = worker._calculate_backoff(current=0, initial=1.0, max_delay=60.0)
    assert val == 1.0

    val = worker._calculate_backoff(current=1.0, initial=1.0, max_delay=60.0)
    assert val == 2.0

    val = worker._calculate_backoff(current=30.0, initial=1.0, max_delay=60.0)
    assert val == 60.0


def test_backoff_rate_limit_floor(worker):
    # Rate limit should always be >= 30.0 even if progression is smaller
    val = worker._calculate_backoff(current=0, initial=1.0, max_delay=60.0, is_rate_limit=True)
    assert val == 30.0

    val = worker._calculate_backoff(current=1.0, initial=1.0, max_delay=60.0, is_rate_limit=True)
    assert val == 30.0

    # But it should still follow progression if it exceeds 30
    val = worker._calculate_backoff(current=30.0, initial=1.0, max_delay=120.0, is_rate_limit=True)
    assert val == 60.0


def test_backoff_max_limit(worker):
    val = worker._calculate_backoff(current=100.0, initial=1.0, max_delay=60.0)
    assert val == 60.0
