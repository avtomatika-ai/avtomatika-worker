# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import pytest

from avtomatika_worker.worker import Worker


@pytest.fixture
def worker():
    return Worker(worker_type="test")


def test_parse_retry_after_seconds(worker):
    assert worker._parse_retry_after("60") == 60.0
    assert worker._parse_retry_after(120) == 120.0


def test_parse_retry_after_http_date(worker):
    # Use future date in UTC
    future_date = datetime.now(UTC) + timedelta(seconds=100)
    # Format according to RFC 7231 (HTTP-date), mandatory usegmt=True for "GMT" suffix
    date_str = format_datetime(future_date, usegmt=True)

    parsed = worker._parse_retry_after(date_str)
    # Allow delta due to execution time
    assert 90 < parsed <= 101


def test_parse_retry_after_past_date(worker):
    past_date = datetime.now(UTC) - timedelta(seconds=100)
    date_str = format_datetime(past_date, usegmt=True)
    assert worker._parse_retry_after(date_str) == 0.0


def test_parse_retry_after_invalid(worker):
    assert worker._parse_retry_after("invalid") == 0.0
    assert worker._parse_retry_after(None) == 0.0
    assert worker._parse_retry_after([]) == 0.0


def test_parse_retry_after_naive_date(worker, mocker):
    # Test case where date might be naive
    naive_date = datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=60)
    # email.utils format_datetime with usegmt=True requires UTC aware object
    aware_date = naive_date.replace(tzinfo=UTC)
    date_str = format_datetime(aware_date, usegmt=True)
    parsed = worker._parse_retry_after(date_str)
    assert parsed > 0
