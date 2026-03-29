# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger
from typing import Any, cast

logger = getLogger(__name__)
try:
    from opentelemetry import metrics, trace
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.metrics import Counter, Histogram, Meter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes
    from opentelemetry.trace import Status, StatusCode
    from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

    _HAS_OTEL = True
except ImportError:
    metrics = trace = None
    Meter = Counter = Histogram = Status = StatusCode = Resource = Any
    generate_latest = None
    CONTENT_TYPE_LATEST = "text/plain"
    ResourceAttributes = Any
    _HAS_OTEL = False


class ObservabilityManager:
    """Manages OpenTelemetry Metrics and Traces for the worker.
    Provides a No-op implementation if OpenTelemetry is not installed.
    """

    def __init__(
        self,
        enabled: bool = True,
        service_name: str = "avtomatika-worker",
        worker_id: str | None = None,
        worker_type: str | None = None,
        version: str | None = None,
    ):
        self.enabled = enabled and _HAS_OTEL
        self.tracer = None
        self.meter = None
        self.registry: Any | None = None

        if not self.enabled:
            if enabled and not _HAS_OTEL:
                logger.warning(
                    "Observability is enabled in config, but 'opentelemetry' is not installed. "
                    "Run 'pip install avtomatika-worker[metrics]'."
                )
            return

        # Setup Resource Attributes
        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: service_name,
                ResourceAttributes.SERVICE_INSTANCE_ID: worker_id or "unknown",
                "worker.type": worker_type or "unknown",
                "worker.version": version or "unknown",
            }
        )

        # Initialize OTel Tracer and Meter
        from opentelemetry.sdk.trace import TracerProvider

        self._tracer_provider = TracerProvider(resource=resource)
        trace.set_tracer_provider(self._tracer_provider)
        self.tracer = trace.get_tracer(service_name)

        # Setup Prometheus reader for metrics
        self._prom_reader = PrometheusMetricReader()
        self._meter_provider = MeterProvider(resource=resource, metric_readers=[self._prom_reader])
        metrics.set_meter_provider(self._meter_provider)
        self.meter = metrics.get_meter(service_name)

        # Metrics Definition
        self.tasks_total = self.meter.create_counter(
            "worker.tasks.total", unit="1", description="Total number of tasks processed"
        )
        self.tasks_duration = self.meter.create_histogram(
            "worker.tasks.duration", unit="s", description="Task execution duration"
        )
        self.s3_ops_total = self.meter.create_counter(
            "worker.s3.operations.total", unit="1", description="Total number of S3 operations"
        )

    @contextmanager
    def start_task_span(self, task_type: str, task_id: str, job_id: str) -> Generator[Any | None, None, None]:
        """Starts a span for a task execution."""
        if not self.enabled or not self.tracer:
            yield None
            return

        with self.tracer.start_as_current_span(
            f"task.{task_type}",
            attributes={
                "task.id": task_id,
                "job.id": job_id,
                "task.type": task_type,
            },
        ) as span:
            yield span

    @contextmanager
    def start_s3_span(self, op: str, uri: str) -> Generator[Any | None, None, None]:
        """Starts a span for an S3 operation."""
        if not self.enabled or not self.tracer:
            yield None
            return

        with self.tracer.start_as_current_span(
            f"s3.{op}",
            attributes={
                "s3.uri": uri,
                "s3.operation": op,
            },
        ) as span:
            yield span

    def record_task_finished(self, task_type: str, status: str, duration: float) -> None:
        """Updates metrics when a task is finished."""
        if not self.enabled:
            return
        self.tasks_total.add(1, {"task.type": task_type, "status": status})
        self.tasks_duration.record(duration, {"task.type": task_type})

    def record_s3_op(self, op: str, status: str) -> None:
        """Updates S3 metrics."""
        if not self.enabled:
            return
        self.s3_ops_total.add(1, {"s3.operation": op, "status": status})

    def set_span_status_error(self, span: Any, message: str) -> None:
        """Sets the span status to ERROR with a message."""
        if not self.enabled or not span or not Status:
            return
        span.set_status(Status(StatusCode.ERROR, message))

    def generate_latest(self) -> bytes:
        """Generates the latest metrics in Prometheus format."""
        if not self.enabled or not generate_latest:
            return b""
        return cast(bytes, generate_latest(self.registry))

    @property
    def content_type(self) -> str:
        """Returns the correct Content-Type for the metrics endpoint."""
        return cast(str, CONTENT_TYPE_LATEST)

    @property
    def has_otel(self) -> bool:
        return self.enabled
