# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Dmitrii Gagarin aka madgagarin

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from logging import getLogger
from os import getenv
from typing import Any

logger = getLogger(__name__)
try:
    from opentelemetry import metrics, propagate, trace
    from opentelemetry.metrics import Counter, Histogram, Meter
    from opentelemetry.propagate import set_global_textmap
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.semconv.resource import ResourceAttributes
    from opentelemetry.trace import Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

    # Set global propagator to W3C TraceContext
    set_global_textmap(TraceContextTextMapPropagator())

    try:
        from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        try:
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        except ImportError:
            OTLPSpanExporter = OTLPMetricExporter = None
    except ImportError:
        TracerProvider = BatchSpanProcessor = ConsoleSpanExporter = None
        OTLPSpanExporter = OTLPMetricExporter = None
        ConsoleMetricExporter = PeriodicExportingMetricReader = None

    _HAS_OTEL = True
except ImportError:
    metrics = trace = propagate = None
    Meter = Counter = Histogram = Status = StatusCode = Resource = Any
    ResourceAttributes = Any
    TracerProvider = BatchSpanProcessor = ConsoleSpanExporter = None
    OTLPSpanExporter = OTLPMetricExporter = None
    ConsoleMetricExporter = PeriodicExportingMetricReader = None
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

        if not self.enabled:
            if enabled and not _HAS_OTEL:
                logger.warning(
                    "Observability is enabled in config, but 'opentelemetry' is not installed. "
                    "Run 'pip install avtomatika-worker[metrics]'."
                )
            return

        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: service_name,
                ResourceAttributes.SERVICE_INSTANCE_ID: worker_id or "unknown",
                "worker.type": worker_type or "unknown",
                "worker.version": version or "unknown",
            }
        )

        otlp_endpoint = getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

        if TracerProvider:
            self._tracer_provider = TracerProvider(resource=resource)

            if otlp_endpoint and OTLPSpanExporter:
                logger.info(f"OTLP Trace exporter enabled for worker, sending to {otlp_endpoint}")
                trace_processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True))
            else:
                logger.info("Using ConsoleSpanExporter for worker tracing.")
                trace_processor = BatchSpanProcessor(ConsoleSpanExporter())

            self._tracer_provider.add_span_processor(trace_processor)
            trace.set_tracer_provider(self._tracer_provider)
            self.tracer = trace.get_tracer(service_name)

        if PeriodicExportingMetricReader:
            if otlp_endpoint and OTLPMetricExporter:
                logger.info(f"OTLP Metric exporter enabled for worker, sending to {otlp_endpoint}")
                metric_exporter = OTLPMetricExporter(endpoint=otlp_endpoint, insecure=True)
            else:
                logger.info("Using ConsoleMetricExporter for worker metrics.")
                metric_exporter = ConsoleMetricExporter()

            self._metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=60000)
            self._meter_provider = MeterProvider(resource=resource, metric_readers=[self._metric_reader])
            metrics.set_meter_provider(self._meter_provider)
            self.meter = metrics.get_meter(service_name)

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
    def start_task_span(
        self, task_type: str, task_id: str, job_id: str, parent_context: dict[str, Any] | None = None
    ) -> Generator[Any | None, None, None]:
        """Starts a span for a task execution, optionally linked to a parent context."""
        if not self.enabled or not self.tracer:
            yield None
            return

        ctx = None
        if parent_context and propagate:
            ctx = propagate.extract(parent_context)

        with self.tracer.start_as_current_span(
            f"task.{task_type}",
            context=ctx,
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
        if not self.enabled or not self.meter:
            return
        self.tasks_total.add(1, {"task.type": task_type, "status": status})
        self.tasks_duration.record(duration, {"task.type": task_type})

    def record_s3_op(self, op: str, status: str) -> None:
        """Updates S3 metrics."""
        if not self.enabled or not self.meter:
            return
        self.s3_ops_total.add(1, {"s3.operation": op, "status": status})

    def set_span_status_error(self, span: Any, message: str) -> None:
        """Sets the span status to ERROR with a message."""
        if not self.enabled or not span or not Status:
            return
        span.set_status(Status(StatusCode.ERROR, message))

    async def shutdown(self) -> None:
        """Gracefully shuts down the observability providers and flushes data."""
        if not self.enabled:
            return

        logger.info("Shutting down Observability providers...")

        if hasattr(self, "_tracer_provider"):
            try:
                self._tracer_provider.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down tracer provider: {e}")

        if hasattr(self, "_meter_provider"):
            try:
                self._meter_provider.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down meter provider: {e}")

    @property
    def has_otel(self) -> bool:
        return self.enabled
