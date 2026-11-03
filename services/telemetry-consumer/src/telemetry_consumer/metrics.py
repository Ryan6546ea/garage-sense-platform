"""Prometheus metrics owned by one telemetry consumer instance."""

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class ConsumerMetrics:
    """Bounded-cardinality metrics for queueing, processing, and failures."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.received = Counter(
            "telemetry_consumer_messages_received_total",
            "MQTT telemetry messages received by the dispatcher.",
            ("sensor_type",),
            registry=self.registry,
        )
        self.persisted = Counter(
            "telemetry_consumer_messages_persisted_total",
            "Telemetry messages newly persisted in a typed table.",
            ("sensor_type",),
            registry=self.registry,
        )
        self.duplicates = Counter(
            "telemetry_consumer_duplicates_total",
            "Duplicate telemetry messages ignored by messageId.",
            ("sensor_type",),
            registry=self.registry,
        )
        self.failures = Counter(
            "telemetry_consumer_failures_total",
            "Telemetry messages that could not be accepted or processed.",
            ("sensor_type", "reason"),
            registry=self.registry,
        )
        self.queue_length = Gauge(
            "telemetry_consumer_queue_length",
            "Current number of messages waiting in a sensor-type queue.",
            ("sensor_type",),
            registry=self.registry,
        )
        self.queue_wait_seconds = Histogram(
            "telemetry_consumer_queue_wait_seconds",
            "Time a telemetry message waits before its worker starts.",
            ("sensor_type",),
            registry=self.registry,
        )
        self.processing_seconds = Histogram(
            "telemetry_consumer_processing_seconds",
            "Validation and persistence duration for one telemetry message.",
            ("sensor_type",),
            registry=self.registry,
        )
