from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
)


REGISTRY = CollectorRegistry()


rows_attempted_total = Counter(
    name="rows_attempted_total",
    documentation="Total number of rows attempted for replay",
    registry=REGISTRY,
)

rows_sent_total = Counter(
    name="rows_sent_total",
    documentation="Total number of rows successfully sent via HTTP",
    registry=REGISTRY,
)

rows_failed_validation_total = Counter(
    name="rows_failed_validation_total",
    documentation="Total number of rows that failed schema validation",
    registry=REGISTRY,
)


active_sources = Gauge(
    name="active_sources",
    documentation="Number of active replay sources",
    registry=REGISTRY,
)


batch_latency_ms = Histogram(
    name="batch_latency_ms",
    documentation="Latency of batch flush in milliseconds",
    buckets=(
        50,
        100,
        250,
        500,
        1000,
        2500,
        5000,
        10000,
    ),
    registry=REGISTRY,
)


def export_metrics() -> bytes:
    
    return generate_latest(REGISTRY)
