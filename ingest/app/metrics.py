from prometheus_client import (
    Counter,
    Histogram,
    CollectorRegistry,
    generate_latest,
)

REGISTRY = CollectorRegistry()

ingest_requests_total = Counter(
    name="ingest_requests_total",
    documentation="Total number of ingest HTTP requests received",
    registry=REGISTRY,
)

ingest_rows_accepted_total = Counter(
    name="ingest_rows_accepted_total",
    documentation="Total number of rows accepted for Kafka production",
    registry=REGISTRY,
)

ingest_rows_rejected_total = Counter(
    name="ingest_rows_rejected_total",
    documentation="Total number of rows rejected by ingest service",
    registry=REGISTRY,
)

ingest_validation_latency_ms = Histogram(
    name="ingest_validation_latency_ms",
    documentation="Schema validation latency in milliseconds",
    buckets=(
        1,
        5,
        10,
        25,
        50,
        100,
        250,
        500,
        1000,
    ),
    registry=REGISTRY,
)

ingest_http_latency_ms = Histogram(
    name="ingest_http_latency_ms",
    documentation="End-to-end ingest HTTP latency in milliseconds",
    buckets=(
        5,
        10,
        25,
        50,
        100,
        250,
        500,
        1000,
        2500,
        5000,
    ),
    registry=REGISTRY,
)

def export_metrics() -> bytes:
    
    return generate_latest(REGISTRY)
