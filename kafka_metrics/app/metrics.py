from prometheus_client import (
    Counter,
    Histogram,
    CollectorRegistry,
    generate_latest,
)

# -------------------------------------------------
# Dedicated registry (DO NOT use default)
# -------------------------------------------------
REGISTRY = CollectorRegistry()

# -------------------------------------------------
# Counters
# -------------------------------------------------
kafka_rows_total = Counter(
    name="kafka_rows_total",
    documentation="Total number of rows consumed from Kafka",
    registry=REGISTRY,
)

kafka_rows_per_vehicle = Counter(
    name="kafka_rows_per_vehicle",
    documentation="Total rows consumed per vehicle",
    labelnames=("vehicle_id",),
    registry=REGISTRY,
)

# -------------------------------------------------
# Latency histogram (ms)
# -------------------------------------------------
kafka_processing_latency_ms = Histogram(
    name="kafka_processing_latency_ms",
    documentation="Latency between ingest_ts and Kafka timestamp (ms)",
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
        2500,
        5000,
        10000,
    ),
    registry=REGISTRY,
)

# -------------------------------------------------
# Export helper
# -------------------------------------------------
def export_metrics() -> bytes:
    """
    Export all Kafka metrics in Prometheus text format.
    """
    return generate_latest(REGISTRY)
