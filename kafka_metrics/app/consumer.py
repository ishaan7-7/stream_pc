import json
import logging
from typing import List

from aiokafka import AIOKafkaConsumer

from kafka_metrics.app.schemas import parse_message
from kafka_metrics.app.state import MetricsState
from kafka_metrics.app.metrics import (
    kafka_rows_total,
    kafka_rows_per_vehicle,
    kafka_processing_latency_ms,
)

logger = logging.getLogger("kafka_metrics.consumer")


class KafkaMetricsConsumer:
    """
    Observer-only Kafka consumer.

    - No retries
    - No custom offset management
    - Never crashes on bad data
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topics: List[str],
        group_id: str,
        state: MetricsState,
    ):
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: v,  # raw bytes
        )
        self._state = state

    async def start(self) -> None:
        await self._consumer.start()
        logger.info("Kafka metrics consumer started")

    async def stop(self) -> None:
        await self._consumer.stop()
        logger.info("Kafka metrics consumer stopped")

    async def run_forever(self) -> None:
        """
        Main consume loop.
        This method NEVER raises.
        """
        try:
            async for record in self._consumer:
                await self._handle_record(record)
        except Exception:
            logger.exception("Kafka consumer loop crashed (unexpected)")

    async def _handle_record(self, record) -> None:
        try:
            kafka_ts_ms = record.timestamp
            raw_bytes = record.value

            try:
                payload = json.loads(raw_bytes.decode("utf-8"))
            except Exception:
                logger.warning("Malformed JSON received, skipping")
                return

            parsed = parse_message(payload)

            latency_ms = None
            if parsed.ingest_ts_ms is not None and kafka_ts_ms is not None:
                delta = kafka_ts_ms - parsed.ingest_ts_ms
                if delta >= 0:
                    latency_ms = delta

            # -------------------------
            # State update
            # -------------------------
            self._state.record_message(
                vehicle_id=parsed.vehicle_id,
                full_event=payload,
                latency_ms=latency_ms,
            )

            # -------------------------
            # Metrics
            # -------------------------
            kafka_rows_total.inc()
            kafka_rows_per_vehicle.labels(
                vehicle_id=parsed.vehicle_id
            ).inc()

            if latency_ms is not None:
                kafka_processing_latency_ms.observe(latency_ms)

        except Exception:
            logger.exception("Failed to process Kafka record, skipping")
