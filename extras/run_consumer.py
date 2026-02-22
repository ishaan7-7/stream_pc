import sys
from pathlib import Path

# -------------------------------------------------
# Ensure repo root is on PYTHONPATH
# -------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import asyncio

from kafka_metrics.app.config_loader import MetricsConsumerConfig
from kafka_metrics.app.consumer import KafkaMetricsConsumer
from kafka_metrics.app.state import MetricsState


async def main():
    config = MetricsConsumerConfig(
        REPO_ROOT / "kafka_metrics" / "config" / "metrics_consumer.json"
    )

    state = MetricsState()

    consumer = KafkaMetricsConsumer(
        bootstrap_servers=config.kafka_bootstrap_servers,
        topics=config.kafka_topics,
        group_id=config.kafka_group_id,
        state=state,
    )

    await consumer.start()
    print("Kafka metrics consumer started")

    try:
        await consumer.run_forever()
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
