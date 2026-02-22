import json
from typing import Dict, Any

from aiokafka import AIOKafkaProducer


class KafkaProduceError(RuntimeError):
    pass


class KafkaProducerWrapper:
    

    def __init__(
        self,
        *,
        producer: AIOKafkaProducer,
        topic_map: Dict[str, str],
    ):
        self._producer = producer
        self._topic_map = topic_map

    async def send(self, *, event: Dict[str, Any]) -> None:
        try:
            metadata = event["metadata"]
            module = metadata["module"]
            vehicle_id = metadata["vehicle_id"]
        except KeyError as e:
            raise KafkaProduceError(
                f"Missing required metadata field: {e}"
            ) from e

        if module not in self._topic_map:
            raise KafkaProduceError(
                f"No Kafka topic configured for module '{module}'"
            )

        topic = self._topic_map[module]
        key = vehicle_id.encode("utf-8")

        try:
            value = json.dumps(
                event,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except Exception as e:
            raise KafkaProduceError("JSON serialization failed") from e

        try:
            await self._producer.send_and_wait(
                topic=topic,
                key=key,
                value=value,
            )
        except Exception as e:
            raise KafkaProduceError(
                f"Kafka send failed for topic={topic}"
            ) from e
