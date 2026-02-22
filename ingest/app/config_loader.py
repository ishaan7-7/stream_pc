from pathlib import Path
import json
from typing import Dict, Any


class IngestConfigError(RuntimeError):
    pass


class IngestConfig:
    REQUIRED_TOP_LEVEL_KEYS = {"kafka", "topics", "dlq", "metrics"}
    REQUIRED_KAFKA_KEYS = {"bootstrap_servers"}
    REQUIRED_DLQ_KEYS = {"path"}
    REQUIRED_METRICS_KEYS = {"port"}

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self._raw: Dict[str, Any] = {}

        self._kafka: Dict[str, Any] = {}
        self._topics: Dict[str, str] = {}
        self._dlq: Dict[str, Any] = {}
        self._metrics: Dict[str, Any] = {}

        self._load_and_validate()

    
    def _load_and_validate(self) -> None:
        if not self.config_path.exists():
            raise IngestConfigError(
                f"ingest_config.json not found at: {self.config_path}"
            )

        try:
            with self.config_path.open("r", encoding="utf-8") as f:
                self._raw = json.load(f)
        except json.JSONDecodeError as e:
            raise IngestConfigError("ingest_config.json is not valid JSON") from e

        if not isinstance(self._raw, dict):
            raise IngestConfigError("ingest_config.json must be a JSON object")

        missing = self.REQUIRED_TOP_LEVEL_KEYS - self._raw.keys()
        if missing:
            raise IngestConfigError(
                f"Missing top-level keys: {sorted(missing)}"
            )

        self._kafka = self._validate_kafka(self._raw["kafka"])
        self._topics = self._validate_topics(self._raw["topics"])
        self._dlq = self._validate_dlq(self._raw["dlq"])
        self._metrics = self._validate_metrics(self._raw["metrics"])

    
    def _validate_kafka(self, kafka: Any) -> Dict[str, Any]:
        if not isinstance(kafka, dict):
            raise IngestConfigError("kafka must be an object")

        missing = self.REQUIRED_KAFKA_KEYS - kafka.keys()
        if missing:
            raise IngestConfigError(
                f"kafka missing keys: {sorted(missing)}"
            )

        if not isinstance(kafka["bootstrap_servers"], str):
            raise IngestConfigError("kafka.bootstrap_servers must be a string")

        return kafka

    def _validate_topics(self, topics: Any) -> Dict[str, str]:
        if not isinstance(topics, dict) or not topics:
            raise IngestConfigError("topics must be a non-empty object")

        for module, topic in topics.items():
            if not isinstance(module, str) or not isinstance(topic, str):
                raise IngestConfigError(
                    "topics must be string-to-string mapping"
                )

        return topics

    def _validate_dlq(self, dlq: Any) -> Dict[str, Any]:
        if not isinstance(dlq, dict):
            raise IngestConfigError("dlq must be an object")

        missing = self.REQUIRED_DLQ_KEYS - dlq.keys()
        if missing:
            raise IngestConfigError(
                f"dlq missing keys: {sorted(missing)}"
            )

        path = Path(dlq["path"])
        dlq["path"] = path

        return dlq

    def _validate_metrics(self, metrics: Any) -> Dict[str, Any]:
        if not isinstance(metrics, dict):
            raise IngestConfigError("metrics must be an object")

        missing = self.REQUIRED_METRICS_KEYS - metrics.keys()
        if missing:
            raise IngestConfigError(
                f"metrics missing keys: {sorted(missing)}"
            )

        port = metrics["port"]
        if not isinstance(port, int) or not (1024 <= port <= 65535):
            raise IngestConfigError("metrics.port must be a valid TCP port")

        return metrics

    
    @property
    def kafka_bootstrap_servers(self) -> str:
        return self._kafka["bootstrap_servers"]

    @property
    def topic_mapping(self) -> Dict[str, str]:
        return self._topics

    @property
    def dlq_path(self) -> Path:
        return self._dlq["path"]

    @property
    def metrics_port(self) -> int:
        return self._metrics["port"]
