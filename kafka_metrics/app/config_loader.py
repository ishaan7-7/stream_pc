from pathlib import Path
import json
from typing import Any, Dict, List


class MetricsConfigError(RuntimeError):
    """Raised when metrics_consumer.json is invalid."""
    pass


class MetricsConsumerConfig:
    REQUIRED_TOP_LEVEL_KEYS = {"kafka", "http"}
    REQUIRED_KAFKA_KEYS = {"bootstrap_servers", "topics", "group_id"}
    REQUIRED_HTTP_KEYS = {"port"}

    def __init__(self, config_path: Path):
        self.config_path = config_path

        self._raw: Dict[str, Any] = {}
        self._kafka: Dict[str, Any] = {}
        self._http: Dict[str, Any] = {}

        self._load_and_validate()

    # -------------------------------------------------
    # Load & validate
    # -------------------------------------------------
    def _load_and_validate(self) -> None:
        if not self.config_path.exists():
            raise MetricsConfigError(
                f"metrics_consumer.json not found at: {self.config_path}"
            )

        try:
            with self.config_path.open("r", encoding="utf-8") as f:
                self._raw = json.load(f)
        except json.JSONDecodeError as e:
            raise MetricsConfigError(
                "metrics_consumer.json is not valid JSON"
            ) from e

        if not isinstance(self._raw, dict):
            raise MetricsConfigError(
                "metrics_consumer.json must be a JSON object"
            )

        missing = self.REQUIRED_TOP_LEVEL_KEYS - self._raw.keys()
        if missing:
            raise MetricsConfigError(
                f"Missing top-level keys: {sorted(missing)}"
            )

        self._kafka = self._validate_kafka(self._raw["kafka"])
        self._http = self._validate_http(self._raw["http"])

    # -------------------------------------------------
    # Section validators
    # -------------------------------------------------
    def _validate_kafka(self, kafka: Any) -> Dict[str, Any]:
        if not isinstance(kafka, dict):
            raise MetricsConfigError("kafka must be an object")

        missing = self.REQUIRED_KAFKA_KEYS - kafka.keys()
        if missing:
            raise MetricsConfigError(
                f"kafka missing keys: {sorted(missing)}"
            )

        if not isinstance(kafka["bootstrap_servers"], str):
            raise MetricsConfigError(
                "kafka.bootstrap_servers must be a string"
            )

        topics = kafka["topics"]
        if (
            not isinstance(topics, list)
            or not topics
            or not all(isinstance(t, str) for t in topics)
        ):
            raise MetricsConfigError(
                "kafka.topics must be a non-empty list of strings"
            )

        if not isinstance(kafka["group_id"], str):
            raise MetricsConfigError(
                "kafka.group_id must be a string"
            )

        return kafka

    def _validate_http(self, http: Any) -> Dict[str, Any]:
        if not isinstance(http, dict):
            raise MetricsConfigError("http must be an object")

        missing = self.REQUIRED_HTTP_KEYS - http.keys()
        if missing:
            raise MetricsConfigError(
                f"http missing keys: {sorted(missing)}"
            )

        port = http["port"]
        if not isinstance(port, int) or not (1024 <= port <= 65535):
            raise MetricsConfigError(
                "http.port must be a valid TCP port (1024–65535)"
            )

        return http

    # -------------------------------------------------
    # Read-only accessors
    # -------------------------------------------------
    @property
    def kafka_bootstrap_servers(self) -> str:
        return self._kafka["bootstrap_servers"]

    @property
    def kafka_topics(self) -> List[str]:
        return self._kafka["topics"]

    @property
    def kafka_group_id(self) -> str:
        return self._kafka["group_id"]

    @property
    def http_port(self) -> int:
        return self._http["port"]
