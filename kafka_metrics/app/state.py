from collections import defaultdict, deque
from threading import Lock
from typing import Dict, Any, Optional, List


class MetricsState:
    """
    In-memory, thread-safe state store for Kafka metrics consumer.

    This state is:
    - ephemeral (reset on restart)
    - bounded (no unbounded growth)
    - observation-only (no side effects)
    """

    def __init__(
        self,
        *,
        max_latency_samples: int = 10_000,
    ):
        self._lock = Lock()

        # -------------------------
        # Counters
        # -------------------------
        self._total_rows: int = 0
        self._per_vehicle_count: Dict[str, int] = defaultdict(int)

        # -------------------------
        # Latest message per vehicle
        # -------------------------
        self._latest_per_vehicle: Dict[str, Dict[str, Any]] = {}

        # -------------------------
        # Rolling latency samples (ms)
        # -------------------------
        self._latency_samples_ms: deque[float] = deque(
            maxlen=max_latency_samples
        )

    # -------------------------------------------------
    # Update methods (write path)
    # -------------------------------------------------
    def record_message(
        self,
        *,
        vehicle_id: str,
        full_event: Dict[str, Any],
        latency_ms: Optional[float] = None,
    ) -> None:
        """
        Record a single Kafka message observation.
        """

        with self._lock:
            self._total_rows += 1
            self._per_vehicle_count[vehicle_id] += 1
            self._latest_per_vehicle[vehicle_id] = full_event

            if latency_ms is not None:
                self._latency_samples_ms.append(latency_ms)

    # -------------------------------------------------
    # Read-only accessors (safe snapshots)
    # -------------------------------------------------
    def total_rows(self) -> int:
        with self._lock:
            return self._total_rows

    def per_vehicle_counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._per_vehicle_count)

    def latest_all(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._latest_per_vehicle)

    def latest_for_vehicle(
        self, *, vehicle_id: str
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._latest_per_vehicle.get(vehicle_id)

    def latency_samples(self) -> List[float]:
        """
        Returns a snapshot list of latency samples (ms).
        """
        with self._lock:
            return list(self._latency_samples_ms)

    # -------------------------------------------------
    # Reset (used only for tests)
    # -------------------------------------------------
    def reset(self) -> None:
        """
        Clear all state. Intended for testing only.
        """
        with self._lock:
            self._total_rows = 0
            self._per_vehicle_count.clear()
            self._latest_per_vehicle.clear()
            self._latency_samples_ms.clear()
