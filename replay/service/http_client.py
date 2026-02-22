import time
import random
from typing import Dict, Any, Optional

import requests
from requests import Response


class HttpDeliveryError(RuntimeError):
    

    def __init__(
        self,
        *,
        status_code: Optional[int],
        message: str,
        attempts: int,
    ):
        self.status_code = status_code
        self.attempts = attempts
        super().__init__(message)


class HttpClient:
    

    def __init__(
        self,
        *,
        endpoint: str,
        timeout_seconds: float = 5.0,
        max_retries: int = 5,
        base_backoff_seconds: float = 0.5,
        max_backoff_seconds: float = 10.0,
    ):
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds

    
    def post_json(self, payload: Dict[str, Any]) -> Response:
        

        attempt = 0

        while True:
            attempt += 1

            try:
                response = requests.post(
                    self.endpoint,
                    json=payload,
                    timeout=self.timeout_seconds,
                )

                if self._is_success(response):
                    return response

                if not self._is_retryable_status(response.status_code):
                    raise HttpDeliveryError(
                        status_code=response.status_code,
                        message=f"Non-retryable HTTP status {response.status_code}",
                        attempts=attempt,
                    )

            except requests.exceptions.RequestException as e:
                # Network / timeout errors are retryable
                last_exception = e
            else:
                last_exception = None

            
            if attempt >= self.max_retries:
                raise HttpDeliveryError(
                    status_code=(
                        response.status_code if "response" in locals() else None
                    ),
                    message="HTTP delivery failed after retries",
                    attempts=attempt,
                )

            sleep_time = self._compute_backoff(attempt)
            time.sleep(sleep_time)

    
    @staticmethod
    def _is_success(response: Response) -> bool:
        return 200 <= response.status_code < 300

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        if status_code >= 500:
            return True
        if status_code == 429:
            return True
        return False

    def _compute_backoff(self, attempt: int) -> float:
        
        exp = self.base_backoff_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0.5, 1.5)
        return min(exp * jitter, self.max_backoff_seconds)
