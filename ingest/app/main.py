from fastapi import FastAPI, status
from fastapi.responses import JSONResponse, PlainTextResponse
from pathlib import Path
import time
from datetime import datetime, timezone  

from replay.service.schema_loader import MasterSchema

from ingest.app.config_loader import IngestConfig
from ingest.app.schemas import IngestRequest
from ingest.app.validator import DefensiveValidator, IngestValidationError
from ingest.app.idempotency import IdempotencyCache
from ingest.app.producer import KafkaProducerWrapper, KafkaProduceError
from ingest.app.dlq import IngestDLQWriter
from ingest.app.metrics import (
    ingest_requests_total,
    ingest_rows_accepted_total,
    ingest_rows_rejected_total,
    ingest_validation_latency_ms,
    ingest_http_latency_ms,
    export_metrics,
)

from aiokafka import AIOKafkaProducer

# -------------------------------------------------
# App init
# -------------------------------------------------
app = FastAPI(title="Ingest Gateway", version="1.0")

# -------------------------------------------------
# Load config & core components (startup-time only)
# -------------------------------------------------
config = IngestConfig(
    Path("ingest/config/ingest_config.json")
)

master_schema = MasterSchema(
    Path("contracts/master.json")
)

validator = DefensiveValidator(master_schema)

idempotency = IdempotencyCache(
    max_entries=500_000,
    ttl_seconds=3600,
)

producer = AIOKafkaProducer(
    bootstrap_servers=config.kafka_bootstrap_servers
)

producer_wrapper = KafkaProducerWrapper(
    producer=producer,
    topic_map=config.topic_mapping,
)

dlq_writer = IngestDLQWriter(
    dlq_root=config.dlq_path
)

# -------------------------------------------------
# Startup / shutdown hooks
# -------------------------------------------------
@app.on_event("startup")
async def on_startup():
    await producer.start()

@app.on_event("shutdown")
async def on_shutdown():
    await producer.stop()

# -------------------------------------------------
# Health
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -------------------------------------------------
# Metrics
# -------------------------------------------------
@app.get("/metrics")
def metrics():
    return PlainTextResponse(
        export_metrics(),
        media_type="text/plain",
    )

# -------------------------------------------------
# Ingest endpoint
# -------------------------------------------------
@app.post("/ingest/v1/events")
async def ingest_event(payload: IngestRequest):
    ingest_requests_total.inc()
    start_ts = time.monotonic()

    try:
        # -------------------------
        # Defensive validation
        # -------------------------
        with ingest_validation_latency_ms.time():
            validated_data = validator.validate(
                module=payload.metadata.module,
                data=payload.data,
            )

        # -------------------------
        # Idempotency
        # -------------------------
        if idempotency.seen_before(payload.metadata.row_hash):
            ingest_rows_rejected_total.inc()
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={"status": "duplicate"},
            )

        # -------------------------
        # ADD ingest_ts (server-side)
        # -------------------------
        now_ts = datetime.now(timezone.utc).isoformat()  # ✅ ADDED

        # -------------------------
        # Kafka publish
        # -------------------------
        event = {
            "metadata": payload.metadata.model_dump(),
            "data": validated_data,
        }

        event["metadata"]["ingest_ts"] = now_ts  # ✅ ADDED

        await producer_wrapper.send(event=event)

        idempotency.mark_seen(payload.metadata.row_hash)
        ingest_rows_accepted_total.inc()

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": "accepted"},
        )

    except IngestValidationError as e:
        ingest_rows_rejected_total.inc()
        dlq_writer.write(
            error_type="validation_error",
            error_message=e.message,
            error_details=e.details,
            event=payload.model_dump(),
        )
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "validation_failed"},
        )

    except KafkaProduceError as e:
        ingest_rows_rejected_total.inc()
        dlq_writer.write(
            error_type="kafka_error",
            error_message=str(e),
            event=payload.model_dump(),
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"error": "kafka_unavailable"},
        )

    except Exception as e:
        ingest_rows_rejected_total.inc()
        dlq_writer.write(
            error_type="internal_error",
            error_message=str(e),
            event=payload.model_dump(),
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "internal_error"},
        )

    finally:
        elapsed_ms = (time.monotonic() - start_ts) * 1000
        ingest_http_latency_ms.observe(elapsed_ms)
