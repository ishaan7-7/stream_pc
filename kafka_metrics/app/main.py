# kafka_metrics/app/main.py

import asyncio
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse, JSONResponse

from kafka_metrics.app.config_loader import MetricsConsumerConfig
from kafka_metrics.app.consumer import KafkaMetricsConsumer
from kafka_metrics.app.state import MetricsState
from kafka_metrics.app.metrics import export_metrics

# -------------------------------------------------
# Logging
# -------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kafka_metrics.main")

# -------------------------------------------------
# Load configuration (fail fast)
# -------------------------------------------------
CONFIG_PATH = (
    Path(__file__).resolve()
    .parents[1] / "config" / "metrics_consumer.json"
)

config = MetricsConsumerConfig(CONFIG_PATH)

# -------------------------------------------------
# Core state
# -------------------------------------------------
state = MetricsState()

consumer = KafkaMetricsConsumer(
    bootstrap_servers=config.kafka_bootstrap_servers,
    topics=config.kafka_topics,
    group_id=config.kafka_group_id,
    state=state,
)

# -------------------------------------------------
# FastAPI app
# -------------------------------------------------
app = FastAPI(
    title="Kafka Metrics Service",
    version="1.0",
)

# -------------------------------------------------
# Background task handle
# -------------------------------------------------
_consumer_task: asyncio.Task | None = None


# -------------------------------------------------
# Startup / Shutdown
# -------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    global _consumer_task

    try:
        await consumer.start()
        _consumer_task = asyncio.create_task(
            consumer.run_forever()
        )
        logger.info("Kafka metrics consumer started")
    except Exception:
        # Kafka failure must NOT crash HTTP
        logger.exception("Failed to start Kafka consumer")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global _consumer_task

    try:
        await consumer.stop()
    except Exception:
        logger.exception("Error stopping Kafka consumer")

    if _consumer_task:
        _consumer_task.cancel()


# -------------------------------------------------
# Health
# -------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------------------------------
# Metrics (Prometheus)
# -------------------------------------------------
@app.get("/metrics")
def metrics():
    return PlainTextResponse(
        export_metrics(),
        media_type="text/plain",
    )


# -------------------------------------------------
# Latest state inspection
# -------------------------------------------------
@app.get("/latest")
def latest_all():
    return JSONResponse(
        content=state.latest_all()
    )


@app.get("/latest/{vehicle_id}")
def latest_vehicle(vehicle_id: str):
    data = state.latest_for_vehicle(vehicle_id=vehicle_id)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="vehicle_id not found",
        )
    return JSONResponse(content=data)
