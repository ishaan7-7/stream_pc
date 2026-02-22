from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.post("/ingest/v1/events")
def ingest(payload: dict):
    return JSONResponse({"status": "ok"}, status_code=202)
