"""Replay API scaffold (FastAPI).

Phase 1 deliverable: green CI + smoke endpoint. No endpoints implemented yet.
"""

from fastapi import FastAPI

app = FastAPI(title="Replay", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok", "service": "replay-api"}
