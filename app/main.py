from fastapi import FastAPI, HTTPException
from app.db import get_db_connection

import json
import uuid
from datetime import datetime

app = FastAPI()


@app.on_event("startup")
def startup_event():
    conn = get_db_connection()
    conn.close()


@app.post("/ingest")
def ingest_event(payload: dict):
    """
    Accept raw JSON payload and store it unchanged.
    """
    event_id = uuid.uuid4()
    received_at = datetime.utcnow()

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_events (id, received_at, payload)
                VALUES (%s, %s, %s)
                """,
                (event_id, received_at, json.dumps(payload)),
            )
            conn.commit()
    except Exception as e:
        # raise HTTPException(status_code=500, detail="Failed to ingest event")
        print(f"Error ingesting event: {e}")
    finally:
        if conn:
            conn.close()

    return {
        "id": str(event_id),
        "received_at": received_at.isoformat()
    }


@app.get("/health")
def health_check():
    return {"status": "ok"}
