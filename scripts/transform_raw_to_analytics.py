from datetime import datetime, timezone
import sys

from app.db import get_db_connection


def transform_raw_events():
    """
    Read raw_events and upsert into analytics_events.
    This function is idempotent.
    """
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            # Read all raw events
            cur.execute(
                """
                SELECT id, received_at, payload
                FROM raw_events
                """
            )
            raw_rows = cur.fetchall()

            print(f"Found {len(raw_rows)} raw events")

            for row in raw_rows:
                event_id = row["id"]
                received_at = row["received_at"]
                payload = row["payload"]

                # Extract fields from payload safely
                source = payload.get("source")
                value = payload.get("value")

                transformed_at = datetime.now(timezone.utc)

                # Upsert into analytics table
                cur.execute(
                    """
                    INSERT INTO analytics_events (
                        event_id,
                        received_at,
                        source,
                        value,
                        transformed_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id)
                    DO UPDATE SET
                        received_at = EXCLUDED.received_at,
                        source = EXCLUDED.source,
                        value = EXCLUDED.value,
                        transformed_at = EXCLUDED.transformed_at
                    """,
                    (
                        event_id,
                        received_at,
                        source,
                        value,
                        transformed_at,
                    ),
                )

            conn.commit()
            print(f"Upserted {len(raw_rows)} analytics rows")

    finally:
        conn.close()


if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).isoformat()}] Transform job started")

    try:
        transform_raw_events()
    except Exception as e:
        print(f"[ERROR] Transform job failed: {e}", file=sys.stderr)
        raise
    finally:
        print(f"[{datetime.now(timezone.utc).isoformat()}] Transform job finished")

