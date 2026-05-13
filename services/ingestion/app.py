import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
import sseclient
from kafka import KafkaProducer
from dateutil import parser as date_parser

STREAM_URL = os.getenv("WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/page-create")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "page-create-events")


def get_nested(obj, *keys, default=None):
    current = obj
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def normalize_event(event: dict) -> dict | None:
    meta = event.get("meta", {})
    page = event.get("page", {})
    performer = event.get("performer", {})

    page_id = page.get("page_id") or event.get("page_id")
    page_title = page.get("page_title") or event.get("page_title")
    domain = meta.get("domain") or event.get("database") or event.get("wiki")
    dt = meta.get("dt") or event.get("dt") or datetime.now(timezone.utc).isoformat()

    if not page_id or not page_title or not domain:
        return None

    try:
        created_at = date_parser.parse(dt).astimezone(timezone.utc).isoformat()
    except Exception:
        created_at = datetime.now(timezone.utc).isoformat()

    user_id = performer.get("user_id") or event.get("user_id") or 0
    user_name = performer.get("user_text") or performer.get("user_name") or event.get("user_name") or "unknown"
    is_bot = bool(performer.get("user_is_bot") or event.get("bot") or event.get("is_bot") or False)

    stable_id = f"{domain}:{page_id}:{created_at}"
    return {
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, stable_id)),
        "domain": str(domain),
        "page_id": int(page_id),
        "page_title": str(page_title),
        "user_id": int(user_id or 0),
        "user_name": str(user_name),
        "is_bot": is_bot,
        "created_at": created_at,
        "title_length": len(str(page_title)),
        "raw": event,
    }


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8"),
        linger_ms=100,
        retries=5,
    )


def run() -> None:
    producer = build_producer()
    while True:
        try:
            print(f"Connecting to {STREAM_URL}", flush=True)
            response = requests.get(STREAM_URL, stream=True, timeout=60, headers={"User-Agent": "wiki-analytics-course-project/1.0"})
            response.raise_for_status()
            client = sseclient.SSEClient(response)
            for msg in client.events():
                if not msg.data or msg.data == "{}":
                    continue
                try:
                    event = json.loads(msg.data)
                    normalized = normalize_event(event)
                    if normalized:
                        producer.send(KAFKA_TOPIC, key=normalized["domain"], value=normalized)
                        producer.flush(timeout=1)
                        print(f"Sent {normalized['domain']} / {normalized['page_title']}", flush=True)
                except Exception as e:
                    print(f"Bad event skipped: {e}", flush=True)
        except Exception as e:
            print(f"Stream error: {e}. Reconnecting...", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    run()
