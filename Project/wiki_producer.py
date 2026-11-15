import json
import os
import time

import requests
from confluent_kafka import Producer

from wikimedia_client import default_user_agent

STREAM_URL = os.environ.get("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia.recentchange")
STATS_EVERY_SECONDS = 60


def sse_events(last_event_id=None):
    """Yield (event_id, data) pairs from the EventStreams endpoint."""
    headers = {"User-Agent": default_user_agent(), "Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        event_id, data = None, []
        for raw in response.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                if data:
                    yield event_id, "\n".join(data)
                event_id, data = None, []
            elif raw.startswith("id:"):
                event_id = raw[3:].strip()
            elif raw.startswith("data:"):
                data.append(raw[5:].lstrip())


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "wiki-producer",
        "linger.ms": 200,
        "compression.type": "gzip",
        "enable.idempotence": True,
    })
    last_event_id, sent, failed, backoff = None, 0, 0, 1
    window_start = time.monotonic()

    def on_delivery(error, _message):
        nonlocal failed
        if error is not None:
            failed += 1

    print(f"Streaming {STREAM_URL} to Kafka topic {TOPIC} at {BOOTSTRAP}", flush=True)
    while True:
        try:
            for event_id, data in sse_events(last_event_id):
                try:
                    event = json.loads(data)
                except ValueError:
                    continue
                if event.get("meta", {}).get("domain") == "canary":
                    continue
                producer.produce(TOPIC, key=event.get("wiki", ""), value=data.encode("utf-8"), on_delivery=on_delivery)
                producer.poll(0)
                last_event_id = event_id or last_event_id
                sent += 1
                backoff = 1
                if time.monotonic() - window_start >= STATS_EVERY_SECONDS:
                    print(f"{time.strftime('%H:%M:%S')} sent={sent:,} failed={failed:,}", flush=True)
                    window_start = time.monotonic()
        except (requests.RequestException, ValueError) as error:
            print(f"Stream interrupted ({error.__class__.__name__}: {error}); reconnecting in {backoff}s", flush=True)
            producer.flush(10)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()
