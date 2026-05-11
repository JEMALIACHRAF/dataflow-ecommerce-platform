"""
ingestion/connectors/pubsub_clickstream_producer.py

Pub/Sub Clickstream Event Producer
Simule les events comportementaux d'une plateforme e-commerce
publiés vers Google Cloud Pub/Sub (équivalent Kafka managé GCP).

En prod : remplacé par le vrai tracking SDK du frontend
En dev  : simule 2B events/day = ~23 000 events/sec
"""

from __future__ import annotations

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ID = "projet-dbt-495310"
TOPIC_ID   = "clickstream-events"

EVENT_TYPES = [
    "page_view", "product_view", "add_to_cart", "remove_from_cart",
    "checkout_start", "purchase", "search", "session_start",
]
COUNTRIES  = ["FR", "DE", "ES", "GB", "IT", "BE", "NL"]
DEVICES    = ["desktop", "mobile", "tablet"]
BROWSERS   = ["chrome", "firefox", "safari", "edge"]
REFERRERS  = [
    "https://www.google.com/search?q=chaussures",
    "https://www.facebook.com/ads",
    "",
    "https://shop.dataflow.io/home",
    "https://www.instagram.com/p/abc",
]
PRODUCTS   = [f"SKU-{i:04d}" for i in range(1, 500)]


def make_event() -> dict:
    etype = random.choice(EVENT_TYPES)
    props = {}
    if etype in ("product_view", "add_to_cart", "purchase"):
        props = {
            "product_id": random.choice(PRODUCTS),
            "price":      round(random.uniform(9.99, 299.99), 2),
            "category":   random.choice(["chaussures", "vêtements", "sport"]),
        }
    if etype == "search":
        props = {
            "search_query":  random.choice(["chaussures", "veste", "sac"]),
            "results_count": random.randint(0, 200),
        }

    return {
        "event_id":        str(uuid.uuid4()),
        "session_id":      str(uuid.uuid4()),
        "user_id":         str(uuid.uuid4()) if random.random() > 0.2 else None,
        "anonymous_id":    str(uuid.uuid4()),
        "event_type":      etype,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "page_url":        f"https://shop.dataflow.io/products/{random.choice(PRODUCTS)}",
        "referrer_url":    random.choice(REFERRERS),
        "device_type":     random.choice(DEVICES),
        "os":              random.choice(["windows", "macos", "ios", "android"]),
        "browser":         random.choice(BROWSERS),
        "country_code":    random.choice(COUNTRIES),
        "ip_address":      f"{random.randint(1,254)}.{random.randint(0,254)}.{random.randint(0,254)}.1",
        "properties":      json.dumps(props) if props else None,
        "revenue":         str(round(random.uniform(15, 350), 2))
                           if etype == "purchase" else None,
        "_ingested_at":    datetime.now(timezone.utc).isoformat(),
        "_source":         "pubsub_producer",
    }


def publish_events(
    n_events: int = 10_000,
    batch_size: int = 100,
    delay_between_batches: float = 0.1,
) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    logger.info("Publishing %d events to %s", n_events, topic_path)

    published  = 0
    futures    = []

    for i in range(n_events):
        event   = make_event()
        payload = json.dumps(event).encode("utf-8")
        future  = publisher.publish(
            topic_path,
            payload,
            event_type=event["event_type"],
            country=event["country_code"],
        )
        futures.append(future)
        published += 1

        if published % batch_size == 0:
            # Wait for batch to complete
            for f in futures:
                f.result()
            futures = []
            logger.info("Published %d/%d events", published, n_events)
            time.sleep(delay_between_batches)

    # Flush remaining
    for f in futures:
        f.result()

    logger.info("✓ Published %d events to Pub/Sub topic: %s", published, TOPIC_ID)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--n-events",   type=int,   default=10_000)
    p.add_argument("--batch-size", type=int,   default=100)
    p.add_argument("--delay",      type=float, default=0.1)
    args = p.parse_args()

    publish_events(
        n_events=args.n_events,
        batch_size=args.batch_size,
        delay_between_batches=args.delay,
    )