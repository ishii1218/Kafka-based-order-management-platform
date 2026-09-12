import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders-dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-analytics-v1")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("RETRY_BACKOFF_SECONDS", "0.5"))


def load_schema(name: str) -> str:
    return (BASE_DIR / "schemas" / name).read_text(encoding="utf-8")
