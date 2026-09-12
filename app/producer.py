from __future__ import annotations

import argparse
import random
import time
import uuid

from confluent_kafka import Producer

from app.avro_codec import encode_avro
from app.settings import BOOTSTRAP_SERVERS, ORDERS_TOPIC


def delivery_report(error, message) -> None:
    if error:
        print(f"DELIVERY FAILED: {error}")
    else:
        print(
            f"PRODUCED key={message.key()} "
            f"topic={message.topic()} partition={message.partition()} offset={message.offset()}"
        )


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def demo_orders(normal_count: int) -> list[dict]:
    products = ["Laptop", "Phone", "Monitor", "Keyboard", "Headphones"]
    orders = [
        {
            "orderId": str(uuid.uuid4()),
            "product": random.choice(products),
            "price": round(random.uniform(10.0, 1000.0), 2),
        }
        for _ in range(normal_count)
    ]
    orders.extend(
        [
            {"orderId": "demo-retry-success", "product": "TEMPORARY_FAIL", "price": 125.0},
            {"orderId": "demo-permanent", "product": "PERMANENT_FAIL", "price": 70.0},
            {"orderId": "demo-retry-exhausted", "product": "ALWAYS_TEMP_FAIL", "price": 90.0},
        ]
    )
    return orders


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce Avro order messages")
    parser.add_argument("--count", type=int, default=10, help="number of valid random orders")
    parser.add_argument("--interval", type=float, default=0.25, help="seconds between messages")
    parser.add_argument("--no-failures", action="store_true", help="omit the three demo failure orders")
    args = parser.parse_args()

    producer = build_producer()
    orders = demo_orders(args.count)
    if args.no_failures:
        orders = orders[: args.count]

    print(f"Producing {len(orders)} Avro orders to {ORDERS_TOPIC}")
    for order in orders:
        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["orderId"].encode("utf-8"),
            value=encode_avro(order, "order.avsc"),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        time.sleep(args.interval)

    outstanding = producer.flush(15)
    if outstanding:
        raise RuntimeError(f"{outstanding} message(s) were not delivered")
    print("Producer finished")


if __name__ == "__main__":
    main()
