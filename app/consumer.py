from __future__ import annotations

import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from app.avro_codec import decode_avro, encode_avro
from app.processing import (
    OrderProcessor,
    PermanentProcessingError,
    RunningAverage,
    TemporaryProcessingError,
)
from app.settings import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    DLQ_TOPIC,
    MAX_RETRIES,
    ORDERS_TOPIC,
    RETRY_BACKOFF_SECONDS,
)


def build_clients():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq_producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
        }
    )
    return consumer, dlq_producer


def send_to_dlq(producer, order: dict, error: Exception, retries: int) -> None:
    failed = {
        "orderId": str(order.get("orderId", "unknown")),
        "product": str(order.get("product", "unknown")),
        "price": float(order.get("price", 0.0)),
        "errorType": type(error).__name__,
        "errorMessage": str(error),
        "retryCount": retries,
        "failedAt": datetime.now(timezone.utc).isoformat(),
    }
    delivery_errors: list[str] = []

    def delivered(err, _message) -> None:
        if err:
            delivery_errors.append(str(err))

    producer.produce(
        DLQ_TOPIC,
        key=failed["orderId"].encode("utf-8"),
        value=encode_avro(failed, "failed_order.avsc"),
        on_delivery=delivered,
    )
    remaining = producer.flush(15)
    if remaining or delivery_errors:
        raise RuntimeError(f"DLQ delivery failed: {delivery_errors or remaining}")
    print(
        f"DLQ orderId={failed['orderId']} error={failed['errorType']} "
        f"retries={retries}"
    )


def main() -> None:
    consumer, dlq_producer = build_clients()
    processor = OrderProcessor()
    average = RunningAverage()
    consumer.subscribe([ORDERS_TOPIC])
    print(f"Consuming Avro orders from {ORDERS_TOPIC}; group={CONSUMER_GROUP}")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            order = decode_avro(message.value(), "order.avsc")
            retries_used = 0
            try:
                while True:
                    try:
                        processor.process(order)
                        current_average = average.add(float(order["price"]))
                        print(
                            f"PROCESSED orderId={order['orderId']} price={order['price']:.2f} "
                            f"count={average.count} runningAverage={current_average:.2f}"
                        )
                        break
                    except TemporaryProcessingError as error:
                        if retries_used >= MAX_RETRIES:
                            send_to_dlq(dlq_producer, order, error, retries_used)
                            break
                        retries_used += 1
                        delay = RETRY_BACKOFF_SECONDS * (2 ** (retries_used - 1))
                        print(
                            f"RETRY orderId={order.get('orderId')} retry={retries_used}/{MAX_RETRIES} "
                            f"delay={delay:.2f}s reason={error}"
                        )
                        time.sleep(delay)
            except PermanentProcessingError as error:
                send_to_dlq(dlq_producer, order, error, retries_used)

            # Commit only after processing succeeds or the DLQ write is acknowledged.
            consumer.commit(message=message, asynchronous=False)
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
