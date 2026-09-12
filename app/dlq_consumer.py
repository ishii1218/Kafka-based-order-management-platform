from __future__ import annotations

from confluent_kafka import Consumer, KafkaError, KafkaException

from app.avro_codec import decode_avro
from app.settings import BOOTSTRAP_SERVERS, DLQ_TOPIC


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "order-dlq-monitor-v1",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    print(f"Monitoring Avro dead-letter messages on {DLQ_TOPIC}")
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())
            failed = decode_avro(message.value(), "failed_order.avsc")
            print(
                f"DLQ RECEIVED orderId={failed['orderId']} product={failed['product']} "
                f"error={failed['errorType']} retries={failed['retryCount']} "
                f"reason={failed['errorMessage']}"
            )
    except KeyboardInterrupt:
        print("DLQ monitor stopped")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
