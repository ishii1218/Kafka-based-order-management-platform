# Real-time Kafka Order-processing Pipeline

This project implements the Kafka-based system that produces and consumes order messages

## What it demonstrates

- Apache Kafka order producer and consumer
- Avro binary serialization using the supplied schemas
- Live global running average of successfully processed order prices
- Exponential-backoff retry logic for temporary failures
- An Avro-serialized `orders-dlq` topic for permanent failures and exhausted retries
- Manual Kafka offset commits only after success or acknowledged DLQ delivery
- Deterministic demo messages and automated unit tests

## Prerequisites

- Docker Desktop with Docker Compose
- Approximately 2 GB free memory for the containers

No local Python or Kafka installation is required.

## Run the live demonstration

Open PowerShell in this folder and run:

```powershell
.\run-demo.ps1
```

Or run the equivalent command directly:

```powershell
docker compose up --build
```

Watch the combined logs. The expected evidence is:

1. `PRODUCED` messages from the producer.
2. `PROCESSED ... runningAverage=...` messages from the consumer.
3. Two `RETRY` messages followed by a successful `demo-retry-success` order.
4. A permanent failure sent immediately to the DLQ.
5. An always-temporary failure sent to the DLQ after three retries.
6. `DLQ RECEIVED` messages from the independent DLQ consumer.

Press `Ctrl+C`, then remove the demo containers and volumes:

```powershell
docker compose down -v
```

## Repeat only the producer

While the stack is running in one terminal, use another terminal:

```powershell
docker compose run --rm producer python -m app.producer --count 5
```

To send only valid orders:

```powershell
docker compose run --rm producer python -m app.producer --count 20 --no-failures
```

## Run tests

```powershell
docker compose build consumer
docker compose run --rm --no-deps consumer python -m pytest -q
```

## Inspect Kafka

List topics:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
```

## Failure policy

The special product values exist only to make failure behavior easy to prove live:

| Product | Behavior |
|---|---|
| Any ordinary name | Processes immediately |
| `TEMPORARY_FAIL` | Fails twice, then succeeds |
| `ALWAYS_TEMP_FAIL` | Exhausts retries, then moves to the DLQ |
| `PERMANENT_FAIL` | Moves directly to the DLQ |

Invalid order identifiers, products, and non-positive/non-finite prices are also permanent processing errors. Avro itself rejects records that do not match `schemas/order.avsc` before publication.

