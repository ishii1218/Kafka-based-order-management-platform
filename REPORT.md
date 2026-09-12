# Kafka-Based Order Analytics System

**Student:** _Add your name and registration number_  
**Module:** Big Data Analytics  
**Assignment:** Chapter 3

## 1. Overview

The solution is an event-driven order-processing pipeline built with Apache Kafka. A Python producer publishes purchase transactions to the `orders` topic. Every value is encoded as Avro binary against the supplied `Order` schema. A consumer decodes the records with the same schema, applies validation and simulated business processing, and continuously prints the running average of successfully processed prices.

## 2. Architecture

```text
Order Producer
      |
      | Avro Order
      v
Kafka: orders (Avro binary values)
      |
      v
Analytics Consumer ---- success ----> Running count, total, and average
      |
      +---- temporary error ----> exponential retry ----> success or DLQ
      |
      +---- permanent error -----------------------------> Kafka: orders-dlq
                                                               |
                                                               v
                                                         DLQ Monitor
```

The Compose environment contains one Apache Kafka broker in KRaft mode, topic initialization, producer, analytics consumer, and DLQ consumer services. Three `orders` partitions demonstrate a scalable topic layout. The DLQ has one partition because this local demonstration prioritizes simple inspection.

## 3. Avro message design

The required `Order` record contains `orderId` as a string, `product` as a string, and `price` as a float. The producer uses `fastavro` schemaless binary encoding, and the analytics consumer decodes against the matching version-controlled schema. DLQ values also use Avro, with the original fields plus error type, message, retry count, and UTC failure timestamp.

Avro provides a compact binary representation and a version-controlled data contract at publication and consumption boundaries. In this assignment, producer and consumer deploy the schema files together. A larger production system could add a schema registry for independent schema discovery and compatibility enforcement.

## 4. Real-time aggregation

For every successfully processed order, the consumer updates an in-memory count and total. It calculates the running average as:

```text
running average = cumulative price total / successfully processed order count
```

Failed orders are excluded because they have not completed business processing. This assignment implementation demonstrates a live running result. In production, Kafka Streams, ksqlDB, Flink, or an external state store could persist and recover aggregation state.

## 5. Retry and DLQ behavior

Temporary failures use bounded exponential backoff. With three configured retries and a 0.5-second base delay, waits are 0.5, 1.0, and 2.0 seconds. A temporary error that later clears is processed normally. If all retries are exhausted, the original order and failure metadata are published to `orders-dlq`.

Permanent validation or business errors bypass retries and go directly to the DLQ. The consumer synchronously confirms the DLQ publication before committing the source offset. This prevents a source record from being acknowledged before its failed representation is safely recorded. Kafka producer idempotence and `acks=all` improve delivery reliability.

## 6. Demonstration scenarios

The producer sends ordinary randomized orders plus three deterministic examples:

1. `TEMPORARY_FAIL` fails twice and succeeds on its third processing attempt.
2. `PERMANENT_FAIL` goes directly to the DLQ.
3. `ALWAYS_TEMP_FAIL` exhausts three retries and then goes to the DLQ.

The separate DLQ monitor consumes and displays the two failed records. Unit tests cover aggregation, recovery after temporary failures, permanent failures, and invalid prices.

## 7. Limitations and production improvements

The running average is local to one consumer process and resets when it restarts. A production system should use durable state and partition-aware aggregation. Retries performed inside the consumer intentionally simplify the live demo, but a high-throughput design should use delayed retry topics so one failing record does not pause its assigned partition. Production deployments should also use multiple brokers, replication, authentication, TLS, monitoring, and controlled schema compatibility rules.

## 8. Reproduction

The complete reproduction and Git submission commands are documented in `README.md`. Running `docker compose up --build` produces the required live evidence in one combined log stream.
