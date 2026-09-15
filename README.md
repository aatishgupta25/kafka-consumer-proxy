# Kafka Consumer Proxy

A push-based Kafka consumer proxy that decouples application concurrency from Kafka partition count.

The proxy owns Kafka consumption, dispatches records to worker processes over gRPC, tracks acknowledgements independently from processing order, and advances Kafka offsets only across the contiguous acknowledged prefix. Failed records can be routed to a dead-letter queue so poison-pill messages do not stall progress indefinitely.

## Architecture

The system is split into four core components:

- **Fetcher** reads records from Kafka.
- **Dispatcher** fans records out to available gRPC workers.
- **Commit tracker** accepts out-of-order acknowledgements but advances commits only when all earlier offsets are complete.
- **Failure controller** routes exhausted records to a DLQ and temporarily stops dispatch when consecutive failures cross a configured threshold.

This lets one Kafka partition feed multiple application workers while preserving at-least-once delivery semantics.
