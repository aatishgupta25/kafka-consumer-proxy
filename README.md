# Kafka Consumer Proxy

A push-based Kafka consumer proxy that decouples application concurrency from Kafka partition count.

The proxy owns Kafka consumption, dispatches records to worker processes over gRPC, tracks acknowledgements independently from processing order, and advances Kafka offsets only across the contiguous acknowledged prefix. Failed records are retried and can be routed to a Kafka dead-letter topic so poison-pill messages do not stall the partition indefinitely.

## Architecture

The system is split into four core components:

- **Kafka adapter** fetches records and commits explicit next offsets for the consumer group.
- **Dispatcher** fans records out across gRPC workers with a bounded number of in-flight requests.
- **Commit tracker** accepts out-of-order acknowledgements but advances commits only when every earlier offset is complete.
- **Failure controller** retries failed work, writes exhausted records to a DLQ, and pauses new dispatch after repeated worker failures.

Because processing is concurrent, a slow message does not block later messages from being worked on. Because commits only advance across the contiguous completed prefix, a later completion cannot cause Kafka to skip an earlier unprocessed message. This preserves at-least-once delivery while allowing application concurrency to exceed the Kafka partition count.

## Offset flow

For records at offsets `10`, `11`, and `12`, workers may complete them in the order `11`, `12`, `10`. The tracker holds the later acknowledgements without committing them. Once offset `10` completes, the contiguous prefix extends through `12`, so the proxy commits next offset `13`.

If a record exhausts its retry budget, the proxy publishes the original key/value plus source topic, partition, offset, and failure reason to the configured DLQ. The record is considered complete only after that DLQ write succeeds.

## gRPC workers

Workers implement a unary `worker.Worker/Process` RPC. The proxy sends the topic, partition, offset, key, and value for each record and treats a successful RPC response as the acknowledgement for that delivery attempt.

Worker addresses are supplied independently of Kafka partitions, so multiple worker instances can consume work originating from the same partition.

## Configuration

The runnable proxy is in `cmd/proxy` and is configured with environment variables:

```text
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=events
KAFKA_GROUP=consumer-proxy
DLQ_TOPIC=events-dlq
WORKERS=localhost:7001,localhost:7002,localhost:7003
MAX_IN_FLIGHT=64
MAX_ATTEMPTS=3
BREAKER_FAILURES=5
BREAKER_COOLDOWN_MS=1000
```

Run it with:

```bash
go run ./cmd/proxy
```

Run the tests with:

```bash
go test ./...
```

The test suite covers contiguous-prefix offset advancement, worker fan-out, slow out-of-order completions, poison-pill DLQ handling, circuit-breaker cooldown, and an in-memory gRPC client/server round trip.
