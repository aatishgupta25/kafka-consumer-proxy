# Kafka Consumer Proxy

A push-based Kafka consumer proxy that decouples application concurrency from Kafka partition count.

The proxy owns Kafka consumption, dispatches records to multiple application workers over gRPC, tracks acknowledgements independently from processing order, and advances Kafka offsets only across the contiguous acknowledged prefix. Records that exhaust retries are written to a Kafka dead-letter topic so poison-pill messages do not block the partition indefinitely.

## Architecture

The runtime is split into four core components:

- **Kafka consumer adapter** fetches records and commits only offsets declared safe by the commit tracker.
- **Dispatcher** fans records from a partition out across a round-robin pool of gRPC workers with bounded in-flight concurrency.
- **Commit tracker** accepts out-of-order acknowledgements but advances commits only when every earlier offset has completed.
- **Failure controller** retries failed records, routes exhausted records to the DLQ, and pauses new fetches when consecutive worker failures open the circuit breaker.

This lets one Kafka partition keep multiple application workers busy while preserving at-least-once delivery semantics across normal processing failures.

## Worker contract

Workers implement the gRPC method:

```text
consumerproxy.Worker/Process
```

The request contains the serialized record metadata, key, and value. A successful response acknowledges processing; a gRPC error triggers the retry and failure path.

The service contract is defined in `api/worker.proto`.

## Run

The proxy is started from `cmd/proxy` and configured through environment variables:

```text
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=events
KAFKA_GROUP_ID=consumer-proxy
WORKER_TARGETS=localhost:50051,localhost:50052
DLQ_TOPIC=events-dlq
```

Optional tuning:

```text
MAX_ATTEMPTS=3
MAX_IN_FLIGHT=64
BREAKER_THRESHOLD=5
BREAKER_COOLDOWN_MS=2000
```

Run locally with:

```bash
go run ./cmd/proxy
```

Run the test suite with:

```bash
go test ./...
```
