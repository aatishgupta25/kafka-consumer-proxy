# Live Kafka CI

The CI workflow starts an Apache Kafka 3.8.1 broker and runs the Kafka adapter integration test against it.

The integration test verifies that the proxy can:

- produce and fetch a record through Kafka
- commit the consumed offset through the consumer group
- publish a failed record to the Kafka dead-letter topic
- read the dead-letter record back from the broker

The test is skipped during ordinary local `go test ./...` runs unless `KAFKA_INTEGRATION=1` is set.
