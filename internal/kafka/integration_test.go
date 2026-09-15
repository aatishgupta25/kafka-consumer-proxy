package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	segmentio "github.com/segmentio/kafka-go"
)

func TestLiveKafkaConsumerAndDLQ(t *testing.T) {
	if os.Getenv("KAFKA_INTEGRATION") != "1" {
		t.Skip("set KAFKA_INTEGRATION=1 to run live Kafka integration test")
	}

	broker := envOr("KAFKA_BROKER", "localhost:9092")
	suffix := time.Now().UnixNano()
	topic := fmt.Sprintf("proxy-it-%d", suffix)
	dlqTopic := fmt.Sprintf("proxy-it-dlq-%d", suffix)
	group := fmt.Sprintf("proxy-it-group-%d", suffix)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	producer := &segmentio.Writer{Addr: segmentio.TCP(broker), Topic: topic}
	defer producer.Close()
	if err := producer.WriteMessages(ctx, segmentio.Message{Key: []byte("k1"), Value: []byte("hello")}); err != nil {
		t.Fatalf("write source message: %v", err)
	}

	consumer := NewConsumer([]string{broker}, topic, group)
	defer consumer.Close()
	record, err := consumer.Fetch(ctx)
	if err != nil {
		t.Fatalf("fetch source message: %v", err)
	}
	if string(record.Key) != "k1" || string(record.Value) != "hello" {
		t.Fatalf("unexpected record: key=%q value=%q", record.Key, record.Value)
	}
	if err := consumer.CommitOffset(ctx, record.Topic, record.Partition, record.Offset+1); err != nil {
		t.Fatalf("commit source offset: %v", err)
	}

	dlq := NewDLQWriter([]string{broker}, dlqTopic)
	defer dlq.Close()
	if err := dlq.Write(ctx, model.Record{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Key: record.Key, Value: record.Value,
	}, errors.New("poison")); err != nil {
		t.Fatalf("write dlq message: %v", err)
	}

	dlqReader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers: []string{broker}, Topic: dlqTopic, Partition: 0, StartOffset: segmentio.FirstOffset,
	})
	defer dlqReader.Close()
	message, err := dlqReader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read dlq message: %v", err)
	}
	if string(message.Value) != "hello" {
		t.Fatalf("unexpected dlq value: %q", message.Value)
	}

	headers := make(map[string]string, len(message.Headers))
	for _, header := range message.Headers {
		headers[header.Key] = string(header.Value)
	}
	if headers["source-topic"] != topic || headers["failure"] != "poison" {
		t.Fatalf("unexpected dlq headers: %v", headers)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
