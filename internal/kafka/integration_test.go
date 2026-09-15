package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
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
	createTopics(t, broker, topic, dlqTopic)

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

func createTopics(t *testing.T, broker string, topics ...string) {
	t.Helper()
	conn, err := segmentio.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial Kafka: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("find Kafka controller: %v", err)
	}
	controllerConn, err := segmentio.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial Kafka controller: %v", err)
	}
	defer controllerConn.Close()

	configs := make([]segmentio.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, segmentio.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		t.Fatalf("create Kafka topics: %v", err)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
