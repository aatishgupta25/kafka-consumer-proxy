package kafkaadapter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/segmentio/kafka-go"
)

type DeadLetterWriter struct {
	writer *kafka.Writer
	topic  string
}

type deadLetterEnvelope struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value,omitempty"`
	Error     string `json:"error"`
	FailedAt  string `json:"failed_at"`
}

func NewDeadLetterWriter(brokers []string, topic string) *DeadLetterWriter {
	return &DeadLetterWriter{
		writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Balancer: &kafka.Hash{}},
		topic:  topic,
	}
}

func (w *DeadLetterWriter) Write(ctx context.Context, record model.Record, cause error) error {
	payload, err := json.Marshal(deadLetterEnvelope{
		Topic: record.Topic, Partition: record.Partition, Offset: record.Offset,
		Key: record.Key, Value: record.Value, Error: cause.Error(), FailedAt: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	return w.writer.WriteMessages(ctx, kafka.Message{Topic: w.topic, Key: record.Key, Value: payload})
}

func (w *DeadLetterWriter) Close() error {
	return w.writer.Close()
}
