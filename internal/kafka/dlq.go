package kafka

import (
	"context"
	"strconv"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	segmentio "github.com/segmentio/kafka-go"
)

type DLQWriter struct {
	writer *segmentio.Writer
	topic  string
}

func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	return &DLQWriter{
		writer: &segmentio.Writer{Addr: segmentio.TCP(brokers...), Balancer: &segmentio.LeastBytes{}},
		topic:  topic,
	}
}

func (w *DLQWriter) Write(ctx context.Context, record model.Record, cause error) error {
	return w.writer.WriteMessages(ctx, segmentio.Message{
		Topic: w.topic,
		Key:   record.Key,
		Value: record.Value,
		Headers: []segmentio.Header{
			{Key: "source-topic", Value: []byte(record.Topic)},
			{Key: "source-partition", Value: []byte(strconv.Itoa(record.Partition))},
			{Key: "source-offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
			{Key: "failure", Value: []byte(cause.Error())},
		},
	})
}

func (w *DLQWriter) Close() error {
	return w.writer.Close()
}
