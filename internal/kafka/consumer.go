package kafkaadapter

import (
	"context"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{reader: kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})}
}

func (c *Consumer) Fetch(ctx context.Context) (model.Record, error) {
	message, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return model.Record{}, err
	}
	return model.Record{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       message.Key,
		Value:     message.Value,
	}, nil
}

func (c *Consumer) CommitOffset(ctx context.Context, topic string, partition int, nextOffset int64) error {
	if nextOffset <= 0 {
		return nil
	}
	return c.reader.CommitMessages(ctx, kafka.Message{
		Topic:     topic,
		Partition: partition,
		Offset:    nextOffset - 1,
	})
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
