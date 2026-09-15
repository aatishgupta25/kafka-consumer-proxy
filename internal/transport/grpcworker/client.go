package grpcworker

import (
	"context"
	"encoding/base64"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type Client struct {
	conn *grpc.ClientConn
}

func Dial(target string) (*Client, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Process(ctx context.Context, record model.Record) error {
	request, err := structpb.NewStruct(map[string]any{
		"topic":     record.Topic,
		"partition": float64(record.Partition),
		"offset":    float64(record.Offset),
		"key":       base64.StdEncoding.EncodeToString(record.Key),
		"value":     base64.StdEncoding.EncodeToString(record.Value),
	})
	if err != nil {
		return err
	}
	return c.conn.Invoke(ctx, "/worker.Worker/Process", request, &emptypb.Empty{})
}

func (c *Client) Close() error {
	return c.conn.Close()
}
