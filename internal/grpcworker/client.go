package grpcworker

import (
	"context"
	"encoding/json"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type Client struct {
	conn *grpc.ClientConn
}

func New(target string) (*Client, error) {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Process(ctx context.Context, record model.Record) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return c.conn.Invoke(
		ctx,
		"/consumerproxy.Worker/Process",
		wrapperspb.Bytes(payload),
		&emptypb.Empty{},
	)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
