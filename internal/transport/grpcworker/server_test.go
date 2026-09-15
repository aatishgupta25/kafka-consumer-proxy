package grpcworker

import (
	"context"
	"net"
	"testing"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type captureWorker struct {
	record model.Record
}

func (w *captureWorker) Process(_ context.Context, record model.Record) error {
	w.record = record
	return nil
}

func TestClientServerRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	worker := &captureWorker{}
	RegisterServer(server, worker)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := &Client{conn: conn}
	want := model.Record{Topic: "events", Partition: 2, Offset: 42, Key: []byte("k"), Value: []byte("payload")}
	if err := client.Process(ctx, want); err != nil {
		t.Fatal(err)
	}

	if worker.record.Topic != want.Topic || worker.record.Partition != want.Partition || worker.record.Offset != want.Offset || string(worker.record.Key) != string(want.Key) || string(worker.record.Value) != string(want.Value) {
		t.Fatalf("received %#v, want %#v", worker.record, want)
	}
}
