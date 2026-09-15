package grpcworker

import (
	"context"
	"encoding/base64"

	"github.com/aatishgupta25/kafka-consumer-proxy/internal/model"
	"github.com/aatishgupta25/kafka-consumer-proxy/internal/ports"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type workerService interface {
	Process(context.Context, *structpb.Struct) (*emptypb.Empty, error)
}

type server struct {
	worker ports.Worker
}

func RegisterServer(s *grpc.Server, worker ports.Worker) {
	s.RegisterService(&serviceDesc, &server{worker: worker})
}

func (s *server) Process(ctx context.Context, req *structpb.Struct) (*emptypb.Empty, error) {
	fields := req.GetFields()
	key, err := base64.StdEncoding.DecodeString(fields["key"].GetStringValue())
	if err != nil {
		return nil, err
	}
	value, err := base64.StdEncoding.DecodeString(fields["value"].GetStringValue())
	if err != nil {
		return nil, err
	}
	record := model.Record{
		Topic: fields["topic"].GetStringValue(),
		Partition: int(fields["partition"].GetNumberValue()),
		Offset: int64(fields["offset"].GetNumberValue()),
		Key: key,
		Value: value,
	}
	if err := s.worker.Process(ctx, record); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func processHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	request := new(structpb.Struct)
	if err := dec(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(workerService).Process(ctx, request)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/worker.Worker/Process"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(workerService).Process(ctx, req.(*structpb.Struct))
	}
	return interceptor(ctx, request, info, handler)
}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: "worker.Worker",
	HandlerType: (*workerService)(nil),
	Methods: []grpc.MethodDesc{{MethodName: "Process", Handler: processHandler}},
}
