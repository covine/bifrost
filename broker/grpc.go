package broker

import (
	"context"

	"github.com/covine/bifrost/pb"
)

type grpcServer struct {
	broker *Broker
}

func newGrpcServer(b *Broker) (*grpcServer, error) {
	return &grpcServer{broker: b}, nil
}

func (g *grpcServer) Ping(_ context.Context, _ *proto.PingReq) (*proto.PingResp, error) {
	return &proto.PingResp{}, nil
}
