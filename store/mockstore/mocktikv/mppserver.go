package mocktikv

import (
	"context"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// CreateRPCServer creates a rpc server for mpp.
func CreateRPCServer() *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in mpp server", zap.Any("stack", v))
		}
	}()

	s := grpc.NewServer()
	tikvpb.RegisterTikvServer(s, &mppServer{})
	return s
}

type mppServer struct {
	MockGrpcServer
}

// Coprocessor implements the TiKVServer interface.
func (c *mppServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (*coprocessor.Response, error) {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in mpp server coprocessor", zap.Any("stack", v))
		}
	}()
	handler := &rpcHandler{}
	res := handler.handleCopDAGRequest(in)
	return res, nil
}

// MockGrpcServer mock a TiKV gprc server for testing.
type MockGrpcServer struct {
	tikvpb.TikvServer
}
