package tikv

import (
	"fmt"
	"net"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type server struct {
	tikvpb.TikvServer
}

func (s *server) BatchCommands(ss tikvpb.Tikv_BatchCommandsServer) error {
	for {
		req, err := ss.Recv()
		if err != nil {
			logutil.BgLogger().Error("batch commands receive fail", zap.Error(err))
			return err
		}

		responses := make([]*tikvpb.BatchCommandsResponse_Response, 0, len(req.GetRequestIds()))
		for i := 0; i < len(req.GetRequestIds()); i++ {
			responses = append(responses, &tikvpb.BatchCommandsResponse_Response{
				Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{
					Empty: &tikvpb.BatchCommandsEmptyResponse{},
				},
			})
		}

		err = ss.Send(&tikvpb.BatchCommandsResponse{
			Responses:  responses,
			RequestIds: req.GetRequestIds(),
		})
		if err != nil {
			logutil.BgLogger().Error("batch commands send fail", zap.Error(err))
			return err
		}
	}
}

// Try to start a gRPC server and retrun the server instance and binded port.
func startMockTikvService() (*grpc.Server, int) {
	for port := 40000; port < 50000; port++ {
		lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", "127.0.0.1", port))
		if err != nil {
			logutil.BgLogger().Error("can't listen", zap.Error(err))
			continue
		}
		s := grpc.NewServer(grpc.ConnectionTimeout(time.Minute))
		tikvpb.RegisterTikvServer(s, &server{})
		go func() {
			if err = s.Serve(lis); err != nil {
				logutil.BgLogger().Error(
					"can't serve gRPC requests",
					zap.Error(err),
				)
			}
		}()
		return s, port
	}
	logutil.BgLogger().Error("can't start mock tikv service because no available ports")
	return nil, -1
}
