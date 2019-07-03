package tikv

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type server struct{}

func (s *server) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvPessimisticLock(context.Context, *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KVPessimisticRollback(context.Context, *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	return errors.New("unimplemented in the mock service")
}

func (s *server) Raft(tikvpb.Tikv_RaftServer) error {
	return errors.New("unimplemented in the mock service")
}

func (s *server) BatchRaft(tikvpb.Tikv_BatchRaftServer) error {
	return errors.New("unimplemented in the mock service")
}

func (s *server) Snapshot(tikvpb.Tikv_SnapshotServer) error {
	return errors.New("unimplemented in the mock service")
}

func (s *server) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
}

func (s *server) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unimplemented in the mock service")
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
				Cmd: &tikvpb.BatchCommandsResponse_Response_Get{
					Get: &kvrpcpb.GetResponse{
						Value: []byte{'a', 'b', 'c'},
					},
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
	return nil
}

// Try to start a gRPC server and retrun the binded port.
func startMockTikvService() int {
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
		return port
	}
	logutil.BgLogger().Error("can't start mock tikv service because no available ports")
	return -1
}
