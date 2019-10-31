package mocktikv

import (
	"context"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

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
}

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

func (s *mppServer) CoprocessorStream(r *coprocessor.Request, copStream tikvpb.Tikv_CoprocessorStreamServer) error {
	return errors.New("unreachable")
}

func (c *mppServer) KvGet(ctx context.Context, in *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	out := new(kvrpcpb.GetResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvScan(ctx context.Context, in *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	out := new(kvrpcpb.ScanResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvPrewrite(ctx context.Context, in *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	out := new(kvrpcpb.PrewriteResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvPessimisticLock(ctx context.Context, in *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	out := new(kvrpcpb.PessimisticLockResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KVPessimisticRollback(ctx context.Context, in *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	out := new(kvrpcpb.PessimisticRollbackResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvTxnHeartBeat(ctx context.Context, in *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	out := new(kvrpcpb.TxnHeartBeatResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	out := new(kvrpcpb.CheckTxnStatusResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvCommit(ctx context.Context, in *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	out := new(kvrpcpb.CommitResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvImport(ctx context.Context, in *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	out := new(kvrpcpb.ImportResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvCleanup(ctx context.Context, in *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	out := new(kvrpcpb.CleanupResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvBatchGet(ctx context.Context, in *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	out := new(kvrpcpb.BatchGetResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvBatchRollback(ctx context.Context, in *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	out := new(kvrpcpb.BatchRollbackResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvScanLock(ctx context.Context, in *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	out := new(kvrpcpb.ScanLockResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvResolveLock(ctx context.Context, in *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	out := new(kvrpcpb.ResolveLockResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvGC(ctx context.Context, in *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	out := new(kvrpcpb.GCResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) KvDeleteRange(ctx context.Context, in *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	out := new(kvrpcpb.DeleteRangeResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawGet(ctx context.Context, in *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	out := new(kvrpcpb.RawGetResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawBatchGet(ctx context.Context, in *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	out := new(kvrpcpb.RawBatchGetResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawPut(ctx context.Context, in *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	out := new(kvrpcpb.RawPutResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawBatchPut(ctx context.Context, in *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	out := new(kvrpcpb.RawBatchPutResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawDelete(ctx context.Context, in *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	out := new(kvrpcpb.RawDeleteResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawBatchDelete(ctx context.Context, in *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	out := new(kvrpcpb.RawBatchDeleteResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawScan(ctx context.Context, in *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	out := new(kvrpcpb.RawScanResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawDeleteRange(ctx context.Context, in *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	out := new(kvrpcpb.RawDeleteRangeResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) RawBatchScan(ctx context.Context, in *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	out := new(kvrpcpb.RawBatchScanResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (c *mppServer) UnsafeDestroyRange(ctx context.Context, in *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	out := new(kvrpcpb.UnsafeDestroyRangeResponse)
	out.RegionError = &errorpb.Error{Message: *proto.String("not implement")}
	return out, nil
}

func (s *mppServer) Raft(tikvpb.Tikv_RaftServer) error {
	return errors.New("unreachable")
}
func (s *mppServer) BatchRaft(tikvpb.Tikv_BatchRaftServer) error {
	return errors.New("unreachable")
}
func (s *mppServer) Snapshot(tikvpb.Tikv_SnapshotServer) error {
	return errors.New("unreachable")
}
func (s *mppServer) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mppServer) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}
func (s *mppServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}

func (s *mppServer) BatchCommands(tikvpb.Tikv_BatchCommandsServer) error {
	return errors.New("unreachable")
}

func (s *mppServer) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}
