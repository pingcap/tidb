package mocktikv

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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
type MockGrpcServer struct{}

// KvGet commands with mvcc/txn supported.
func (s *MockGrpcServer) KvGet(context.Context, *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	return nil, errors.New("unreachable")
}

// KvScan implements the TiKVServer interface.
func (s *MockGrpcServer) KvScan(context.Context, *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	return nil, errors.New("unreachable")
}

// KvPrewrite implements the TiKVServer interface.
func (s *MockGrpcServer) KvPrewrite(context.Context, *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	return nil, errors.New("unreachable")
}

// KvCommit implements the TiKVServer interface.
func (s *MockGrpcServer) KvCommit(context.Context, *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	return nil, errors.New("unreachable")
}

// KvImport implements the TiKVServer interface.
func (s *MockGrpcServer) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	return nil, errors.New("unreachable")
}

// KvCleanup implements the TiKVServer interface.
func (s *MockGrpcServer) KvCleanup(context.Context, *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	return nil, errors.New("unreachable")
}

// KvBatchGet implements the TiKVServer interface.
func (s *MockGrpcServer) KvBatchGet(context.Context, *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	return nil, errors.New("unreachable")
}

// KvBatchRollback implements the TiKVServer interface.
func (s *MockGrpcServer) KvBatchRollback(context.Context, *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	return nil, errors.New("unreachable")
}

// KvScanLock implements the TiKVServer interface.
func (s *MockGrpcServer) KvScanLock(context.Context, *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	return nil, errors.New("unreachable")
}

// KvResolveLock implements the TiKVServer interface.
func (s *MockGrpcServer) KvResolveLock(context.Context, *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	return nil, errors.New("unreachable")
}

// KvPessimisticLock implements the TiKVServer interface.
func (s *MockGrpcServer) KvPessimisticLock(context.Context, *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	return nil, errors.New("unreachable")
}

// KVPessimisticRollback implements the TiKVServer interface.
func (s *MockGrpcServer) KVPessimisticRollback(context.Context, *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	return nil, errors.New("unreachable")
}

// KvCheckTxnStatus implements the TiKVServer interface.
func (s *MockGrpcServer) KvCheckTxnStatus(ctx context.Context, in *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	return nil, errors.New("unreachable")
}

// KvTxnHeartBeat implements the TiKVServer interface.
func (s *MockGrpcServer) KvTxnHeartBeat(ctx context.Context, in *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	return nil, errors.New("unreachable")
}

// KvGC implements the TiKVServer interface.
func (s *MockGrpcServer) KvGC(context.Context, *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	return nil, errors.New("unreachable")
}

// KvDeleteRange implements the TiKVServer interface.
func (s *MockGrpcServer) KvDeleteRange(context.Context, *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}

// RawGet implements the TiKVServer interface.
func (s *MockGrpcServer) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return nil, errors.New("unreachable")
}

// RawBatchGet implements the TiKVServer interface.
func (s *MockGrpcServer) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return nil, errors.New("unreachable")
}

// RawPut implements the TiKVServer interface.
func (s *MockGrpcServer) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return nil, errors.New("unreachable")
}

// RawBatchPut implements the TiKVServer interface.
func (s *MockGrpcServer) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return nil, errors.New("unreachable")
}

// RawDelete implements the TiKVServer interface.
func (s *MockGrpcServer) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return nil, errors.New("unreachable")
}

// RawBatchDelete implements the TiKVServer interface.
func (s *MockGrpcServer) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return nil, errors.New("unreachable")
}

// RawScan implements the TiKVServer interface.
func (s *MockGrpcServer) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return nil, errors.New("unreachable")
}

// RawDeleteRange implements the TiKVServer interface.
func (s *MockGrpcServer) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return nil, errors.New("unreachable")
}

// RawBatchScan implements the TiKVServer interface.
func (s *MockGrpcServer) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return nil, errors.New("unreachable")
}

// UnsafeDestroyRange implements the TiKVServer interface.
func (s *MockGrpcServer) UnsafeDestroyRange(context.Context, *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	return nil, errors.New("unreachable")
}

// Coprocessor implements the TiKVServer interface.
func (s *MockGrpcServer) Coprocessor(context.Context, *coprocessor.Request) (*coprocessor.Response, error) {
	return nil, errors.New("unreachable")
}

// Raft implements the TiKVServer interface.
func (s *MockGrpcServer) Raft(tikvpb.Tikv_RaftServer) error {
	return errors.New("unreachable")
}

// BatchRaft implements the TiKVServer interface.
func (s *MockGrpcServer) BatchRaft(tikvpb.Tikv_BatchRaftServer) error {
	return errors.New("unreachable")
}

// Snapshot implements the TiKVServer interface.
func (s *MockGrpcServer) Snapshot(tikvpb.Tikv_SnapshotServer) error {
	return errors.New("unreachable")
}

// MvccGetByKey implements the TiKVServer interface.
func (s *MockGrpcServer) MvccGetByKey(context.Context, *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	return nil, errors.New("unreachable")
}

// MvccGetByStartTs implements the TiKVServer interface.
func (s *MockGrpcServer) MvccGetByStartTs(context.Context, *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	return nil, errors.New("unreachable")
}

// SplitRegion implements the TiKVServer interface.
func (s *MockGrpcServer) SplitRegion(context.Context, *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	return nil, errors.New("unreachable")
}

// CoprocessorStream implements the TiKVServer interface.
func (s *MockGrpcServer) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	return errors.New("unreachable")
}

// BatchCommands implements the TiKVServer interface.
func (s *MockGrpcServer) BatchCommands(tikvpb.Tikv_BatchCommandsServer) error {
	return errors.New("unreachable")
}

// ReadIndex implements the TiKVServer interface.
func (s *MockGrpcServer) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	return nil, errors.New("unreachable")
}
