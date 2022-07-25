// Copyright 2019-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	deadlockPb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/unistore/client"
	"github.com/pingcap/tidb/store/mockstore/unistore/cophandler"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/kverrors"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/pberror"
	"github.com/pingcap/tidb/store/mockstore/unistore/util/lockwaiter"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var _ tikvpb.TikvServer = new(Server)

// Server implements the tikvpb.TikvServer interface.
type Server struct {
	mvccStore     *MVCCStore
	regionManager RegionManager
	innerServer   InnerServer
	RPCClient     client.Client
	refCount      int32
	stopped       int32
}

// NewServer returns a new server.
func NewServer(rm RegionManager, store *MVCCStore, innerServer InnerServer) *Server {
	return &Server{
		mvccStore:     store,
		regionManager: rm,
		innerServer:   innerServer,
	}
}

// Stop stops the server.
func (svr *Server) Stop() {
	atomic.StoreInt32(&svr.stopped, 1)
	for {
		if atomic.LoadInt32(&svr.refCount) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

	if err := svr.mvccStore.Close(); err != nil {
		log.Error("close mvcc store failed", zap.Error(err))
	}
	if err := svr.regionManager.Close(); err != nil {
		log.Error("close region manager failed", zap.Error(err))
	}
	if err := svr.innerServer.Stop(); err != nil {
		log.Error("close inner server failed", zap.Error(err))
	}
}

// GetStoreIDByAddr gets a store id by the store address.
func (svr *Server) GetStoreIDByAddr(addr string) (uint64, error) {
	return svr.regionManager.GetStoreIDByAddr(addr)
}

// GetStoreAddrByStoreID gets a store address by the store id.
func (svr *Server) GetStoreAddrByStoreID(storeID uint64) (string, error) {
	return svr.regionManager.GetStoreAddrByStoreID(storeID)
}

type requestCtx struct {
	svr              *Server
	regCtx           RegionCtx
	regErr           *errorpb.Error
	buf              []byte
	reader           *dbreader.DBReader
	method           string
	startTime        time.Time
	rpcCtx           *kvrpcpb.Context
	storeAddr        string
	storeID          uint64
	asyncMinCommitTS uint64
	onePCCommitTS    uint64
}

func newRequestCtx(svr *Server, ctx *kvrpcpb.Context, method string) (*requestCtx, error) {
	atomic.AddInt32(&svr.refCount, 1)
	if atomic.LoadInt32(&svr.stopped) > 0 {
		atomic.AddInt32(&svr.refCount, -1)
		return nil, kverrors.ErrRetryable("server is closed")
	}
	req := &requestCtx{
		svr:       svr,
		method:    method,
		startTime: time.Now(),
		rpcCtx:    ctx,
	}
	req.regCtx, req.regErr = svr.regionManager.GetRegionFromCtx(ctx)
	storeAddr, storeID, regErr := svr.regionManager.GetStoreInfoFromCtx(ctx)
	req.storeAddr = storeAddr
	req.storeID = storeID
	if regErr != nil {
		req.regErr = regErr
	}
	return req, nil
}

// For read-only requests that doesn't acquire latches, this function must be called after all locks has been checked.
func (req *requestCtx) getDBReader() *dbreader.DBReader {
	if req.reader == nil {
		mvccStore := req.svr.mvccStore
		txn := mvccStore.db.NewTransaction(false)
		req.reader = dbreader.NewDBReader(req.regCtx.RawStart(), req.regCtx.RawEnd(), txn)
		req.reader.RcCheckTS = req.isRcCheckTSIsolationLevel()
	}
	return req.reader
}

func (req *requestCtx) isSnapshotIsolation() bool {
	return req.rpcCtx.IsolationLevel == kvrpcpb.IsolationLevel_SI
}

func (req *requestCtx) isRcCheckTSIsolationLevel() bool {
	return req.rpcCtx.IsolationLevel == kvrpcpb.IsolationLevel_RCCheckTS
}

func (req *requestCtx) finish() {
	atomic.AddInt32(&req.svr.refCount, -1)
	if req.reader != nil {
		req.reader.Close()
	}
}

// KvGet implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGet")
	if err != nil {
		return &kvrpcpb.GetResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.GetResponse{RegionError: reqCtx.regErr}, nil
	}
	val, err := svr.mvccStore.Get(reqCtx, req.Key, req.Version)
	return &kvrpcpb.GetResponse{
		Value: val,
		Error: convertToKeyError(err),
	}, nil
}

// KvScan implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScan")
	if err != nil {
		return &kvrpcpb.ScanResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanResponse{RegionError: reqCtx.regErr}, nil
	}
	pairs := svr.mvccStore.Scan(reqCtx, req)
	return &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}, nil
}

// KvPessimisticLock implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvPessimisticLock(ctx context.Context, req *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticLock")
	if err != nil {
		return &kvrpcpb.PessimisticLockResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticLockResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := &kvrpcpb.PessimisticLockResponse{}
	waiter, err := svr.mvccStore.PessimisticLock(reqCtx, req, resp)
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	if waiter == nil {
		return resp, nil
	}
	result := waiter.Wait()
	svr.mvccStore.DeadlockDetectCli.CleanUpWaitFor(req.StartVersion, waiter.LockTS, waiter.KeyHash)
	svr.mvccStore.lockWaiterManager.CleanUp(waiter)
	if result.WakeupSleepTime == lockwaiter.WaitTimeout {
		return resp, nil
	}
	if result.DeadlockResp != nil {
		log.Error("deadlock found", zap.Stringer("entry", &result.DeadlockResp.Entry))
		errLocked := err.(*kverrors.ErrLocked)
		deadlockErr := &kverrors.ErrDeadlock{
			LockKey:         errLocked.Key,
			LockTS:          errLocked.Lock.StartTS,
			DeadlockKeyHash: result.DeadlockResp.DeadlockKeyHash,
			WaitChain:       result.DeadlockResp.WaitChain,
		}
		resp.Errors, resp.RegionError = convertToPBErrors(deadlockErr)
		return resp, nil
	}
	if result.WakeupSleepTime == lockwaiter.WakeUpThisWaiter {
		if req.Force {
			req.WaitTimeout = lockwaiter.LockNoWait
			_, err := svr.mvccStore.PessimisticLock(reqCtx, req, resp)
			resp.Errors, resp.RegionError = convertToPBErrors(err)
			if err == nil {
				return resp, nil
			}
			if _, ok := err.(*kverrors.ErrLocked); !ok {
				resp.Errors, resp.RegionError = convertToPBErrors(err)
				return resp, nil
			}
			log.Warn("wakeup force lock request, try lock still failed", zap.Error(err))
		}
	}
	// The key is rollbacked, we don't have the exact commitTS, but we can use the server's latest.
	// Always use the store latest ts since the waiter result commitTs may not be the real conflict ts
	conflictCommitTS := svr.mvccStore.getLatestTS()
	err = &kverrors.ErrConflict{
		StartTS:          req.GetForUpdateTs(),
		ConflictTS:       waiter.LockTS,
		ConflictCommitTS: conflictCommitTS,
	}
	resp.Errors, _ = convertToPBErrors(err)
	return resp, nil
}

// KVPessimisticRollback implements implements the tikvpb.TikvServer interface.
func (svr *Server) KVPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "PessimisticRollback")
	if err != nil {
		return &kvrpcpb.PessimisticRollbackResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PessimisticRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.PessimisticRollback(reqCtx, req)
	resp := &kvrpcpb.PessimisticRollbackResponse{}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

// KvTxnHeartBeat implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvTxnHeartBeat(ctx context.Context, req *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "TxnHeartBeat")
	if err != nil {
		return &kvrpcpb.TxnHeartBeatResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.TxnHeartBeatResponse{RegionError: reqCtx.regErr}, nil
	}
	lockTTL, err := svr.mvccStore.TxnHeartBeat(reqCtx, req)
	resp := &kvrpcpb.TxnHeartBeatResponse{LockTtl: lockTTL}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

// KvCheckTxnStatus implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCheckTxnStatus")
	if err != nil {
		return &kvrpcpb.CheckTxnStatusResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CheckTxnStatusResponse{RegionError: reqCtx.regErr}, nil
	}
	txnStatus, err := svr.mvccStore.CheckTxnStatus(reqCtx, req)
	ttl := uint64(0)
	if txnStatus.lockInfo != nil {
		ttl = txnStatus.lockInfo.LockTtl
	}
	resp := &kvrpcpb.CheckTxnStatusResponse{
		LockTtl:       ttl,
		CommitVersion: txnStatus.commitTS,
		Action:        txnStatus.action,
		LockInfo:      txnStatus.lockInfo,
	}
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

// KvCheckSecondaryLocks implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvCheckSecondaryLocks(ctx context.Context, req *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCheckSecondaryLocks")
	if err != nil {
		return &kvrpcpb.CheckSecondaryLocksResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CheckSecondaryLocksResponse{RegionError: reqCtx.regErr}, nil
	}
	locksStatus, err := svr.mvccStore.CheckSecondaryLocks(reqCtx, req.Keys, req.StartVersion)
	resp := &kvrpcpb.CheckSecondaryLocksResponse{}
	if err == nil {
		resp.Locks = locksStatus.locks
		resp.CommitTs = locksStatus.commitTS
	} else {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

// KvPrewrite implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvPrewrite")
	if err != nil {
		return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{convertToKeyError(err)}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.PrewriteResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Prewrite(reqCtx, req)
	resp := &kvrpcpb.PrewriteResponse{}
	if reqCtx.asyncMinCommitTS > 0 {
		resp.MinCommitTs = reqCtx.asyncMinCommitTS
	}
	if reqCtx.onePCCommitTS > 0 {
		resp.OnePcCommitTs = reqCtx.onePCCommitTS
	}
	resp.Errors, resp.RegionError = convertToPBErrors(err)
	return resp, nil
}

// KvCommit implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCommit")
	if err != nil {
		return &kvrpcpb.CommitResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CommitResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.CommitResponse)
	err = svr.mvccStore.Commit(reqCtx, req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

// RawGetKeyTTL implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawGetKeyTTL(ctx context.Context, req *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
	// TODO
	return &kvrpcpb.RawGetKeyTTLResponse{}, nil
}

// KvImport implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvImport(context.Context, *kvrpcpb.ImportRequest) (*kvrpcpb.ImportResponse, error) {
	// TODO
	return &kvrpcpb.ImportResponse{}, nil
}

// KvCleanup implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvCleanup")
	if err != nil {
		return &kvrpcpb.CleanupResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.CleanupResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.Cleanup(reqCtx, req.Key, req.StartVersion, req.CurrentTs)
	resp := new(kvrpcpb.CleanupResponse)
	if committed, ok := err.(kverrors.ErrAlreadyCommitted); ok {
		resp.CommitVersion = uint64(committed)
	} else if err != nil {
		log.Error("cleanup failed", zap.Error(err))
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

// KvBatchGet implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchGet")
	if err != nil {
		return &kvrpcpb.BatchGetResponse{Pairs: []*kvrpcpb.KvPair{{Error: convertToKeyError(err)}}}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchGetResponse{RegionError: reqCtx.regErr}, nil
	}
	pairs := svr.mvccStore.BatchGet(reqCtx, req.Keys, req.GetVersion())
	return &kvrpcpb.BatchGetResponse{
		Pairs: pairs,
	}, nil
}

// KvBatchRollback implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvBatchRollback")
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.BatchRollbackResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.BatchRollbackResponse)
	err = svr.mvccStore.Rollback(reqCtx, req.Keys, req.StartVersion)
	resp.Error, resp.RegionError = convertToPBError(err)
	return resp, nil
}

// KvScanLock implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvScanLock")
	if err != nil {
		return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ScanLockResponse{RegionError: reqCtx.regErr}, nil
	}
	log.Debug("kv scan lock")
	locks, err := svr.mvccStore.ScanLock(reqCtx, req.MaxVersion, int(req.Limit))
	return &kvrpcpb.ScanLockResponse{Error: convertToKeyError(err), Locks: locks}, nil
}

// KvResolveLock implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvResolveLock")
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.ResolveLockResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := &kvrpcpb.ResolveLockResponse{}
	if len(req.TxnInfos) > 0 {
		for _, txnInfo := range req.TxnInfos {
			log.S().Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.Meta().Id, txnInfo.Txn)
			err := svr.mvccStore.ResolveLock(reqCtx, nil, txnInfo.Txn, txnInfo.Status)
			if err != nil {
				resp.Error, resp.RegionError = convertToPBError(err)
				break
			}
		}
	} else {
		log.S().Debugf("kv resolve lock region:%d txn:%v", reqCtx.regCtx.Meta().Id, req.StartVersion)
		err := svr.mvccStore.ResolveLock(reqCtx, req.Keys, req.StartVersion, req.CommitVersion)
		resp.Error, resp.RegionError = convertToPBError(err)
	}
	return resp, nil
}

// KvGC implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvGC")
	if err != nil {
		return &kvrpcpb.GCResponse{Error: convertToKeyError(err)}, nil
	}
	defer reqCtx.finish()
	svr.mvccStore.UpdateSafePoint(req.SafePoint)
	return &kvrpcpb.GCResponse{}, nil
}

// KvDeleteRange implements implements the tikvpb.TikvServer interface.
func (svr *Server) KvDeleteRange(ctx context.Context, req *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "KvDeleteRange")
	if err != nil {
		return &kvrpcpb.DeleteRangeResponse{Error: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.DeleteRangeResponse{RegionError: reqCtx.regErr}, nil
	}
	err = svr.mvccStore.dbWriter.DeleteRange(req.StartKey, req.EndKey, reqCtx.regCtx)
	if err != nil {
		log.Error("delete range failed", zap.Error(err))
	}
	return &kvrpcpb.DeleteRangeResponse{}, nil
}

// RawKV commands.

// RawGet implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawGet(context.Context, *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	return &kvrpcpb.RawGetResponse{}, nil
}

// RawPut implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawPut(context.Context, *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawDelete(context.Context, *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawScan(context.Context, *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	return &kvrpcpb.RawScanResponse{}, nil
}

// RawBatchDelete implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawBatchDelete(context.Context, *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	return &kvrpcpb.RawBatchDeleteResponse{}, nil
}

// RawBatchGet implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawBatchGet(context.Context, *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	return &kvrpcpb.RawBatchGetResponse{}, nil
}

// RawBatchPut implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawBatchPut(context.Context, *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	return &kvrpcpb.RawBatchPutResponse{}, nil
}

// RawBatchScan implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawBatchScan(context.Context, *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	return &kvrpcpb.RawBatchScanResponse{}, nil
}

// RawDeleteRange implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawDeleteRange(context.Context, *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	return &kvrpcpb.RawDeleteRangeResponse{}, nil
}

// SQL push down commands.

// Coprocessor implements implements the tikvpb.TikvServer interface.
func (svr *Server) Coprocessor(_ context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "Coprocessor")
	if err != nil {
		return &coprocessor.Response{OtherError: convertToKeyError(err).String()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &coprocessor.Response{RegionError: reqCtx.regErr}, nil
	}
	return cophandler.HandleCopRequest(reqCtx.getDBReader(), svr.mvccStore.lockStore, req), nil
}

// CoprocessorStream implements implements the tikvpb.TikvServer interface.
func (svr *Server) CoprocessorStream(*coprocessor.Request, tikvpb.Tikv_CoprocessorStreamServer) error {
	// TODO
	return nil
}

// RegionError represents a region error
type RegionError struct {
	err *errorpb.Error
}

// Error implements Error method.
func (regionError *RegionError) Error() string {
	return regionError.err.Message
}

// BatchCoprocessor implements implements the tikvpb.TikvServer interface.
func (svr *Server) BatchCoprocessor(req *coprocessor.BatchRequest, batchCopServer tikvpb.Tikv_BatchCoprocessorServer) error {
	reqCtxs := make([]*requestCtx, 0, len(req.Regions))
	defer func() {
		for _, ctx := range reqCtxs {
			ctx.finish()
		}
	}()
	if req.TableRegions != nil {
		// Support PartitionTableScan for BatchCop
		req.Regions = req.Regions[:]
		for _, tr := range req.TableRegions {
			req.Regions = append(req.Regions, tr.Regions...)
		}
	}
	for _, ri := range req.Regions {
		cop := coprocessor.Request{
			Tp:      kv.ReqTypeDAG,
			Data:    req.Data,
			StartTs: req.StartTs,
			Ranges:  ri.Ranges,
		}
		regionCtx := *req.Context
		regionCtx.RegionEpoch = ri.RegionEpoch
		regionCtx.RegionId = ri.RegionId
		cop.Context = &regionCtx

		reqCtx, err := newRequestCtx(svr, &regionCtx, "Coprocessor")
		if err != nil {
			return err
		}
		reqCtxs = append(reqCtxs, reqCtx)
		if reqCtx.regErr != nil {
			return &RegionError{err: reqCtx.regErr}
		}
		copResponse := cophandler.HandleCopRequestWithMPPCtx(reqCtx.getDBReader(), svr.mvccStore.lockStore, &cop, nil)
		err = batchCopServer.Send(&coprocessor.BatchResponse{Data: copResponse.Data})
		if err != nil {
			return err
		}
	}
	return nil
}

// RawCoprocessor implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawCoprocessor(context.Context, *kvrpcpb.RawCoprocessorRequest) (*kvrpcpb.RawCoprocessorResponse, error) {
	panic("unimplemented")
}

func (mrm *MockRegionManager) removeMPPTaskHandler(taskID int64, storeID uint64) error {
	set := mrm.getMPPTaskSet(storeID)
	if set == nil {
		return errors.New("cannot find mpp task set for store")
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	if _, ok := set.taskHandlers[taskID]; ok {
		delete(set.taskHandlers, taskID)
		return nil
	}
	return errors.New("cannot find mpp task")
}

// IsAlive implements the tikvpb.TikvServer interface.
func (svr *Server) IsAlive(_ context.Context, _ *mpp.IsAliveRequest) (*mpp.IsAliveResponse, error) {
	panic("todo")
}

// DispatchMPPTask implements the tikvpb.TikvServer interface.
func (svr *Server) DispatchMPPTask(_ context.Context, _ *mpp.DispatchTaskRequest) (*mpp.DispatchTaskResponse, error) {
	panic("todo")
}

func (svr *Server) executeMPPDispatch(ctx context.Context, req *mpp.DispatchTaskRequest, storeAddr string, storeID uint64, handler *cophandler.MPPTaskHandler) error {
	var reqCtx *requestCtx
	if len(req.TableRegions) > 0 {
		// Simple unistore logic for PartitionTableScan.
		for _, tr := range req.TableRegions {
			req.Regions = append(req.Regions, tr.Regions...)
		}
	}

	if len(req.Regions) > 0 {
		kvContext := &kvrpcpb.Context{
			RegionId:    req.Regions[0].RegionId,
			RegionEpoch: req.Regions[0].RegionEpoch,
			// this is a hack to reuse task id in kvContext to pass mpp task id
			TaskId: uint64(handler.Meta.TaskId),
			Peer:   &metapb.Peer{StoreId: storeID},
		}
		var err error
		reqCtx, err = newRequestCtx(svr, kvContext, "Mpp")
		if err != nil {
			return errors.Trace(err)
		}
	}
	copReq := &coprocessor.Request{
		Tp:      kv.ReqTypeDAG,
		Data:    req.EncodedPlan,
		StartTs: req.Meta.StartTs,
	}
	for _, regionMeta := range req.Regions {
		copReq.Ranges = append(copReq.Ranges, regionMeta.Ranges...)
	}
	var dbreader *dbreader.DBReader
	if reqCtx != nil {
		dbreader = reqCtx.getDBReader()
	}
	go func() {
		resp := cophandler.HandleCopRequestWithMPPCtx(dbreader, svr.mvccStore.lockStore, copReq, &cophandler.MPPCtx{
			RPCClient:   svr.RPCClient,
			StoreAddr:   storeAddr,
			TaskHandler: handler,
			Ctx:         ctx,
		})
		handler.Err = svr.RemoveMPPTaskHandler(req.Meta.TaskId, storeID)
		if len(resp.OtherError) > 0 {
			handler.Err = errors.New(resp.OtherError)
		}
		if reqCtx != nil {
			reqCtx.finish()
		}
	}()
	return nil
}

// DispatchMPPTaskWithStoreID implements implements the tikvpb.TikvServer interface.
func (svr *Server) DispatchMPPTaskWithStoreID(ctx context.Context, req *mpp.DispatchTaskRequest, storeID uint64) (*mpp.DispatchTaskResponse, error) {
	mppHandler, err := svr.CreateMPPTaskHandler(req.Meta, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storeAddr, err := svr.GetStoreAddrByStoreID(storeID)
	if err != nil {
		return nil, err
	}
	err = svr.executeMPPDispatch(ctx, req, storeAddr, storeID, mppHandler)
	resp := &mpp.DispatchTaskResponse{}
	if err != nil {
		resp.Error = &mpp.Error{Msg: err.Error()}
	}
	return resp, nil
}

// CancelMPPTask implements implements the tikvpb.TikvServer interface.
func (svr *Server) CancelMPPTask(_ context.Context, _ *mpp.CancelTaskRequest) (*mpp.CancelTaskResponse, error) {
	panic("todo")
}

// GetMPPTaskHandler implements implements the tikvpb.TikvServer interface.
func (svr *Server) GetMPPTaskHandler(taskID int64, storeID uint64) (*cophandler.MPPTaskHandler, error) {
	if mrm, ok := svr.regionManager.(*MockRegionManager); ok {
		set := mrm.getMPPTaskSet(storeID)
		if set == nil {
			return nil, errors.New("cannot find mpp task set for store")
		}
		set.mu.Lock()
		defer set.mu.Unlock()
		if handler, ok := set.taskHandlers[taskID]; ok {
			return handler, nil
		}
		return nil, nil
	}
	return nil, errors.New("Only mock region mgr supports get mpp task")
}

// RemoveMPPTaskHandler implements implements the tikvpb.TikvServer interface.
func (svr *Server) RemoveMPPTaskHandler(taskID int64, storeID uint64) error {
	if mrm, ok := svr.regionManager.(*MockRegionManager); ok {
		err := mrm.removeMPPTaskHandler(taskID, storeID)
		return errors.Trace(err)
	}
	return errors.New("Only mock region mgr supports remove mpp task")
}

// CreateMPPTaskHandler implements implements the tikvpb.TikvServer interface.
func (svr *Server) CreateMPPTaskHandler(meta *mpp.TaskMeta, storeID uint64) (*cophandler.MPPTaskHandler, error) {
	if mrm, ok := svr.regionManager.(*MockRegionManager); ok {
		set := mrm.getMPPTaskSet(storeID)
		if set == nil {
			return nil, errors.New("cannot find mpp task set for store")
		}
		set.mu.Lock()
		defer set.mu.Unlock()
		if handler, ok := set.taskHandlers[meta.TaskId]; ok {
			return handler, errors.Errorf("Task %d has been created", meta.TaskId)
		}
		handler := &cophandler.MPPTaskHandler{
			TunnelSet: make(map[int64]*cophandler.ExchangerTunnel),
			Meta:      meta,
			RPCClient: svr.RPCClient,
		}
		set.taskHandlers[meta.TaskId] = handler
		return handler, nil
	}
	return nil, errors.New("Only mock region mgr supports get mpp task")
}

// EstablishMPPConnection implements implements the tikvpb.TikvServer interface.
func (svr *Server) EstablishMPPConnection(*mpp.EstablishMPPConnectionRequest, tikvpb.Tikv_EstablishMPPConnectionServer) error {
	panic("todo")
}

// EstablishMPPConnectionWithStoreID implements implements the tikvpb.TikvServer interface.
func (svr *Server) EstablishMPPConnectionWithStoreID(req *mpp.EstablishMPPConnectionRequest, server tikvpb.Tikv_EstablishMPPConnectionServer, storeID uint64) error {
	var (
		mppHandler *cophandler.MPPTaskHandler
		err        error
	)
	maxRetryTime := 5
	for i := 0; i < maxRetryTime; i++ {
		mppHandler, err = svr.GetMPPTaskHandler(req.SenderMeta.TaskId, storeID)
		if err != nil {
			return errors.Trace(err)
		}
		if mppHandler == nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if mppHandler == nil {
		return errors.New("task not found")
	}
	ctx1, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel, err := mppHandler.HandleEstablishConn(ctx1, req)
	if err != nil {
		return errors.Trace(err)
	}
	var sendError error
	for sendError == nil {
		chunk, err := tunnel.RecvChunk()
		if err != nil {
			sendError = server.Send(&mpp.MPPDataPacket{Error: &mpp.Error{Msg: err.Error()}})
			break
		}
		if chunk == nil {
			// todo return io.EOF error?
			break
		}
		res := tipb.SelectResponse{
			Chunks: []tipb.Chunk{*chunk},
		}
		raw, err := res.Marshal()
		if err != nil {
			sendError = server.Send(&mpp.MPPDataPacket{Error: &mpp.Error{Msg: err.Error()}})
			break
		}
		sendError = server.Send(&mpp.MPPDataPacket{Data: raw})
	}
	return sendError
}

// Raft commands (tikv <-> tikv).

// Raft implements implements the tikvpb.TikvServer interface.
func (svr *Server) Raft(stream tikvpb.Tikv_RaftServer) error {
	return svr.innerServer.Raft(stream)
}

// Snapshot implements implements the tikvpb.TikvServer interface.
func (svr *Server) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return svr.innerServer.Snapshot(stream)
}

// BatchRaft implements implements the tikvpb.TikvServer interface.
func (svr *Server) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return svr.innerServer.BatchRaft(stream)
}

// Region commands.

// SplitRegion implements implements the tikvpb.TikvServer interface.
func (svr *Server) SplitRegion(ctx context.Context, req *kvrpcpb.SplitRegionRequest) (*kvrpcpb.SplitRegionResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "SplitRegion")
	if err != nil {
		return &kvrpcpb.SplitRegionResponse{RegionError: &errorpb.Error{Message: err.Error()}}, nil
	}
	defer reqCtx.finish()
	return svr.regionManager.SplitRegion(req), nil
}

// Compact implements the tikvpb.TikvServer interface.
func (svr *Server) Compact(ctx context.Context, req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
	panic("unimplemented")
}

// ReadIndex implements implements the tikvpb.TikvServer interface.
func (svr *Server) ReadIndex(context.Context, *kvrpcpb.ReadIndexRequest) (*kvrpcpb.ReadIndexResponse, error) {
	// TODO:
	return &kvrpcpb.ReadIndexResponse{}, nil
}

// transaction debugger commands.

// MvccGetByKey implements implements the tikvpb.TikvServer interface.
func (svr *Server) MvccGetByKey(ctx context.Context, req *kvrpcpb.MvccGetByKeyRequest) (*kvrpcpb.MvccGetByKeyResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByKey")
	if err != nil {
		return &kvrpcpb.MvccGetByKeyResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByKeyResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByKeyResponse)
	mvccInfo, err := svr.mvccStore.MvccGetByKey(reqCtx, req.GetKey())
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	return resp, nil
}

// MvccGetByStartTs implements implements the tikvpb.TikvServer interface.
func (svr *Server) MvccGetByStartTs(ctx context.Context, req *kvrpcpb.MvccGetByStartTsRequest) (*kvrpcpb.MvccGetByStartTsResponse, error) {
	reqCtx, err := newRequestCtx(svr, req.Context, "MvccGetByStartTs")
	if err != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{Error: err.Error()}, nil
	}
	defer reqCtx.finish()
	if reqCtx.regErr != nil {
		return &kvrpcpb.MvccGetByStartTsResponse{RegionError: reqCtx.regErr}, nil
	}
	resp := new(kvrpcpb.MvccGetByStartTsResponse)
	mvccInfo, key, err := svr.mvccStore.MvccGetByStartTs(reqCtx, req.StartTs)
	if err != nil {
		resp.Error = err.Error()
	}
	resp.Info = mvccInfo
	resp.Key = key
	return resp, nil
}

// UnsafeDestroyRange implements implements the tikvpb.TikvServer interface.
func (svr *Server) UnsafeDestroyRange(ctx context.Context, req *kvrpcpb.UnsafeDestroyRangeRequest) (*kvrpcpb.UnsafeDestroyRangeResponse, error) {
	start, end := req.GetStartKey(), req.GetEndKey()
	svr.mvccStore.DeleteFileInRange(start, end)
	return &kvrpcpb.UnsafeDestroyRangeResponse{}, nil
}

// GetWaitForEntries tries to get the waitFor entries
// deadlock detection related services
func (svr *Server) GetWaitForEntries(ctx context.Context,
	req *deadlockPb.WaitForEntriesRequest) (*deadlockPb.WaitForEntriesResponse, error) {
	// TODO
	return &deadlockPb.WaitForEntriesResponse{}, nil
}

// Detect will handle detection rpc from other nodes
func (svr *Server) Detect(stream deadlockPb.Deadlock_DetectServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !svr.mvccStore.DeadlockDetectSvr.IsLeader() {
			log.Warn("detection requests received on non leader node")
			break
		}
		resp := svr.mvccStore.DeadlockDetectSvr.Detect(req)
		if resp != nil {
			if sendErr := stream.Send(resp); sendErr != nil {
				log.Error("send deadlock response failed", zap.Error(sendErr))
				break
			}
		}
	}
	return nil
}

// CheckLockObserver implements implements the tikvpb.TikvServer interface.
func (svr *Server) CheckLockObserver(context.Context, *kvrpcpb.CheckLockObserverRequest) (*kvrpcpb.CheckLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.CheckLockObserverResponse{IsClean: true}, nil
}

// PhysicalScanLock implements implements the tikvpb.TikvServer interface.
func (svr *Server) PhysicalScanLock(ctx context.Context, req *kvrpcpb.PhysicalScanLockRequest) (*kvrpcpb.PhysicalScanLockResponse, error) {
	resp := &kvrpcpb.PhysicalScanLockResponse{}
	resp.Locks = svr.mvccStore.PhysicalScanLock(req.StartKey, req.MaxTs, int(req.Limit))
	return resp, nil
}

// RegisterLockObserver implements implements the tikvpb.TikvServer interface.
func (svr *Server) RegisterLockObserver(context.Context, *kvrpcpb.RegisterLockObserverRequest) (*kvrpcpb.RegisterLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.RegisterLockObserverResponse{}, nil
}

// RemoveLockObserver implements implements the tikvpb.TikvServer interface.
func (svr *Server) RemoveLockObserver(context.Context, *kvrpcpb.RemoveLockObserverRequest) (*kvrpcpb.RemoveLockObserverResponse, error) {
	// TODO: implement Observer
	return &kvrpcpb.RemoveLockObserverResponse{}, nil
}

// CheckLeader implements implements the tikvpb.TikvServer interface.
func (svr *Server) CheckLeader(context.Context, *kvrpcpb.CheckLeaderRequest) (*kvrpcpb.CheckLeaderResponse, error) {
	panic("unimplemented")
}

// RawCompareAndSwap implements the tikvpb.TikvServer interface.
func (svr *Server) RawCompareAndSwap(context.Context, *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
	panic("implement me")
}

// GetStoreSafeTS implements the tikvpb.TikvServer interface.
func (svr *Server) GetStoreSafeTS(context.Context, *kvrpcpb.StoreSafeTSRequest) (*kvrpcpb.StoreSafeTSResponse, error) {
	return &kvrpcpb.StoreSafeTSResponse{}, nil
}

// GetLockWaitInfo implements the tikvpb.TikvServer interface.
func (svr *Server) GetLockWaitInfo(context.Context, *kvrpcpb.GetLockWaitInfoRequest) (*kvrpcpb.GetLockWaitInfoResponse, error) {
	panic("unimplemented")
}

// RawChecksum implements implements the tikvpb.TikvServer interface.
func (svr *Server) RawChecksum(context.Context, *kvrpcpb.RawChecksumRequest) (*kvrpcpb.RawChecksumResponse, error) {
	panic("unimplemented")
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}
	causeErr := errors.Cause(err)
	switch x := causeErr.(type) {
	case *kverrors.ErrLocked:
		return &kvrpcpb.KeyError{
			Locked: x.Lock.ToLockInfo(x.Key),
		}
	case kverrors.ErrRetryable:
		return &kvrpcpb.KeyError{
			Retryable: x.Error(),
		}
	case *kverrors.ErrKeyAlreadyExists:
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: x.Key,
			},
		}
	case *kverrors.ErrConflict:
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:          x.StartTS,
				ConflictTs:       x.ConflictTS,
				ConflictCommitTs: x.ConflictCommitTS,
				Key:              x.Key,
			},
		}
	case *kverrors.ErrDeadlock:
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockKey:         x.LockKey,
				LockTs:          x.LockTS,
				DeadlockKeyHash: x.DeadlockKeyHash,
				WaitChain:       x.WaitChain,
			},
		}
	case *kverrors.ErrCommitExpire:
		return &kvrpcpb.KeyError{
			CommitTsExpired: &kvrpcpb.CommitTsExpired{
				StartTs:           x.StartTs,
				AttemptedCommitTs: x.CommitTs,
				Key:               x.Key,
				MinCommitTs:       x.MinCommitTs,
			},
		}
	case *kverrors.ErrTxnNotFound:
		return &kvrpcpb.KeyError{
			TxnNotFound: &kvrpcpb.TxnNotFound{
				StartTs:    x.StartTS,
				PrimaryKey: x.PrimaryKey,
			},
		}
	case *kverrors.ErrAssertionFailed:
		return &kvrpcpb.KeyError{
			AssertionFailed: &kvrpcpb.AssertionFailed{
				StartTs:          x.StartTS,
				Key:              x.Key,
				Assertion:        x.Assertion,
				ExistingStartTs:  x.ExistingStartTS,
				ExistingCommitTs: x.ExistingCommitTS,
			},
		}
	default:
		return &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
	}
}

func convertToPBError(err error) (*kvrpcpb.KeyError, *errorpb.Error) {
	if regErr := extractRegionError(err); regErr != nil {
		return nil, regErr
	}
	return convertToKeyError(err), nil
}

func convertToPBErrors(err error) ([]*kvrpcpb.KeyError, *errorpb.Error) {
	if err != nil {
		if regErr := extractRegionError(err); regErr != nil {
			return nil, regErr
		}
		return []*kvrpcpb.KeyError{convertToKeyError(err)}, nil
	}
	return nil, nil
}

func extractRegionError(err error) *errorpb.Error {
	if pbError, ok := err.(*pberror.PBError); ok {
		return pbError.RequestErr
	}
	return nil
}
