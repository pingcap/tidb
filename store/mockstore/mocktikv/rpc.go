// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tipb/go-tipb"
)

// For gofail injection.
var undeterminedErr = terror.ErrResultUndetermined

const requestMaxSize = 8 * 1024 * 1024

func checkGoContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if locked, ok := errors.Cause(err).(*ErrLocked); ok {
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:             locked.Key.Raw(),
				PrimaryLock:     locked.Primary,
				LockVersion:     locked.StartTS,
				LockTtl:         locked.TTL,
				TxnSize:         locked.TxnSize,
				LockType:        locked.LockType,
				LockForUpdateTs: locked.ForUpdateTS,
			},
		}
	}
	if alreadyExist, ok := errors.Cause(err).(*ErrKeyAlreadyExist); ok {
		return &kvrpcpb.KeyError{
			AlreadyExist: &kvrpcpb.AlreadyExist{
				Key: alreadyExist.Key,
			},
		}
	}
	if writeConflict, ok := errors.Cause(err).(*ErrConflict); ok {
		return &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				Key:              writeConflict.Key,
				ConflictTs:       writeConflict.ConflictTS,
				ConflictCommitTs: writeConflict.ConflictCommitTS,
				StartTs:          writeConflict.StartTS,
			},
		}
	}
	if dead, ok := errors.Cause(err).(*ErrDeadlock); ok {
		return &kvrpcpb.KeyError{
			Deadlock: &kvrpcpb.Deadlock{
				LockTs:          dead.LockTS,
				LockKey:         dead.LockKey,
				DeadlockKeyHash: dead.DealockKeyHash,
			},
		}
	}
	if retryable, ok := errors.Cause(err).(ErrRetryable); ok {
		return &kvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	if expired, ok := errors.Cause(err).(*ErrCommitTSExpired); ok {
		return &kvrpcpb.KeyError{
			CommitTsExpired: &expired.CommitTsExpired,
		}
	}
	if tmp, ok := errors.Cause(err).(*ErrTxnNotFound); ok {
		return &kvrpcpb.KeyError{
			TxnNotFound: &tmp.TxnNotFound,
		}
	}
	return &kvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*kvrpcpb.KeyError {
	var keyErrors = make([]*kvrpcpb.KeyError, 0)
	for _, err := range errs {
		if err != nil {
			keyErrors = append(keyErrors, convertToKeyError(err))
		}
	}
	return keyErrors
}

func convertToPbPairs(pairs []Pair) []*kvrpcpb.KvPair {
	kvPairs := make([]*kvrpcpb.KvPair, 0, len(pairs))
	for _, p := range pairs {
		var kvPair *kvrpcpb.KvPair
		if p.Err == nil {
			kvPair = &kvrpcpb.KvPair{
				Key:   p.Key,
				Value: p.Value,
			}
		} else {
			kvPair = &kvrpcpb.KvPair{
				Error: convertToKeyError(p.Err),
			}
		}
		kvPairs = append(kvPairs, kvPair)
	}
	return kvPairs
}

// kvHandler mocks tikv's side handler behavior. In general, you may assume
// TiKV just translate the logic from Go to Rust.
type kvHandler struct {
	*Session
}

func (h kvHandler) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvGet: key not in region")
	}

	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (h kvHandler) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	endKey := MvccKey(h.endKey).Raw()
	var pairs []Pair
	if !req.Reverse {
		if !h.checkKeyInRegion(req.GetStartKey()) {
			panic("KvScan: startKey not in region")
		}
		if len(req.EndKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.EndKey), h.endKey) < 0) {
			endKey = req.EndKey
		}
		pairs = h.mvccStore.Scan(req.GetStartKey(), endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	} else {
		// TiKV use range [end_key, start_key) for reverse scan.
		// Should use the req.EndKey to check in region.
		if !h.checkKeyInRegion(req.GetEndKey()) {
			panic("KvScan: startKey not in region")
		}

		// TiKV use range [end_key, start_key) for reverse scan.
		// So the req.StartKey actually is the end_key.
		if len(req.StartKey) > 0 && (len(endKey) == 0 || bytes.Compare(NewMvccKey(req.StartKey), h.endKey) < 0) {
			endKey = req.StartKey
		}

		pairs = h.mvccStore.ReverseScan(req.EndKey, endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel, req.Context.ResolvedLocks)
	}

	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h kvHandler) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	regionID := req.Context.RegionId
	h.cluster.handleDelay(req.StartVersion, regionID)

	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPrewrite: key not in region")
		}
	}
	errs := h.mvccStore.Prewrite(req)
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h kvHandler) handleKvPessimisticLock(req *kvrpcpb.PessimisticLockRequest) *kvrpcpb.PessimisticLockResponse {
	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPessimisticLock: key not in region")
		}
	}
	startTS := req.StartVersion
	regionID := req.Context.RegionId
	h.cluster.handleDelay(startTS, regionID)
	return h.mvccStore.PessimisticLock(req)
}

func simulateServerSideWaitLock(errs []error) {
	for _, err := range errs {
		if _, ok := err.(*ErrLocked); ok {
			time.Sleep(time.Millisecond * 5)
			break
		}
	}
}

func (h kvHandler) handleKvPessimisticRollback(req *kvrpcpb.PessimisticRollbackRequest) *kvrpcpb.PessimisticRollbackResponse {
	for _, key := range req.Keys {
		if !h.checkKeyInRegion(key) {
			panic("KvPessimisticRollback: key not in region")
		}
	}
	errs := h.mvccStore.PessimisticRollback(req.Keys, req.StartVersion, req.ForUpdateTs)
	return &kvrpcpb.PessimisticRollbackResponse{
		Errors: convertToKeyErrors(errs),
	}
}

func (h kvHandler) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvCommit: key not in region")
		}
	}
	var resp kvrpcpb.CommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h kvHandler) handleKvCleanup(req *kvrpcpb.CleanupRequest) *kvrpcpb.CleanupResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvCleanup: key not in region")
	}
	var resp kvrpcpb.CleanupResponse
	err := h.mvccStore.Cleanup(req.Key, req.GetStartVersion(), req.GetCurrentTs())
	if err != nil {
		if commitTS, ok := errors.Cause(err).(ErrAlreadyCommitted); ok {
			resp.CommitVersion = uint64(commitTS)
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (h kvHandler) handleKvCheckTxnStatus(req *kvrpcpb.CheckTxnStatusRequest) *kvrpcpb.CheckTxnStatusResponse {
	if !h.checkKeyInRegion(req.PrimaryKey) {
		panic("KvCheckTxnStatus: key not in region")
	}
	var resp kvrpcpb.CheckTxnStatusResponse
	ttl, commitTS, action, err := h.mvccStore.CheckTxnStatus(req.GetPrimaryKey(), req.GetLockTs(), req.GetCallerStartTs(), req.GetCurrentTs(), req.GetRollbackIfNotExist(), req.ResolvingPessimisticLock)
	if err != nil {
		resp.Error = convertToKeyError(err)
	} else {
		resp.LockTtl, resp.CommitVersion, resp.Action = ttl, commitTS, action
	}
	return &resp
}

func (h kvHandler) handleTxnHeartBeat(req *kvrpcpb.TxnHeartBeatRequest) *kvrpcpb.TxnHeartBeatResponse {
	if !h.checkKeyInRegion(req.PrimaryLock) {
		panic("KvTxnHeartBeat: key not in region")
	}
	var resp kvrpcpb.TxnHeartBeatResponse
	ttl, err := h.mvccStore.TxnHeartBeat(req.PrimaryLock, req.StartVersion, req.AdviseLockTtl)
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	resp.LockTtl = ttl
	return &resp
}

func (h kvHandler) handleKvBatchGet(req *kvrpcpb.BatchGetRequest) *kvrpcpb.BatchGetResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvBatchGet: key not in region")
		}
	}
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion(), h.isolationLevel, req.Context.GetResolvedLocks())
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h kvHandler) handleMvccGetByKey(req *kvrpcpb.MvccGetByKeyRequest) *kvrpcpb.MvccGetByKeyResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &kvrpcpb.MvccGetByKeyResponse{
			Error: "not implement",
		}
	}

	if !h.checkKeyInRegion(req.Key) {
		panic("MvccGetByKey: key not in region")
	}
	var resp kvrpcpb.MvccGetByKeyResponse
	resp.Info = debugger.MvccGetByKey(req.Key)
	return &resp
}

func (h kvHandler) handleMvccGetByStartTS(req *kvrpcpb.MvccGetByStartTsRequest) *kvrpcpb.MvccGetByStartTsResponse {
	debugger, ok := h.mvccStore.(MVCCDebugger)
	if !ok {
		return &kvrpcpb.MvccGetByStartTsResponse{
			Error: "not implement",
		}
	}
	var resp kvrpcpb.MvccGetByStartTsResponse
	resp.Info, resp.Key = debugger.MvccGetByStartTS(req.StartTs)
	return &resp
}

func (h kvHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h kvHandler) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	locks, err := h.mvccStore.ScanLock(startKey, endKey, req.GetMaxVersion())
	if err != nil {
		return &kvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (h kvHandler) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.ResolveLock(startKey, endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
}

func (h kvHandler) handleKvGC(req *kvrpcpb.GCRequest) *kvrpcpb.GCResponse {
	startKey := MvccKey(h.startKey).Raw()
	endKey := MvccKey(h.endKey).Raw()
	err := h.mvccStore.GC(startKey, endKey, req.GetSafePoint())
	if err != nil {
		return &kvrpcpb.GCResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GCResponse{}
}

func (h kvHandler) handleKvDeleteRange(req *kvrpcpb.DeleteRangeRequest) *kvrpcpb.DeleteRangeResponse {
	if !h.checkKeyInRegion(req.StartKey) {
		panic("KvDeleteRange: key not in region")
	}
	var resp kvrpcpb.DeleteRangeResponse
	err := h.mvccStore.DeleteRange(req.StartKey, req.EndKey)
	if err != nil {
		resp.Error = err.Error()
	}
	return &resp
}

func (h kvHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawGetResponse{
			Error: "not implemented",
		}
	}
	return &kvrpcpb.RawGetResponse{
		Value: rawKV.RawGet(req.GetKey()),
	}
}

func (h kvHandler) handleKvRawBatchGet(req *kvrpcpb.RawBatchGetRequest) *kvrpcpb.RawBatchGetResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		// TODO should we add error ?
		return &kvrpcpb.RawBatchGetResponse{
			RegionError: &errorpb.Error{
				Message: "not implemented",
			},
		}
	}
	values := rawKV.RawBatchGet(req.Keys)
	kvPairs := make([]*kvrpcpb.KvPair, len(values))
	for i, key := range req.Keys {
		kvPairs[i] = &kvrpcpb.KvPair{
			Key:   key,
			Value: values[i],
		}
	}
	return &kvrpcpb.RawBatchGetResponse{
		Pairs: kvPairs,
	}
}

func (h kvHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawPutResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h kvHandler) handleKvRawBatchPut(req *kvrpcpb.RawBatchPutRequest) *kvrpcpb.RawBatchPutResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawBatchPutResponse{
			Error: "not implemented",
		}
	}
	keys := make([][]byte, 0, len(req.Pairs))
	values := make([][]byte, 0, len(req.Pairs))
	for _, pair := range req.Pairs {
		keys = append(keys, pair.Key)
		values = append(values, pair.Value)
	}
	rawKV.RawBatchPut(keys, values)
	return &kvrpcpb.RawBatchPutResponse{}
}

func (h kvHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (h kvHandler) handleKvRawBatchDelete(req *kvrpcpb.RawBatchDeleteRequest) *kvrpcpb.RawBatchDeleteResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawBatchDeleteResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawBatchDelete(req.Keys)
	return &kvrpcpb.RawBatchDeleteResponse{}
}

func (h kvHandler) handleKvRawDeleteRange(req *kvrpcpb.RawDeleteRangeRequest) *kvrpcpb.RawDeleteRangeResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		return &kvrpcpb.RawDeleteRangeResponse{
			Error: "not implemented",
		}
	}
	rawKV.RawDeleteRange(req.GetStartKey(), req.GetEndKey())
	return &kvrpcpb.RawDeleteRangeResponse{}
}

func (h kvHandler) handleKvRawScan(req *kvrpcpb.RawScanRequest) *kvrpcpb.RawScanResponse {
	rawKV, ok := h.mvccStore.(RawKV)
	if !ok {
		errStr := "not implemented"
		return &kvrpcpb.RawScanResponse{
			RegionError: &errorpb.Error{
				Message: errStr,
			},
		}
	}

	var pairs []Pair
	if req.Reverse {
		lowerBound := h.startKey
		if bytes.Compare(req.EndKey, lowerBound) > 0 {
			lowerBound = req.EndKey
		}
		pairs = rawKV.RawReverseScan(
			req.StartKey,
			lowerBound,
			int(req.GetLimit()),
		)
	} else {
		upperBound := h.endKey
		if len(req.EndKey) > 0 && (len(upperBound) == 0 || bytes.Compare(req.EndKey, upperBound) < 0) {
			upperBound = req.EndKey
		}
		pairs = rawKV.RawScan(
			req.StartKey,
			upperBound,
			int(req.GetLimit()),
		)
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: convertToPbPairs(pairs),
	}
}

func (h kvHandler) handleSplitRegion(req *kvrpcpb.SplitRegionRequest) *kvrpcpb.SplitRegionResponse {
	keys := req.GetSplitKeys()
	resp := &kvrpcpb.SplitRegionResponse{Regions: make([]*metapb.Region, 0, len(keys)+1)}
	for i, key := range keys {
		k := NewMvccKey(key)
		region, _ := h.cluster.GetRegionByKey(k)
		if bytes.Equal(region.GetStartKey(), key) {
			continue
		}
		if i == 0 {
			// Set the leftmost region.
			resp.Regions = append(resp.Regions, region)
		}
		newRegionID, newPeerIDs := h.cluster.AllocID(), h.cluster.AllocIDs(len(region.Peers))
		newRegion := h.cluster.SplitRaw(region.GetId(), newRegionID, k, newPeerIDs, newPeerIDs[0])
		resp.Regions = append(resp.Regions, newRegion)
	}
	return resp
}

func drainRowsFromExecutor(ctx context.Context, e executor, req *tipb.DAGRequest) (tipb.Chunk, error) {
	var chunk tipb.Chunk
	for {
		row, err := e.Next(ctx)
		if err != nil {
			return chunk, errors.Trace(err)
		}
		if row == nil {
			return chunk, nil
		}
		for _, offset := range req.OutputOffsets {
			chunk.RowsData = append(chunk.RowsData, row[offset]...)
		}
	}
}

type coprHandler struct {
	*Session
}

func (h coprHandler) handleBatchCopRequest(ctx context.Context, req *coprocessor.BatchRequest) (*mockBatchCopDataClient, error) {
	client := &mockBatchCopDataClient{}
	for _, ri := range req.Regions {
		cop := coprocessor.Request{
			Tp:      kv.ReqTypeDAG,
			Data:    req.Data,
			StartTs: req.StartTs,
			Ranges:  ri.Ranges,
		}
		_, exec, dagReq, err := h.buildDAGExecutor(&cop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		chunk, err := drainRowsFromExecutor(ctx, exec, dagReq)
		if err != nil {
			return nil, errors.Trace(err)
		}
		client.chunks = append(client.chunks, chunk)
	}
	return client, nil
}

// Client is a client that sends RPC.
// This is same with tikv.Client, define again for avoid circle import.
type Client interface {
	// Close should release all data.
	Close() error
	// SendRequest sends Request.
	SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error)
}

// RPCClient sends kv RPC calls to mock cluster. RPCClient mocks the behavior of
// a rpc client at tikv's side.
type RPCClient struct {
	Cluster       *Cluster
	MvccStore     MVCCStore
	streamTimeout chan *tikvrpc.Lease
	done          chan struct{}
	// rpcCli uses to redirects RPC request to TiDB rpc server, It is only use for test.
	// Mock TiDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
	// sync.Once uses to avoid concurrency initialize rpcCli.
	sync.Once
	rpcCli Client
}

// NewRPCClient creates an RPCClient.
// Note that close the RPCClient may close the underlying MvccStore.
func NewRPCClient(cluster *Cluster, mvccStore MVCCStore) *RPCClient {
	ch := make(chan *tikvrpc.Lease, 1024)
	done := make(chan struct{})
	go tikvrpc.CheckStreamTimeoutLoop(ch, done)
	return &RPCClient{
		Cluster:       cluster,
		MvccStore:     mvccStore,
		streamTimeout: ch,
		done:          done,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*metapb.Store, error) {
	stores, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	if len(stores) == 0 {
		return nil, errors.New("connect fail")
	}
	for _, store := range stores {
		if store.GetState() != metapb.StoreState_Offline &&
			store.GetState() != metapb.StoreState_Tombstone {
			return store, nil
		}
	}
	return nil, errors.New("connection refused")
}

func (c *RPCClient) checkArgs(ctx context.Context, addr string) (*Session, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	store, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	session := &Session{
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return session, nil
}

// GRPCClientFactory is the GRPC client factory.
// Use global variable to avoid circle import.
// TODO: remove this global variable.
var GRPCClientFactory func() Client

// redirectRequestToRPCServer redirects RPC request to TiDB rpc server, It is only use for test.
// Mock TiDB rpc service will have circle import problem, so just use a real RPC client to send this RPC  server.
func (c *RPCClient) redirectRequestToRPCServer(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	c.Once.Do(func() {
		if GRPCClientFactory != nil {
			c.rpcCli = GRPCClientFactory()
		}
	})
	if c.rpcCli == nil {
		return nil, errors.Errorf("GRPCClientFactory is nil")
	}
	return c.rpcCli.SendRequest(ctx, addr, req, timeout)
}

// SendRequest sends a request to mock cluster.
func (c *RPCClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("RPCClient.SendRequest", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	failpoint.Inject("rpcServerBusy", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(tikvrpc.GenRegionErrorResp(req, &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}))
		}
	})

	// increase coverage for mock tikv
	_ = req.Type.String()
	_ = req.ToBatchCommandsRequest()

	reqCtx := &req.Context
	resp := &tikvrpc.Response{}
	// When the store type is TiDB, the request should handle over to TiDB rpc server to handle.
	if req.StoreTp == tikvrpc.TiDB {
		return c.redirectRequestToRPCServer(ctx, addr, req, timeout)
	}

	session, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	switch req.Type {
	case tikvrpc.CmdGet:
		r := req.Get()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvGet(r)
	case tikvrpc.CmdScan:
		r := req.Scan()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvScan(r)

	case tikvrpc.CmdPrewrite:
		failpoint.Inject("rpcPrewriteResult", func(val failpoint.Value) {
			switch val.(string) {
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.PrewriteResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		})

		r := req.Prewrite()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvPrewrite(r)
	case tikvrpc.CmdPessimisticLock:
		r := req.PessimisticLock()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PessimisticLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvPessimisticLock(r)
	case tikvrpc.CmdPessimisticRollback:
		r := req.PessimisticRollback()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.PessimisticRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvPessimisticRollback(r)
	case tikvrpc.CmdCommit:
		failpoint.Inject("rpcCommitResult", func(val failpoint.Value) {
			switch val.(string) {
			case "timeout":
				failpoint.Return(nil, errors.New("timeout"))
			case "notLeader":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			case "keyError":
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{}},
				}, nil)
			}
		})

		r := req.Commit()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvCommit(r)
		failpoint.Inject("rpcCommitTimeout", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, undeterminedErr)
			}
		})
	case tikvrpc.CmdCleanup:
		r := req.Cleanup()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CleanupResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvCleanup(r)
	case tikvrpc.CmdCheckTxnStatus:
		r := req.CheckTxnStatus()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.CheckTxnStatusResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvCheckTxnStatus(r)
	case tikvrpc.CmdTxnHeartBeat:
		r := req.TxnHeartBeat()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.TxnHeartBeatResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleTxnHeartBeat(r)
	case tikvrpc.CmdBatchGet:
		r := req.BatchGet()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.BatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvBatchGet(r)
	case tikvrpc.CmdBatchRollback:
		r := req.BatchRollback()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvBatchRollback(r)
	case tikvrpc.CmdScanLock:
		r := req.ScanLock()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ScanLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvScanLock(r)
	case tikvrpc.CmdResolveLock:
		r := req.ResolveLock()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvResolveLock(r)
	case tikvrpc.CmdGC:
		r := req.GC()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.GCResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvGC(r)
	case tikvrpc.CmdDeleteRange:
		r := req.DeleteRange()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.DeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvDeleteRange(r)
	case tikvrpc.CmdRawGet:
		r := req.RawGet()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawGet(r)
	case tikvrpc.CmdRawBatchGet:
		r := req.RawBatchGet()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawBatchGet(r)
	case tikvrpc.CmdRawPut:
		r := req.RawPut()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawPut(r)
	case tikvrpc.CmdRawBatchPut:
		r := req.RawBatchPut()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchPutResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawBatchPut(r)
	case tikvrpc.CmdRawDelete:
		r := req.RawDelete()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawDelete(r)
	case tikvrpc.CmdRawBatchDelete:
		r := req.RawBatchDelete()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawBatchDeleteResponse{RegionError: err}
		}
		resp.Resp = kvHandler{session}.handleKvRawBatchDelete(r)
	case tikvrpc.CmdRawDeleteRange:
		r := req.RawDeleteRange()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawDeleteRangeResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawDeleteRange(r)
	case tikvrpc.CmdRawScan:
		r := req.RawScan()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.RawScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleKvRawScan(r)
	case tikvrpc.CmdUnsafeDestroyRange:
		panic("unimplemented")
	case tikvrpc.CmdRegisterLockObserver:
		return nil, errors.New("unimplemented")
	case tikvrpc.CmdCheckLockObserver:
		return nil, errors.New("unimplemented")
	case tikvrpc.CmdRemoveLockObserver:
		return nil, errors.New("unimplemented")
	case tikvrpc.CmdPhysicalScanLock:
		return nil, errors.New("unimplemented")
	case tikvrpc.CmdCop:
		r := req.Cop()
		if err := session.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &coprocessor.Response{RegionError: err}
			return resp, nil
		}
		session.rawStartKey = MvccKey(session.startKey).Raw()
		session.rawEndKey = MvccKey(session.endKey).Raw()
		var res *coprocessor.Response
		switch r.GetTp() {
		case kv.ReqTypeDAG:
			res = coprHandler{session}.handleCopDAGRequest(r)
		case kv.ReqTypeAnalyze:
			res = coprHandler{session}.handleCopAnalyzeRequest(r)
		case kv.ReqTypeChecksum:
			res = coprHandler{session}.handleCopChecksumRequest(r)
		default:
			panic(fmt.Sprintf("unknown coprocessor request type: %v", r.GetTp()))
		}
		resp.Resp = res
	case tikvrpc.CmdBatchCop:
		failpoint.Inject("BatchCopCancelled", func(value failpoint.Value) {
			if value.(bool) {
				failpoint.Return(nil, context.Canceled)
			}
		})

		failpoint.Inject("BatchCopRpcErr"+addr, func(value failpoint.Value) {
			if value.(string) == addr {
				failpoint.Return(nil, errors.New("rpc error"))
			}
		})
		r := req.BatchCop()
		if err := session.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &tikvrpc.BatchCopStreamResponse{
				Tikv_BatchCoprocessorClient: &mockBathCopErrClient{Error: err},
				BatchResponse: &coprocessor.BatchResponse{
					OtherError: err.Message,
				},
			}
			return resp, nil
		}
		ctx1, cancel := context.WithCancel(ctx)
		batchCopStream, err := coprHandler{session}.handleBatchCopRequest(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}
		batchResp := &tikvrpc.BatchCopStreamResponse{Tikv_BatchCoprocessorClient: batchCopStream}
		batchResp.Lease.Cancel = cancel
		batchResp.Timeout = timeout
		c.streamTimeout <- &batchResp.Lease

		first, err := batchResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		batchResp.BatchResponse = first
		resp.Resp = batchResp
	case tikvrpc.CmdCopStream:
		r := req.Cop()
		if err := session.checkRequestContext(reqCtx); err != nil {
			resp.Resp = &tikvrpc.CopStreamResponse{
				Tikv_CoprocessorStreamClient: &mockCopStreamErrClient{Error: err},
				Response: &coprocessor.Response{
					RegionError: err,
				},
			}
			return resp, nil
		}
		session.rawStartKey = MvccKey(session.startKey).Raw()
		session.rawEndKey = MvccKey(session.endKey).Raw()
		ctx1, cancel := context.WithCancel(ctx)
		copStream, err := coprHandler{session}.handleCopStream(ctx1, r)
		if err != nil {
			cancel()
			return nil, errors.Trace(err)
		}

		streamResp := &tikvrpc.CopStreamResponse{
			Tikv_CoprocessorStreamClient: copStream,
		}
		streamResp.Lease.Cancel = cancel
		streamResp.Timeout = timeout
		c.streamTimeout <- &streamResp.Lease

		first, err := streamResp.Recv()
		if err != nil {
			return nil, errors.Trace(err)
		}
		streamResp.Response = first
		resp.Resp = streamResp
	case tikvrpc.CmdMvccGetByKey:
		r := req.MvccGetByKey()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.MvccGetByKeyResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleMvccGetByKey(r)
	case tikvrpc.CmdMvccGetByStartTs:
		r := req.MvccGetByStartTs()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.MvccGetByStartTsResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleMvccGetByStartTS(r)
	case tikvrpc.CmdSplitRegion:
		r := req.SplitRegion()
		if err := session.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Resp = &kvrpcpb.SplitRegionResponse{RegionError: err}
			return resp, nil
		}
		resp.Resp = kvHandler{session}.handleSplitRegion(r)
	// DebugGetRegionProperties is for fast analyze in mock tikv.
	case tikvrpc.CmdDebugGetRegionProperties:
		r := req.DebugGetRegionProperties()
		region, _ := c.Cluster.GetRegion(r.RegionId)
		var reqCtx kvrpcpb.Context
		scanResp := kvHandler{session}.handleKvScan(&kvrpcpb.ScanRequest{
			Context:  &reqCtx,
			StartKey: MvccKey(region.StartKey).Raw(),
			EndKey:   MvccKey(region.EndKey).Raw(),
			Version:  math.MaxUint64,
			Limit:    math.MaxUint32})
		resp.Resp = &debugpb.GetRegionPropertiesResponse{
			Props: []*debugpb.Property{{
				Name:  "mvcc.num_rows",
				Value: strconv.Itoa(len(scanResp.Pairs)),
			}}}
	default:
		return nil, errors.Errorf("unsupported this request type %v", req.Type)
	}
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	close(c.done)

	var err error
	if c.MvccStore != nil {
		err = c.MvccStore.Close()
		if err != nil {
			return err
		}
	}

	if c.rpcCli != nil {
		err = c.rpcCli.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
