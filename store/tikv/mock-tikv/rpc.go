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
	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

const requestMaxSize = 4 * 1024 * 1024

func checkGoContext(ctx goctx.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func convertToKeyError(err error) *kvrpcpb.KeyError {
	if locked, ok := err.(*ErrLocked); ok {
		return &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				Key:         locked.Key.Raw(),
				PrimaryLock: locked.Primary,
				LockVersion: locked.StartTS,
				LockTtl:     locked.TTL,
			},
		}
	}
	if retryable, ok := err.(ErrRetryable); ok {
		return &kvrpcpb.KeyError{
			Retryable: retryable.Error(),
		}
	}
	return &kvrpcpb.KeyError{
		Abort: err.Error(),
	}
}

func convertToKeyErrors(errs []error) []*kvrpcpb.KeyError {
	var errors []*kvrpcpb.KeyError
	for _, err := range errs {
		if err != nil {
			errors = append(errors, convertToKeyError(err))
		}
	}
	return errors
}

func convertToPbPairs(pairs []Pair) []*kvrpcpb.KvPair {
	var kvPairs []*kvrpcpb.KvPair
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

type rpcHandler struct {
	Cluster   *Cluster
	MvccStore *MvccStore

	// store id for current request
	storeID uint64
	// Used for handling normal request.
	startKey []byte
	endKey   []byte
	// Used for handling coprocessor request.
	rawStartKey []byte
	rawEndKey   []byte
}

func (h *rpcHandler) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPear := ctx.GetPeer()
	if ctxPear != nil && ctxPear.GetStoreId() != h.storeID {
		return &errorpb.Error{
			Message:       proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := h.Cluster.GetRegion(ctx.GetRegionId())
	// No region found.
	if region == nil {
		return &errorpb.Error{
			Message: proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	var storePeer, leaderPeer *metapb.Peer
	for _, p := range region.Peers {
		if p.GetStoreId() == h.storeID {
			storePeer = p
		}
		if p.GetId() == leaderID {
			leaderPeer = p
		}
	}
	// The Store does not contain a Peer of the Region.
	if storePeer == nil {
		return &errorpb.Error{
			Message: proto.String("region not found"),
			RegionNotFound: &errorpb.RegionNotFound{
				RegionId: proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// No leader.
	if leaderPeer == nil {
		return &errorpb.Error{
			Message: proto.String("no leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: proto.Uint64(ctx.GetRegionId()),
			},
		}
	}
	// The Peer on the Store is not leader.
	if storePeer.GetId() != leaderPeer.GetId() {
		return &errorpb.Error{
			Message: proto.String("not leader"),
			NotLeader: &errorpb.NotLeader{
				RegionId: proto.Uint64(ctx.GetRegionId()),
				Leader:   leaderPeer,
			},
		}
	}
	// Region epoch does not match.
	if !proto.Equal(region.GetRegionEpoch(), ctx.GetRegionEpoch()) {
		nextRegion, _ := h.Cluster.GetRegionByKey(region.GetEndKey())
		newRegions := []*metapb.Region{region}
		if nextRegion != nil {
			newRegions = append(newRegions, nextRegion)
		}
		return &errorpb.Error{
			Message: proto.String("stale epoch"),
			StaleEpoch: &errorpb.StaleEpoch{
				NewRegions: newRegions,
			},
		}
	}
	h.startKey, h.endKey = region.StartKey, region.EndKey
	h.rawStartKey = MvccKey(h.startKey).Raw()
	h.rawEndKey = MvccKey(h.endKey).Raw()
	return nil
}

func (h *rpcHandler) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mock-tikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (h *rpcHandler) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	if err := h.checkRequestContext(ctx); err != nil {
		return err
	}

	if err := h.checkRequestSize(size); err != nil {
		return err
	}
	return nil
}

func (h *rpcHandler) checkKeyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, []byte(NewMvccKey(key)))
}

func (h *rpcHandler) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvGet: key not in region")
	}

	val, err := h.MvccStore.Get(req.Key, req.GetVersion())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (h *rpcHandler) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	if !h.checkKeyInRegion(req.GetStartKey()) {
		panic("KvScan: startKey not in region")
	}
	pairs := h.MvccStore.Scan(req.GetStartKey(), h.endKey, int(req.GetLimit()), req.GetVersion())
	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	for _, m := range req.Mutations {
		if !h.checkKeyInRegion(m.Key) {
			panic("KvPrewrite: key not in region")
		}
	}
	errors := h.MvccStore.Prewrite(req.Mutations, req.PrimaryLock, req.GetStartVersion(), req.GetLockTtl())
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errors),
	}
}

func (h *rpcHandler) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvCommit: key not in region")
		}
	}
	var resp kvrpcpb.CommitResponse
	err := h.MvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) handleKvCleanup(req *kvrpcpb.CleanupRequest) *kvrpcpb.CleanupResponse {
	if !h.checkKeyInRegion(req.Key) {
		panic("KvCleanup: key not in region")
	}
	var resp kvrpcpb.CleanupResponse
	err := h.MvccStore.Cleanup(req.Key, req.GetStartVersion())
	if err != nil {
		if commitTS, ok := err.(ErrAlreadyCommitted); ok {
			resp.CommitVersion = uint64(commitTS)
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (h *rpcHandler) handleKvBatchGet(req *kvrpcpb.BatchGetRequest) *kvrpcpb.BatchGetResponse {
	for _, k := range req.Keys {
		if !h.checkKeyInRegion(k) {
			panic("KvBatchGet: key not in region")
		}
	}
	pairs := h.MvccStore.BatchGet(req.Keys, req.GetVersion())
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := h.MvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	locks, err := h.MvccStore.ScanLock(h.startKey, h.endKey, req.GetMaxVersion())
	if err != nil {
		return &kvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (h *rpcHandler) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	err := h.MvccStore.ResolveLock(h.startKey, h.endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	return &kvrpcpb.RawGetResponse{
		Value: h.MvccStore.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	h.MvccStore.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	h.MvccStore.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (h *rpcHandler) handleCopRequestNew(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	sel := new(tipb.SelectRequest)
	err := proto.Unmarshal(req.Data, sel)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var executors []*tipb.Executor
	// TODO: Now the value of desc is false.
	// And it's used for testing.
	desc := false
	var exec *tipb.Executor
	if sel.TableInfo != nil {
		exec = &tipb.Executor{
			Tp: tipb.ExecType_TypeTableScan,
			TblScan: &tipb.TableScan{
				Columns: sel.TableInfo.Columns,
				Desc:    &desc,
			},
		}
	} else {
		exec = &tipb.Executor{
			Tp: tipb.ExecType_TypeIndexScan,
			IdxScan: &tipb.IndexScan{
				Columns: sel.IndexInfo.Columns,
				Desc:    &desc,
			},
		}
	}
	executors = append(executors, exec)
	if sel.Where != nil {
		exec := &tipb.Executor{
			Tp: tipb.ExecType_TypeSelection,
			Selection: &tipb.Selection{
				Conditions: []*tipb.Expr{sel.Where},
			},
		}
		executors = append(executors, exec)
	}

	dag := &tipb.DAGRequest{
		StartTs:        sel.GetStartTs(),
		TimeZoneOffset: sel.TimeZoneOffset,
		Flags:          sel.Flags,
		Executors:      executors,
	}
	req.Data, err = dag.Marshal()
	if err != nil {
		return nil, errors.Trace(err)
	}

	store := h.Cluster.GetStoreByAddr(addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	resp, err := h.handleCopDAGRequest(req)
	return resp, errors.Trace(err)
}

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient struct {
	Cluster   *Cluster
	MvccStore *MvccStore
}

// NewRPCClient creates a RPCClient.
func NewRPCClient(cluster *Cluster, mvccStore *MvccStore) *RPCClient {
	return &RPCClient{
		Cluster:   cluster,
		MvccStore: mvccStore,
	}
}

func (c *RPCClient) getAndCheckStoreByAddr(addr string) (*metapb.Store, error) {
	store, err := c.Cluster.GetAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("connect fail")
	}
	if store.GetState() == metapb.StoreState_Offline ||
		store.GetState() == metapb.StoreState_Tombstone {
		return nil, errors.New("connection refused")
	}
	return store, nil
}

func (c *RPCClient) checkArgs(ctx goctx.Context, addr string) (*rpcHandler, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	store, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	handler := &rpcHandler{
		Cluster:   c.Cluster,
		MvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return handler, nil
}

func (c *RPCClient) KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.GetResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvGet(req), nil
}

func (c *RPCClient) KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ScanResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvScan(req), nil
}

func (c *RPCClient) KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.PrewriteResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvPrewrite(req), nil
}

func (c *RPCClient) KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.CommitResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvCommit(req), nil
}

func (c *RPCClient) KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.CleanupResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvCleanup(req), nil
}

func (c *RPCClient) KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.BatchGetResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvBatchGet(req), nil
}

func (c *RPCClient) KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvBatchRollback(req), nil
}

func (c *RPCClient) KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ScanLockResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvScanLock(req), nil
}

func (c *RPCClient) KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ResolveLockResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvResolveLock(req), nil
}

func (c *RPCClient) KvGC(ctx goctx.Context, addr string, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.GCResponse{
			RegionError: err,
		}, nil
	}

	return &kvrpcpb.GCResponse{}, nil
}

func (c *RPCClient) RawGet(ctx goctx.Context, addr string, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvRawGet(req), nil
}

func (c *RPCClient) RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := handler.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.RawPutResponse{
			RegionError: err,
		}, nil
	}

	return handler.handleKvRawPut(req), nil
}

func (c *RPCClient) RawDelete(ctx goctx.Context, addr string, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.RawDeleteResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvRawDelete(req), nil
}

func (c *RPCClient) Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}

	if MockDAGRequest {
		if req.GetTp() == kv.ReqTypeSelect || req.GetTp() == kv.ReqTypeIndex {
			req.Tp = kv.ReqTypeDAG
			resp, err := handler.handleCopRequestNew(ctx, addr, req)
			return resp, errors.Trace(err)
		}
	}

	return handler.handleCopRequest(req)
}

func (c *RPCClient) Close() error {
	return nil
}
