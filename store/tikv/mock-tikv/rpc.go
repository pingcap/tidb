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
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
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
	cluster   *Cluster
	mvccStore *MvccStore

	// store id for current request
	storeID uint64
	// Used for handling normal request.
	startKey []byte
	endKey   []byte
	// Used for handling coprocessor request.
	rawStartKey []byte
	rawEndKey   []byte
	// Used for current request.
	isolationLevel kvrpcpb.IsolationLevel
}

func (h *rpcHandler) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPeer := ctx.GetPeer()
	if ctxPeer != nil && ctxPeer.GetStoreId() != h.storeID {
		return &errorpb.Error{
			Message:       proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := h.cluster.GetRegion(ctx.GetRegionId())
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
		nextRegion, _ := h.cluster.GetRegionByKey(region.GetEndKey())
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
	h.isolationLevel = ctx.IsolationLevel
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

	val, err := h.mvccStore.Get(req.Key, req.GetVersion(), h.isolationLevel)
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
	pairs := h.mvccStore.Scan(req.GetStartKey(), h.endKey, int(req.GetLimit()), req.GetVersion(), h.isolationLevel)
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
	errors := h.mvccStore.Prewrite(req.Mutations, req.PrimaryLock, req.GetStartVersion(), req.GetLockTtl())
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
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
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
	err := h.mvccStore.Cleanup(req.Key, req.GetStartVersion())
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
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion(), h.isolationLevel)
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (h *rpcHandler) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	locks, err := h.mvccStore.ScanLock(h.startKey, h.endKey, req.GetMaxVersion())
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
	err := h.mvccStore.ResolveLock(h.startKey, h.endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
}

func (h *rpcHandler) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	return &kvrpcpb.RawGetResponse{
		Value: h.mvccStore.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	h.mvccStore.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
}

func (h *rpcHandler) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	h.mvccStore.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient struct {
	Cluster   *Cluster
	MvccStore *MvccStore
}

// NewRPCClient creates an RPCClient.
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
		cluster:   c.Cluster,
		mvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return handler, nil
}

// SendReq sends a request to mock cluster.
func (c *RPCClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	handler, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	reqCtx, err := req.GetContext()
	if err != nil {
		return nil, err
	}
	resp := &tikvrpc.Response{}
	resp.Type = req.Type
	switch req.Type {
	case tikvrpc.CmdGet:
		r := req.Get
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Get = &kvrpcpb.GetResponse{RegionError: err}
			return resp, nil
		}
		resp.Get = handler.handleKvGet(r)
	case tikvrpc.CmdScan:
		r := req.Scan
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Scan = &kvrpcpb.ScanResponse{RegionError: err}
			return resp, nil
		}
		resp.Scan = handler.handleKvScan(r)

	case tikvrpc.CmdPrewrite:
		r := req.Prewrite
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Prewrite = &kvrpcpb.PrewriteResponse{RegionError: err}
			return resp, nil
		}
		resp.Prewrite = handler.handleKvPrewrite(r)
	case tikvrpc.CmdCommit:
		r := req.Commit
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Commit = &kvrpcpb.CommitResponse{RegionError: err}
			return resp, nil
		}
		resp.Commit = handler.handleKvCommit(r)
	case tikvrpc.CmdCleanup:
		r := req.Cleanup
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.Cleanup = &kvrpcpb.CleanupResponse{RegionError: err}
			return resp, nil
		}
		resp.Cleanup = handler.handleKvCleanup(r)
	case tikvrpc.CmdBatchGet:
		r := req.BatchGet
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.BatchGet = &kvrpcpb.BatchGetResponse{RegionError: err}
			return resp, nil
		}
		resp.BatchGet = handler.handleKvBatchGet(r)
	case tikvrpc.CmdBatchRollback:
		r := req.BatchRollback
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.BatchRollback = &kvrpcpb.BatchRollbackResponse{RegionError: err}
			return resp, nil
		}
		resp.BatchRollback = handler.handleKvBatchRollback(r)
	case tikvrpc.CmdScanLock:
		r := req.ScanLock
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.ScanLock = &kvrpcpb.ScanLockResponse{RegionError: err}
			return resp, nil
		}
		resp.ScanLock = handler.handleKvScanLock(r)
	case tikvrpc.CmdResolveLock:
		r := req.ResolveLock
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.ResolveLock = &kvrpcpb.ResolveLockResponse{RegionError: err}
			return resp, nil
		}
		resp.ResolveLock = handler.handleKvResolveLock(r)
	case tikvrpc.CmdGC:
		r := req.GC
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.GC = &kvrpcpb.GCResponse{RegionError: err}
			return resp, nil
		}
		resp.GC = &kvrpcpb.GCResponse{}
	case tikvrpc.CmdRawGet:
		r := req.RawGet
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.RawGet = &kvrpcpb.RawGetResponse{RegionError: err}
			return resp, nil
		}
		resp.RawGet = handler.handleKvRawGet(r)
	case tikvrpc.CmdRawPut:
		r := req.RawPut
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.RawPut = &kvrpcpb.RawPutResponse{RegionError: err}
			return resp, nil
		}
		resp.RawPut = handler.handleKvRawPut(r)
	case tikvrpc.CmdRawDelete:
		r := req.RawDelete
		if err := handler.checkRequest(reqCtx, r.Size()); err != nil {
			resp.RawDelete = &kvrpcpb.RawDeleteResponse{RegionError: err}
			return resp, nil
		}
		resp.RawDelete = handler.handleKvRawDelete(r)
	case tikvrpc.CmdCop:
		r := req.Cop
		if err := handler.checkRequestContext(reqCtx); err != nil {
			resp.Cop = &coprocessor.Response{RegionError: err}
			return resp, nil
		}
		res, err := handler.handleCopRequest(r)
		if err != nil {
			return nil, err
		}
		resp.Cop = res
	default:
		return nil, errors.Errorf("unsupport this request type %v", req.Type)
	}
	return resp, nil
}

// Close closes the client.
func (c *RPCClient) Close() error {
	return nil
}
