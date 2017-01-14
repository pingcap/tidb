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
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/util/codec"
)

const requestMaxSize = 4 * 1024 * 1024

type rpcHandler struct {
	cluster   *Cluster
	mvccStore *MvccStore
	storeID   uint64
	startKey  []byte
	endKey    []byte
}

func newRPCHandler(cluster *Cluster, mvccStore *MvccStore, storeID uint64) *rpcHandler {
	h := &rpcHandler{
		cluster:   cluster,
		mvccStore: mvccStore,
		storeID:   storeID,
	}
	return h
}

func (h *rpcHandler) handleRequest(req *kvrpcpb.Request) *kvrpcpb.Response {
	var resp kvrpcpb.Response
	if err := h.checkContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return &resp
	}
	// TiKV has a limitation on raft log size.
	// mock-tikv has no raft inside, so we check the request's size instead.
	if err := h.checkSize(req); err != nil {
		resp.RegionError = err
		return &resp
	}
	switch req.GetType() {
	case kvrpcpb.MessageType_CmdGet:
		resp.CmdGetResp = h.onGet(req.CmdGetReq)
	case kvrpcpb.MessageType_CmdScan:
		resp.CmdScanResp = h.onScan(req.CmdScanReq)
	case kvrpcpb.MessageType_CmdPrewrite:
		resp.CmdPrewriteResp = h.onPrewrite(req.CmdPrewriteReq)
	case kvrpcpb.MessageType_CmdCommit:
		resp.CmdCommitResp = h.onCommit(req.CmdCommitReq)
	case kvrpcpb.MessageType_CmdCleanup:
		resp.CmdCleanupResp = h.onCleanup(req.CmdCleanupReq)
	case kvrpcpb.MessageType_CmdBatchGet:
		resp.CmdBatchGetResp = h.onBatchGet(req.CmdBatchGetReq)
	case kvrpcpb.MessageType_CmdScanLock:
		resp.CmdResolveLockResp = h.onResolveLock(req.CmdResolveLockReq)
	case kvrpcpb.MessageType_CmdResolveLock:
		resp.CmdResolveLockResp = h.onResolveLock(req.CmdResolveLockReq)
	case kvrpcpb.MessageType_CmdBatchRollback:
		resp.CmdBatchRollbackResp = h.onBatchRollback(req.CmdBatchRollbackReq)

	case kvrpcpb.MessageType_CmdRawGet:
		resp.CmdRawGetResp = h.onRawGet(req.CmdRawGetReq)
	case kvrpcpb.MessageType_CmdRawPut:
		resp.CmdRawPutResp = h.onRawPut(req.CmdRawPutReq)
	case kvrpcpb.MessageType_CmdRawDelete:
		resp.CmdRawDeleteResp = h.onRawDelete(req.CmdRawDeleteReq)
	}
	resp.Type = req.Type
	return &resp
}

func (h *rpcHandler) checkContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPear := ctx.GetPeer()
	if ctxPear != nil && ctxPear.GetStoreId() != h.storeID {
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
	return nil
}

func (h *rpcHandler) checkSize(req *kvrpcpb.Request) *errorpb.Error {
	if req.Size() >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (h *rpcHandler) keyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, []byte(NewMvccKey(key)))
}

func (h *rpcHandler) onGet(req *kvrpcpb.CmdGetRequest) *kvrpcpb.CmdGetResponse {
	if !h.keyInRegion(req.Key) {
		panic("onGet: key not in region")
	}

	val, err := h.mvccStore.Get(req.Key, req.GetVersion())
	if err != nil {
		return &kvrpcpb.CmdGetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.CmdGetResponse{
		Value: val,
	}
}

func (h *rpcHandler) onScan(req *kvrpcpb.CmdScanRequest) *kvrpcpb.CmdScanResponse {
	if !h.keyInRegion(req.GetStartKey()) {
		panic("onScan: startKey not in region")
	}
	pairs := h.mvccStore.Scan(req.GetStartKey(), h.endKey, int(req.GetLimit()), req.GetVersion())
	return &kvrpcpb.CmdScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) onPrewrite(req *kvrpcpb.CmdPrewriteRequest) *kvrpcpb.CmdPrewriteResponse {
	for _, m := range req.Mutations {
		if !h.keyInRegion(m.Key) {
			panic("onPrewrite: key not in region")
		}
	}
	errors := h.mvccStore.Prewrite(req.Mutations, req.PrimaryLock, req.GetStartVersion(), req.GetLockTtl())
	return &kvrpcpb.CmdPrewriteResponse{
		Errors: convertToKeyErrors(errors),
	}
}

func (h *rpcHandler) onCommit(req *kvrpcpb.CmdCommitRequest) *kvrpcpb.CmdCommitResponse {
	for _, k := range req.Keys {
		if !h.keyInRegion(k) {
			panic("onCommit: key not in region")
		}
	}
	var resp kvrpcpb.CmdCommitResponse
	err := h.mvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) onCleanup(req *kvrpcpb.CmdCleanupRequest) *kvrpcpb.CmdCleanupResponse {
	if !h.keyInRegion(req.Key) {
		panic("onCleanup: key not in region")
	}
	var resp kvrpcpb.CmdCleanupResponse
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

func (h *rpcHandler) onBatchGet(req *kvrpcpb.CmdBatchGetRequest) *kvrpcpb.CmdBatchGetResponse {
	for _, k := range req.Keys {
		if !h.keyInRegion(k) {
			panic("onBatchGet: key not in region")
		}
	}
	pairs := h.mvccStore.BatchGet(req.Keys, req.GetVersion())
	return &kvrpcpb.CmdBatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (h *rpcHandler) onScanLock(req *kvrpcpb.CmdScanLockRequest) *kvrpcpb.CmdScanLockResponse {
	locks, err := h.mvccStore.ScanLock(h.startKey, h.endKey, req.GetMaxVersion())
	if err != nil {
		return &kvrpcpb.CmdScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.CmdScanLockResponse{
		Locks: locks,
	}
}

func (h *rpcHandler) onResolveLock(req *kvrpcpb.CmdResolveLockRequest) *kvrpcpb.CmdResolveLockResponse {
	err := h.mvccStore.ResolveLock(h.startKey, h.endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.CmdResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.CmdResolveLockResponse{}
}

func (h *rpcHandler) onBatchRollback(req *kvrpcpb.CmdBatchRollbackRequest) *kvrpcpb.CmdBatchRollbackResponse {
	err := h.mvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.CmdBatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.CmdBatchRollbackResponse{}
}

func (h *rpcHandler) onRawGet(req *kvrpcpb.CmdRawGetRequest) *kvrpcpb.CmdRawGetResponse {
	return &kvrpcpb.CmdRawGetResponse{
		Value: h.mvccStore.RawGet(req.GetKey()),
	}
}

func (h *rpcHandler) onRawPut(req *kvrpcpb.CmdRawPutRequest) *kvrpcpb.CmdRawPutResponse {
	h.mvccStore.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.CmdRawPutResponse{}
}

func (h *rpcHandler) onRawDelete(req *kvrpcpb.CmdRawDeleteRequest) *kvrpcpb.CmdRawDeleteResponse {
	h.mvccStore.RawDelete(req.GetKey())
	return &kvrpcpb.CmdRawDeleteResponse{}
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

func encodeRegionKey(r *metapb.Region) *metapb.Region {
	if r.StartKey != nil {
		r.StartKey = codec.EncodeBytes(nil, r.StartKey)
	}
	if r.EndKey != nil {
		r.EndKey = codec.EncodeBytes(nil, r.EndKey)
	}
	return r
}

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient struct {
	Cluster   *Cluster
	MvccStore *MvccStore
}

// SendKVReq sends a kv request to mock cluster.
func (c *RPCClient) SendKVReq(addr string, req *kvrpcpb.Request, timeout time.Duration) (*kvrpcpb.Response, error) {
	store := c.Cluster.GetStoreByAddr(addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	handler := newRPCHandler(c.Cluster, c.MvccStore, store.GetId())
	return handler.handleRequest(req), nil
}

// SendCopReq sends a coprocessor request to mock cluster.
func (c *RPCClient) SendCopReq(addr string, req *coprocessor.Request, timeout time.Duration) (*coprocessor.Response, error) {
	store := c.Cluster.GetStoreByAddr(addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	handler := newRPCHandler(c.Cluster, c.MvccStore, store.GetId())
	return handler.handleCopRequest(req)
}

// Close closes the client.
func (c *RPCClient) Close() error {
	return nil
}

// NewRPCClient creates an RPCClient.
func NewRPCClient(cluster *Cluster, mvccStore *MvccStore) *RPCClient {
	return &RPCClient{
		Cluster:   cluster,
		MvccStore: mvccStore,
	}
}
