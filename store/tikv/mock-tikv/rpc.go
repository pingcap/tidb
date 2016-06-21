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
	"github.com/pingcap/kvproto/pkg/kvpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
)

type rpcHandler struct {
	cluster   *Cluster
	mvccStore *MvccStore
	storeID   uint64
	startKey  []byte
	endKey    []byte
}

func newRPCHandler(cluster *Cluster, mvccStore *MvccStore, storeID uint64) *rpcHandler {
	return &rpcHandler{
		cluster:   cluster,
		mvccStore: mvccStore,
		storeID:   storeID,
	}
}

func (h *rpcHandler) handleRequest(req *kvrpcpb.Request) *kvrpcpb.Response {
	resp := &kvrpcpb.Response{
		Type: req.Type,
	}
	if err := h.checkContext(req.GetContext()); err != nil {
		resp.RegionError = err
		return resp
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
	case kvrpcpb.MessageType_CmdCommitThenGet:
		resp.CmdCommitGetResp = h.onCommitThenGet(req.CmdCommitGetReq)
	case kvrpcpb.MessageType_CmdRollbackThenGet:
		resp.CmdRbGetResp = h.onRollbackThenGet(req.CmdRbGetReq)
	case kvrpcpb.MessageType_CmdBatchGet:
		resp.CmdBatchGetResp = h.onBatchGet(req.CmdBatchGetReq)
	}
	return resp
}

func (h *rpcHandler) checkContext(ctx *kvrpcpb.Context) *errorpb.Error {
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
		return &errorpb.Error{
			Message:    proto.String("stale epoch"),
			StaleEpoch: &errorpb.StaleEpoch{},
		}
	}
	h.startKey, h.endKey = region.StartKey, region.EndKey
	return nil
}

func (h *rpcHandler) keyInRegion(key []byte) bool {
	return regionContains(h.startKey, h.endKey, key)
}

func makeColumns(colNames, colVals [][]byte) []*kvpb.Column {
	if colVals == nil {
		return nil
	}
	if len(colNames) != len(colVals) {
		panic("makeColumns: len(colNames) != len(colVals)")
	}
	cols := make([]*kvpb.Column, len(colNames))
	for i := range cols {
		cols[i] = &kvpb.Column{
			Name:  colNames[i],
			Value: colVals[i],
		}
	}
	return cols
}

func (h *rpcHandler) onGet(req *kvrpcpb.CmdGetRequest) *kvrpcpb.CmdGetResponse {
	if !h.keyInRegion(req.GetRowKey()) {
		panic("onGet: key not in region")
	}

	vals, err := h.mvccStore.Get(req.GetRowKey(), req.GetColumns(), req.GetTs())
	if err != nil {
		return &kvrpcpb.CmdGetResponse{
			Row: &kvpb.Row{
				Error:  convertToKeyError(err),
				RowKey: req.GetRowKey(),
			},
		}
	}

	return &kvrpcpb.CmdGetResponse{
		Row: &kvpb.Row{
			RowKey:  req.GetRowKey(),
			Columns: makeColumns(req.GetColumns(), vals),
		},
	}
}

func (h *rpcHandler) onScan(req *kvrpcpb.CmdScanRequest) *kvrpcpb.CmdScanResponse {
	if !h.keyInRegion(req.GetStartRowKey()) {
		panic("onScan: startKey not in region")
	}
	rows := h.mvccStore.Scan(req.GetStartRowKey(), h.endKey, req.GetColumns(), int(req.GetLimit()), req.GetTs())
	return &kvrpcpb.CmdScanResponse{
		Rows: convertToPbRows(rows),
	}
}

func (h *rpcHandler) onPrewrite(req *kvrpcpb.CmdPrewriteRequest) *kvrpcpb.CmdPrewriteResponse {
	for _, m := range req.Mutations {
		if !h.keyInRegion(m.GetRowKey()) {
			panic("onPrewrite: key not in region")
		}
	}
	errors := h.mvccStore.Prewrite(req.GetMutations(), req.GetPrimary(), req.GetTs())
	return &kvrpcpb.CmdPrewriteResponse{
		Errors: convertToKeyErrors(errors),
	}
}

func (h *rpcHandler) onCommit(req *kvrpcpb.CmdCommitRequest) *kvrpcpb.CmdCommitResponse {
	for _, rowKey := range req.GetRowKeys() {
		if !h.keyInRegion(rowKey) {
			panic("onCommit: key not in region")
		}
	}
	var resp kvrpcpb.CmdCommitResponse
	err := h.mvccStore.Commit(req.GetRowKeys(), req.GetStartTs(), req.GetCommitTs())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (h *rpcHandler) onCleanup(req *kvrpcpb.CmdCleanupRequest) *kvrpcpb.CmdCleanupResponse {
	if !h.keyInRegion(req.GetRowKey()) {
		panic("onCleanup: key not in region")
	}
	var resp kvrpcpb.CmdCleanupResponse
	err := h.mvccStore.Cleanup(req.GetRowKey(), req.GetTs())
	if err != nil {
		if commitTS, ok := err.(ErrAlreadyCommitted); ok {
			resp.CommitTs = proto.Uint64(uint64(commitTS))
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (h *rpcHandler) onCommitThenGet(req *kvrpcpb.CmdCommitThenGetRequest) *kvrpcpb.CmdCommitThenGetResponse {
	if !h.keyInRegion(req.GetRowKey()) {
		panic("onCommitThenGet: key not in region")
	}
	vals, err := h.mvccStore.CommitThenGet(req.GetRowKey(), req.GetColumns(), req.GetStartTs(), req.GetCommitTs(), req.GetGetTs())
	if err != nil {
		return &kvrpcpb.CmdCommitThenGetResponse{
			Row: &kvpb.Row{
				Error:  convertToKeyError(err),
				RowKey: req.GetRowKey(),
			},
		}
	}
	return &kvrpcpb.CmdCommitThenGetResponse{
		Row: &kvpb.Row{
			RowKey:  req.GetRowKey(),
			Columns: makeColumns(req.GetColumns(), vals),
		},
	}
}

func (h *rpcHandler) onRollbackThenGet(req *kvrpcpb.CmdRollbackThenGetRequest) *kvrpcpb.CmdRollbackThenGetResponse {
	if !h.keyInRegion(req.GetRowKey()) {
		panic("onRollbackThenGet: key not in region")
	}
	vals, err := h.mvccStore.RollbackThenGet(req.GetRowKey(), req.GetColumns(), req.GetTs())
	if err != nil {
		return &kvrpcpb.CmdRollbackThenGetResponse{
			Row: &kvpb.Row{
				Error:  convertToKeyError(err),
				RowKey: req.GetRowKey(),
			},
		}
	}
	return &kvrpcpb.CmdRollbackThenGetResponse{
		Row: &kvpb.Row{
			RowKey:  req.GetRowKey(),
			Columns: makeColumns(req.GetColumns(), vals),
		},
	}
}

func (h *rpcHandler) onBatchGet(req *kvrpcpb.CmdBatchGetRequest) *kvrpcpb.CmdBatchGetResponse {
	cols := make([][][]byte, len(req.GetRowKeys()))
	for i, row := range req.GetRowKeys() {
		if !h.keyInRegion(row) {
			panic("onBatchGet: key not in region")
		}
		cols[i] = req.GetColumns()[i].GetColumns()
	}
	rows := h.mvccStore.BatchGet(req.GetRowKeys(), cols, req.GetTs())
	return &kvrpcpb.CmdBatchGetResponse{
		Rows: convertToPbRows(rows),
	}
}

func convertToKeyError(err error) *kvpb.KeyError {
	if locked, ok := err.(*ErrLocked); ok {
		return &kvpb.KeyError{
			Locked: &kvpb.LockInfo{
				Row:     locked.Key,
				Primary: locked.Primary,
				Ts:      proto.Uint64(locked.StartTS),
			},
		}
	}
	if retryable, ok := err.(ErrRetryable); ok {
		return &kvpb.KeyError{
			Retryable: proto.String(retryable.Error()),
		}
	}
	return &kvpb.KeyError{
		Abort: proto.String(err.Error()),
	}
}

func convertToKeyErrors(errs []error) []*kvpb.KeyError {
	var errors []*kvpb.KeyError
	for _, err := range errs {
		if err != nil {
			errors = append(errors, convertToKeyError(err))
		}
	}
	return errors
}

func convertToPbRows(rows []Row) []*kvpb.Row {
	pbRows := make([]*kvpb.Row, len(rows))
	for i, r := range rows {
		var pbRow *kvpb.Row
		if r.Err == nil {
			pbRow = &kvpb.Row{
				RowKey:  r.RowKey,
				Columns: makeColumns(r.Columns, r.Values),
			}
		} else {
			pbRow = &kvpb.Row{
				Error: convertToKeyError(r.Err),
			}
		}
		pbRows[i] = pbRow
	}
	return pbRows
}

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient struct {
	addr      string
	cluster   *Cluster
	mvccStore *MvccStore
}

// SendKVReq sends a kv request to mock cluster.
func (c *RPCClient) SendKVReq(req *kvrpcpb.Request) (*kvrpcpb.Response, error) {
	store := c.cluster.GetStoreByAddr(c.addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	handler := newRPCHandler(c.cluster, c.mvccStore, store.GetId())
	return handler.handleRequest(proto.Clone(req).(*kvrpcpb.Request)), nil
}

// SendCopReq sends a coprocessor request to mock cluster.
func (c *RPCClient) SendCopReq(req *coprocessor.Request) (*coprocessor.Response, error) {
	store := c.cluster.GetStoreByAddr(c.addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	handler := newRPCHandler(c.cluster, c.mvccStore, store.GetId())
	return handler.handleCopRequest(proto.Clone(req).(*coprocessor.Request))
}

// Close closes the client.
func (c *RPCClient) Close() error {
	return nil
}

// NewRPCClient creates an RPCClient.
func NewRPCClient(cluster *Cluster, mvccStore *MvccStore, addr string) *RPCClient {
	return &RPCClient{
		addr:      addr,
		cluster:   cluster,
		mvccStore: mvccStore,
	}
}
