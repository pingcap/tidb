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

// RPCClient sends kv RPC calls to mock cluster.
type RPCClient struct {
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

func (c *RPCClient) checkArgs(ctx goctx.Context, addr string) (*RPCClient, error) {
	if err := checkGoContext(ctx); err != nil {
		return nil, err
	}

	store, err := c.getAndCheckStoreByAddr(addr)
	if err != nil {
		return nil, err
	}
	client := &RPCClient{
		Cluster:   c.Cluster,
		MvccStore: c.MvccStore,
		// set store id for current request
		storeID: store.GetId(),
	}
	return client, nil
}

func (c *RPCClient) checkRequestContext(ctx *kvrpcpb.Context) *errorpb.Error {
	ctxPear := ctx.GetPeer()
	if ctxPear != nil && ctxPear.GetStoreId() != c.storeID {
		return &errorpb.Error{
			Message:       proto.String("store not match"),
			StoreNotMatch: &errorpb.StoreNotMatch{},
		}
	}
	region, leaderID := c.Cluster.GetRegion(ctx.GetRegionId())
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
		if p.GetStoreId() == c.storeID {
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
		nextRegion, _ := c.Cluster.GetRegionByKey(region.GetEndKey())
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
	c.startKey, c.endKey = region.StartKey, region.EndKey
	c.rawStartKey = MvccKey(c.startKey).Raw()
	c.rawEndKey = MvccKey(c.endKey).Raw()
	return nil
}

func (c *RPCClient) checkRequestSize(size int) *errorpb.Error {
	// TiKV has a limitation on raft log size.
	// mock-tikv has no raft inside, so we check the request's size instead.
	if size >= requestMaxSize {
		return &errorpb.Error{
			RaftEntryTooLarge: &errorpb.RaftEntryTooLarge{},
		}
	}
	return nil
}

func (c *RPCClient) checkRequest(ctx *kvrpcpb.Context, size int) *errorpb.Error {
	if err := c.checkRequestContext(ctx); err != nil {
		return err
	}

	if err := c.checkRequestSize(size); err != nil {
		return err
	}
	return nil
}

func (c *RPCClient) checkKeyInRegion(key []byte) bool {
	return regionContains(c.startKey, c.endKey, []byte(NewMvccKey(key)))
}

func (c *RPCClient) KvGet(ctx goctx.Context, addr string, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.GetResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvGet(req), nil
}

func (c *RPCClient) handleKvGet(req *kvrpcpb.GetRequest) *kvrpcpb.GetResponse {
	if !c.checkKeyInRegion(req.Key) {
		panic("KvGet: key not in region")
	}

	val, err := c.MvccStore.Get(req.Key, req.GetVersion())
	if err != nil {
		return &kvrpcpb.GetResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.GetResponse{
		Value: val,
	}
}

func (c *RPCClient) KvScan(ctx goctx.Context, addr string, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ScanResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvScan(req), nil
}

func (c *RPCClient) handleKvScan(req *kvrpcpb.ScanRequest) *kvrpcpb.ScanResponse {
	if !c.checkKeyInRegion(req.GetStartKey()) {
		panic("KvScan: startKey not in region")
	}
	pairs := c.MvccStore.Scan(req.GetStartKey(), c.endKey, int(req.GetLimit()), req.GetVersion())
	return &kvrpcpb.ScanResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (c *RPCClient) KvPrewrite(ctx goctx.Context, addr string, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.PrewriteResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvPrewrite(req), nil
}

func (c *RPCClient) handleKvPrewrite(req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	for _, m := range req.Mutations {
		if !c.checkKeyInRegion(m.Key) {
			panic("KvPrewrite: key not in region")
		}
	}
	errors := c.MvccStore.Prewrite(req.Mutations, req.PrimaryLock, req.GetStartVersion(), req.GetLockTtl())
	return &kvrpcpb.PrewriteResponse{
		Errors: convertToKeyErrors(errors),
	}
}

func (c *RPCClient) KvCommit(ctx goctx.Context, addr string, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.CommitResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvCommit(req), nil
}

func (c *RPCClient) handleKvCommit(req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	for _, k := range req.Keys {
		if !c.checkKeyInRegion(k) {
			panic("KvCommit: key not in region")
		}
	}
	var resp kvrpcpb.CommitResponse
	err := c.MvccStore.Commit(req.Keys, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		resp.Error = convertToKeyError(err)
	}
	return &resp
}

func (c *RPCClient) KvCleanup(ctx goctx.Context, addr string, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.CleanupResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvCleanup(req), nil
}

func (c *RPCClient) handleKvCleanup(req *kvrpcpb.CleanupRequest) *kvrpcpb.CleanupResponse {
	if !c.checkKeyInRegion(req.Key) {
		panic("KvCleanup: key not in region")
	}
	var resp kvrpcpb.CleanupResponse
	err := c.MvccStore.Cleanup(req.Key, req.GetStartVersion())
	if err != nil {
		if commitTS, ok := err.(ErrAlreadyCommitted); ok {
			resp.CommitVersion = uint64(commitTS)
		} else {
			resp.Error = convertToKeyError(err)
		}
	}
	return &resp
}

func (c *RPCClient) KvBatchGet(ctx goctx.Context, addr string, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.BatchGetResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvBatchGet(req), nil
}

func (c *RPCClient) handleKvBatchGet(req *kvrpcpb.BatchGetRequest) *kvrpcpb.BatchGetResponse {
	for _, k := range req.Keys {
		if !c.checkKeyInRegion(k) {
			panic("KvBatchGet: key not in region")
		}
	}
	pairs := c.MvccStore.BatchGet(req.Keys, req.GetVersion())
	return &kvrpcpb.BatchGetResponse{
		Pairs: convertToPbPairs(pairs),
	}
}

func (c *RPCClient) KvBatchRollback(ctx goctx.Context, addr string, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvBatchRollback(req), nil
}

func (c *RPCClient) handleKvBatchRollback(req *kvrpcpb.BatchRollbackRequest) *kvrpcpb.BatchRollbackResponse {
	err := c.MvccStore.Rollback(req.Keys, req.StartVersion)
	if err != nil {
		return &kvrpcpb.BatchRollbackResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.BatchRollbackResponse{}
}

func (c *RPCClient) KvScanLock(ctx goctx.Context, addr string, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ScanLockResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvScanLock(req), nil
}

func (c *RPCClient) handleKvScanLock(req *kvrpcpb.ScanLockRequest) *kvrpcpb.ScanLockResponse {
	locks, err := c.MvccStore.ScanLock(c.startKey, c.endKey, req.GetMaxVersion())
	if err != nil {
		return &kvrpcpb.ScanLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ScanLockResponse{
		Locks: locks,
	}
}

func (c *RPCClient) KvResolveLock(ctx goctx.Context, addr string, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.ResolveLockResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvResolveLock(req), nil
}

func (c *RPCClient) handleKvResolveLock(req *kvrpcpb.ResolveLockRequest) *kvrpcpb.ResolveLockResponse {
	err := c.MvccStore.ResolveLock(c.startKey, c.endKey, req.GetStartVersion(), req.GetCommitVersion())
	if err != nil {
		return &kvrpcpb.ResolveLockResponse{
			Error: convertToKeyError(err),
		}
	}
	return &kvrpcpb.ResolveLockResponse{}
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
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvRawGet(req), nil
}

func (c *RPCClient) handleKvRawGet(req *kvrpcpb.RawGetRequest) *kvrpcpb.RawGetResponse {
	return &kvrpcpb.RawGetResponse{
		Value: c.MvccStore.RawGet(req.GetKey()),
	}
}

func (c *RPCClient) RawPut(ctx goctx.Context, addr string, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := client.checkRequest(req.GetContext(), req.Size()); err != nil {
		return &kvrpcpb.RawPutResponse{
			RegionError: err,
		}, nil
	}

	return client.handleKvRawPut(req), nil
}

func (c *RPCClient) handleKvRawPut(req *kvrpcpb.RawPutRequest) *kvrpcpb.RawPutResponse {
	c.MvccStore.RawPut(req.GetKey(), req.GetValue())
	return &kvrpcpb.RawPutResponse{}
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

func (c *RPCClient) handleKvRawDelete(req *kvrpcpb.RawDeleteRequest) *kvrpcpb.RawDeleteResponse {
	c.MvccStore.RawDelete(req.GetKey())
	return &kvrpcpb.RawDeleteResponse{}
}

func (c *RPCClient) CoprocessorNew(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
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

	store := c.Cluster.GetStoreByAddr(addr)
	if store == nil {
		return nil, errors.New("connect fail")
	}
	resp, err := c.handleCopDAGRequest(req)
	return resp, errors.Trace(err)
}

func (c *RPCClient) Coprocessor(ctx goctx.Context, addr string, req *coprocessor.Request) (*coprocessor.Response, error) {
	client, err := c.checkArgs(ctx, addr)
	if err != nil {
		return nil, err
	}

	if MockDAGRequest {
		if req.GetTp() == kv.ReqTypeSelect || req.GetTp() == kv.ReqTypeIndex {
			req.Tp = kv.ReqTypeDAG
			resp, err := client.CoprocessorNew(ctx, addr, req)
			return resp, errors.Trace(err)
		}
	}

	return client.handleCopRequest(req)
}

func (c *RPCClient) Close() error {
	return nil
}
