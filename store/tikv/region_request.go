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

package tikv

import (
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	goctx "golang.org/x/net/context"
)

// RegionRequestSender sends KV/Cop requests to tikv server. It handles network
// errors and some region errors internally.
//
// Typically, a KV/Cop request is bind to a region, all keys that are involved
// in the request should be located in the region.
// The sending process begins with looking for the address of leader store's
// address of the target region from cache, and the request is then sent to the
// destination tikv server over TCP connection.
// If region is updated, can be caused by leader transfer, region split, region
// merge, or region balance, tikv server may not able to process request and
// send back a RegionError.
// RegionRequestSender takes care of errors that does not relevant to region
// range, such as 'I/O timeout', 'NotLeader', and 'ServerIsBusy'. For other
// errors, since region range have changed, the request may need to split, so we
// simply return the error to caller.
type RegionRequestSender struct {
	bo          *Backoffer
	regionCache *RegionCache
	client      Client
	storeAddr   string
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(bo *Backoffer, regionCache *RegionCache, client Client) *RegionRequestSender {
	return &RegionRequestSender{
		bo:          bo,
		regionCache: regionCache,
		client:      client,
	}
}

type RegionErrorResponse interface {
	GetRegionError() *errorpb.Error
}

type rpcFunc func(ctx *RPCContext) (RegionErrorResponse, error, bool)

func (s *RegionRequestSender) callRpcFunc(regionID RegionVerID, f rpcFunc) error {
	for {
		ctx, err := s.regionCache.GetRPCContext(s.bo, regionID)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err, retry := f(ctx)
		if err != nil {
			if e := s.onSendFail(ctx, err); e != nil {
				return errors.Trace(e)
			}
			continue
		}

		if !retry {
			return nil
		}

		if regionErr := resp.GetRegionError(); regionErr != nil {
			retry, err := s.onRegionError(ctx, regionErr)
			if err != nil {
				return errors.Trace(err)
			}
			if retry {
				continue
			}
			return nil
		}

		return nil
	}
}

func (s *RegionRequestSender) KvGet(req *kvrpcpb.GetRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.GetResponse, error) {
	var resp *kvrpcpb.GetResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			resp = &kvrpcpb.GetResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvGet(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (s *RegionRequestSender) KvScan(req *kvrpcpb.ScanRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.ScanResponse, error) {
	var resp *kvrpcpb.ScanResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.ScanResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvScan(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvPrewrite(req *kvrpcpb.PrewriteRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.PrewriteResponse, error) {
	var resp *kvrpcpb.PrewriteResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.PrewriteResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvPrewrite(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvCommit(req *kvrpcpb.CommitRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.CommitResponse, error) {
	var resp *kvrpcpb.CommitResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.CommitResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvCommit(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvCleanup(req *kvrpcpb.CleanupRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.CleanupResponse, error) {
	var resp *kvrpcpb.CleanupResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.CleanupResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvCleanup(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvBatchGet(req *kvrpcpb.BatchGetRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.BatchGetResponse, error) {
	var resp *kvrpcpb.BatchGetResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.BatchGetResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvCleanup(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvBatchRollback(req *kvrpcpb.BatchRollbackRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.BatchRollbackResponse, error) {
	var resp *kvrpcpb.BatchRollbackResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.BatchRollbackResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvBatchRollback(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvScanLock(req *kvrpcpb.ScanLockRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.ScanLockResponse, error) {
	var resp *kvrpcpb.ScanLockResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.ScanLockResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvScanLock(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvResolveLock(req *kvrpcpb.ResolveLockRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.ResolveLockResponse, error) {
	var resp *kvrpcpb.ResolveLockResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.ResolveLockResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvResolveLock(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvGC(req *kvrpcpb.GCRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.GCRequest, error) {
	var resp *kvrpcpb.GCResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.GCResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvGC(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvRawGet(req *kvrpcpb.RawGetRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.RawGetResponse, error) {
	var resp *kvrpcpb.RawGetResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.RawGetResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvRawGet(context, ctx.Addr, req)
		return resp, err, true
	}

	if err := s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvRawPut(req *kvrpcpb.RawPutRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.RawPutResponse, error) {
	var resp *kvrpcpb.RawPutResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.RawPutResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvRawPut(context, ctx.Addr, req)
		return resp, err, true
	}

	if err != s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) KvRawDelete(req *kvrpcpb.RawDeleteRequest, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.RawDeleteResponse, error) {
	var resp *kvrpcpb.RawDeleteResponse
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &kvrpcpb.RawDeleteResponse{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.KvRawDelete(context, ctx.Addr, req)
		return resp, err, true
	}

	if err != s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) Coprocessor(req *coprocessor.Request, regionID RegionVerID, timeout time.Duration) (*coprocessor.Response, error) {
	var resp *coprocessor.Response
	f := func(ctx *RPCContext) (RegionErrorResponse, error, bool) {
		if ctx == nil {
			resp = &coprocessor.Response{
				RegionError: &erropb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}
			return resp, nil, false
		}
		req.Context = ctx.KVCtx
		context, cancel := goctx.WithDeadline(ctx.Context, time.Now().Add(timeout))
		defer cancel()
		resp, err = s.client.Coprocessor(context, ctx.Addr, req)
		return resp, err, true
	}

	if err != s.callRpcFunc(regionID, f); err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *RegionRequestSender) onSendFail(ctx *RPCContext, err error) error {
	s.regionCache.OnRequestFail(ctx)

	// Retry on request failure when it's not Cancelled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	if errors.Cause(err) != goctx.Canceled {
		err = s.bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %s, try next peer later", err, ctx.KVCtx))
	}
	return errors.Trace(err)
}

func (s *RegionRequestSender) onRegionError(ctx *RPCContext, regionErr *errorpb.Error) (retry bool, err error) {
	reportRegionError(regionErr)
	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		// Retry if error is `NotLeader`.
		log.Debugf("tikv reports `NotLeader`: %s, ctx: %s, retry later", notLeader, ctx.KVCtx)
		s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader().GetStoreId())
		if notLeader.GetLeader() == nil {
			err = s.bo.Backoff(boRegionMiss, errors.Errorf("not leader: %v, ctx: %s", notLeader, ctx.KVCtx))
			if err != nil {
				return false, errors.Trace(err)
			}
		}
		return true, nil
	}

	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		// store not match
		log.Warnf("tikv reports `StoreNotMatch`: %s, ctx: %s, retry later", storeNotMatch, ctx.KVCtx)
		s.regionCache.ClearStoreByID(ctx.GetStoreID())
		return true, nil
	}

	if staleEpoch := regionErr.GetStaleEpoch(); staleEpoch != nil {
		log.Debugf("tikv reports `StaleEpoch`, ctx: %s, retry later", ctx.KVCtx)
		err = s.regionCache.OnRegionStale(ctx, staleEpoch.NewRegions)
		return false, errors.Trace(err)
	}
	if regionErr.GetServerIsBusy() != nil {
		log.Warnf("tikv reports `ServerIsBusy`, ctx: %s, retry later", ctx.KVCtx)
		err = s.bo.Backoff(boServerBusy, errors.Errorf("server is busy, ctx: %s", ctx.KVCtx))
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	if regionErr.GetStaleCommand() != nil {
		log.Debugf("tikv reports `StaleCommand`, ctx: %s", ctx.KVCtx)
		return true, nil
	}
	if regionErr.GetRaftEntryTooLarge() != nil {
		log.Warnf("tikv reports `RaftEntryTooLarge`, ctx: %s", ctx.KVCtx)
		return false, errors.New(regionErr.String())
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	log.Debugf("tikv reports region error: %s, ctx: %s", regionErr, ctx.KVCtx)
	s.regionCache.DropRegion(ctx.Region)
	return false, nil
}
