// Copyright 2021 PingCAP, Inc.
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

package copr

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegionBatchRequestSender sends BatchCop requests to TiFlash server by stream way.
type RegionBatchRequestSender struct {
	*tikv.RegionRequestSender
}

// NewRegionBatchRequestSender creates a RegionBatchRequestSender object.
func NewRegionBatchRequestSender(cache *tikv.RegionCache, client tikv.Client) *RegionBatchRequestSender {
	return &RegionBatchRequestSender{
		RegionRequestSender: tikv.NewRegionRequestSender(cache, client),
	}
}

func (ss *RegionBatchRequestSender) sendStreamReqToAddr(bo *tikv.Backoffer, ctxs []copTaskAndRPCContext, req *tikvrpc.Request, timout time.Duration) (resp *tikvrpc.Response, retry bool, cancel func(), err error) {
	// use the first ctx to send request, because every ctx has same address.
	cancel = func() {}
	rpcCtx := ctxs[0].ctx
	if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
		return nil, false, cancel, errors.Trace(e)
	}
	ctx := bo.GetCtx()
	if rawHook := ctx.Value(tikv.RPCCancellerCtxKey{}); rawHook != nil {
		ctx, cancel = rawHook.(*tikv.RPCCanceller).WithCancel(ctx)
	}
	start := time.Now()
	resp, err = ss.GetClient().SendRequest(ctx, rpcCtx.Addr, req, timout)
	if ss.Stats != nil {
		tikv.RecordRegionRequestRuntimeStats(ss.Stats, req.Type, time.Since(start))
	}
	if err != nil {
		cancel()
		ss.SetRPCError(err)
		e := ss.onSendFail(bo, ctxs, err)
		if e != nil {
			return nil, false, func() {}, errors.Trace(e)
		}
		return nil, true, func() {}, nil
	}
	// We don't need to process region error or lock error. Because TiFlash will retry by itself.
	return
}

func (ss *RegionBatchRequestSender) onSendFail(bo *tikv.Backoffer, ctxs []copTaskAndRPCContext, err error) error {
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled || status.Code(errors.Cause(err)) == codes.Canceled {
		return errors.Trace(err)
	} else if atomic.LoadUint32(&tikv.ShuttingDown) > 0 {
		return kv.ErrTiDBShuttingDown
	}

	for _, failedCtx := range ctxs {
		ctx := failedCtx.ctx
		if ctx.Meta != nil {
			ss.GetRegionCache().OnSendFail(bo, ctx, ss.NeedReloadRegion(ctx), err)
		}
	}

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	err = bo.Backoff(tikv.BoTiFlashRPC, errors.Errorf("send tikv request error: %v, ctxs: %v, try next peer later", err, ctxs))
	return errors.Trace(err)
}
