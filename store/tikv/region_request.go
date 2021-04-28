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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
)

// ShuttingDown is a flag to indicate tidb-server is exiting (Ctrl+C signal
// receved for example). If this flag is set, tikv client should not retry on
// network error because tidb-server expect tikv client to exit as soon as possible.
var ShuttingDown uint32

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
	regionCache       *RegionCache
	client            Client
	storeAddr         string
	rpcError          error
	failStoreIDs      map[uint64]struct{}
	failProxyStoreIDs map[uint64]struct{}
	RegionRequestRuntimeStats
}

// RegionRequestRuntimeStats records the runtime stats of send region requests.
type RegionRequestRuntimeStats struct {
	Stats map[tikvrpc.CmdType]*RPCRuntimeStats
}

// NewRegionRequestRuntimeStats returns a new RegionRequestRuntimeStats.
func NewRegionRequestRuntimeStats() RegionRequestRuntimeStats {
	return RegionRequestRuntimeStats{
		Stats: make(map[tikvrpc.CmdType]*RPCRuntimeStats),
	}
}

// RPCRuntimeStats indicates the RPC request count and consume time.
type RPCRuntimeStats struct {
	Count int64
	// Send region request consume time.
	Consume int64
}

// String implements fmt.Stringer interface.
func (r *RegionRequestRuntimeStats) String() string {
	var buf bytes.Buffer
	for k, v := range r.Stats {
		if buf.Len() > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("%s:{num_rpc:%d, total_time:%s}", k.String(), v.Count, util.FormatDuration(time.Duration(v.Consume))))
	}
	return buf.String()
}

// Clone returns a copy of itself.
func (r *RegionRequestRuntimeStats) Clone() RegionRequestRuntimeStats {
	newRs := NewRegionRequestRuntimeStats()
	for cmd, v := range r.Stats {
		newRs.Stats[cmd] = &RPCRuntimeStats{
			Count:   v.Count,
			Consume: v.Consume,
		}
	}
	return newRs
}

// Merge merges other RegionRequestRuntimeStats.
func (r *RegionRequestRuntimeStats) Merge(rs RegionRequestRuntimeStats) {
	for cmd, v := range rs.Stats {
		stat, ok := r.Stats[cmd]
		if !ok {
			r.Stats[cmd] = &RPCRuntimeStats{
				Count:   v.Count,
				Consume: v.Consume,
			}
			continue
		}
		stat.Count += v.Count
		stat.Consume += v.Consume
	}
}

// RecordRegionRequestRuntimeStats records request runtime stats.
func RecordRegionRequestRuntimeStats(stats map[tikvrpc.CmdType]*RPCRuntimeStats, cmd tikvrpc.CmdType, d time.Duration) {
	stat, ok := stats[cmd]
	if !ok {
		stats[cmd] = &RPCRuntimeStats{
			Count:   1,
			Consume: int64(d),
		}
		return
	}
	stat.Count++
	stat.Consume += int64(d)
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client Client) *RegionRequestSender {
	return &RegionRequestSender{
		regionCache: regionCache,
		client:      client,
	}
}

// GetRegionCache returns the region cache.
func (s *RegionRequestSender) GetRegionCache() *RegionCache {
	return s.regionCache
}

// GetClient returns the RPC client.
func (s *RegionRequestSender) GetClient() Client {
	return s.client
}

// SetStoreAddr specifies the dest store address.
func (s *RegionRequestSender) SetStoreAddr(addr string) {
	s.storeAddr = addr
}

// GetStoreAddr returns the dest store address.
func (s *RegionRequestSender) GetStoreAddr() string {
	return s.storeAddr
}

// GetRPCError returns the RPC error.
func (s *RegionRequestSender) GetRPCError() error {
	return s.rpcError
}

// SetRPCError rewrite the rpc error.
func (s *RegionRequestSender) SetRPCError(err error) {
	s.rpcError = err
}

// SendReq sends a request to tikv server.
func (s *RegionRequestSender) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, _, err := s.SendReqCtx(bo, req, regionID, timeout, tikvrpc.TiKV)
	return resp, err
}

func (s *RegionRequestSender) getRPCContext(
	bo *Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (*RPCContext, error) {
	switch et {
	case tikvrpc.TiKV:
		var seed uint32
		if req.ReplicaReadSeed != nil {
			seed = *req.ReplicaReadSeed
		}
		return s.regionCache.GetTiKVRPCContext(bo, regionID, req.ReplicaReadType, seed, opts...)
	case tikvrpc.TiFlash:
		return s.regionCache.GetTiFlashRPCContext(bo, regionID, true)
	case tikvrpc.TiDB:
		return &RPCContext{Addr: s.storeAddr}, nil
	default:
		return nil, errors.Errorf("unsupported storage type: %v", et)
	}
}

// SendReqCtx sends a request to tikv server and return response and RPCCtx of this RPC.
func (s *RegionRequestSender) SendReqCtx(
	bo *Backoffer,
	req *tikvrpc.Request,
	regionID RegionVerID,
	timeout time.Duration,
	et tikvrpc.EndpointType,
	opts ...StoreSelectorOption,
) (
	resp *tikvrpc.Response,
	rpcCtx *RPCContext,
	err error,
) {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.SendReqCtx", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		// TODO(MyonKeminta): Make sure trace works without cloning the backoffer.
		// bo = bo.Clone()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	failpoint.Inject("tikvStoreSendReqResult", func(val failpoint.Value) {
		switch val.(string) {
		case "timeout":
			failpoint.Return(nil, nil, errors.New("timeout"))
		case "GCNotLeader":
			if req.Type == tikvrpc.CmdGC {
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil, nil)
			}
		case "GCServerIsBusy":
			if req.Type == tikvrpc.CmdGC {
				failpoint.Return(&tikvrpc.Response{
					Resp: &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil, nil)
			}
		case "callBackofferHook":
			if bo.vars != nil && bo.vars.Hook != nil {
				bo.vars.Hook("callBackofferHook", bo.vars)
			}
		case "requestTiDBStoreError":
			if et == tikvrpc.TiDB {
				failpoint.Return(nil, nil, tikverr.ErrTiKVServerTimeout)
			}
		case "requestTiFlashError":
			if et == tikvrpc.TiFlash {
				failpoint.Return(nil, nil, tikverr.ErrTiFlashServerTimeout)
			}
		}
	})

	tryTimes := 0
	for {
		if (tryTimes > 0) && (tryTimes%1000 == 0) {
			logutil.Logger(bo.ctx).Warn("retry get ", zap.Uint64("region = ", regionID.GetID()), zap.Int("times = ", tryTimes))
		}

		rpcCtx, err = s.getRPCContext(bo, req, regionID, et, opts...)
		if err != nil {
			return nil, nil, err
		}

		failpoint.Inject("invalidCacheAndRetry", func() {
			// cooperate with github.com/pingcap/tidb/store/gcworker/setGcResolveMaxBackoff
			if c := bo.ctx.Value("injectedBackoff"); c != nil {
				resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
				failpoint.Return(resp, nil, err)
			}
		})
		if rpcCtx == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.

			// TODO: Change the returned error to something like "region missing in cache",
			// and handle this error like EpochNotMatch, which means to re-split the request and retry.
			logutil.Logger(bo.ctx).Debug("throwing pseudo region error due to region not found in cache", zap.Stringer("region", &regionID))
			resp, err = tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
			return resp, nil, err
		}

		logutil.Eventf(bo.ctx, "send %s request to region %d at %s", req.Type, regionID.id, rpcCtx.Addr)
		s.storeAddr = rpcCtx.Addr
		var retry bool
		resp, retry, err = s.sendReqToRegion(bo, rpcCtx, req, timeout)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		// recheck whether the session/query is killed during the Next()
		if bo.vars != nil && bo.vars.Killed != nil && atomic.LoadUint32(bo.vars.Killed) == 1 {
			return nil, nil, tikverr.ErrQueryInterrupted
		}
		failpoint.Inject("mockRetrySendReqToRegion", func(val failpoint.Value) {
			if val.(bool) {
				retry = true
			}
		})
		if retry {
			tryTimes++
			continue
		}

		var regionErr *errorpb.Error
		regionErr, err = resp.GetRegionError()
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if regionErr != nil {
			retry, err = s.onRegionError(bo, rpcCtx, req.ReplicaReadSeed, regionErr)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if retry {
				tryTimes++
				continue
			}
		}
		return resp, rpcCtx, nil
	}
}

// RPCCancellerCtxKey is context key attach rpc send cancelFunc collector to ctx.
type RPCCancellerCtxKey struct{}

// RPCCanceller is rpc send cancelFunc collector.
type RPCCanceller struct {
	sync.Mutex
	allocID   int
	cancels   map[int]func()
	cancelled bool
}

// NewRPCanceller creates RPCCanceller with init state.
func NewRPCanceller() *RPCCanceller {
	return &RPCCanceller{cancels: make(map[int]func())}
}

// WithCancel generates new context with cancel func.
func (h *RPCCanceller) WithCancel(ctx context.Context) (context.Context, func()) {
	nctx, cancel := context.WithCancel(ctx)
	h.Lock()
	if h.cancelled {
		h.Unlock()
		cancel()
		return nctx, func() {}
	}
	id := h.allocID
	h.allocID++
	h.cancels[id] = cancel
	h.Unlock()
	return nctx, func() {
		cancel()
		h.Lock()
		delete(h.cancels, id)
		h.Unlock()
	}
}

// CancelAll cancels all inflight rpc context.
func (h *RPCCanceller) CancelAll() {
	h.Lock()
	for _, c := range h.cancels {
		c()
	}
	h.cancelled = true
	h.Unlock()
}

func (s *RegionRequestSender) sendReqToRegion(bo *Backoffer, rpcCtx *RPCContext, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, retry bool, err error) {
	if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
		return nil, false, errors.Trace(e)
	}
	// judge the store limit switch.
	if limit := kv.StoreLimit.Load(); limit > 0 {
		if err := s.getStoreToken(rpcCtx.Store, limit); err != nil {
			return nil, false, err
		}
		defer s.releaseStoreToken(rpcCtx.Store)
	}

	ctx := bo.ctx
	if rawHook := ctx.Value(RPCCancellerCtxKey{}); rawHook != nil {
		var cancel context.CancelFunc
		ctx, cancel = rawHook.(*RPCCanceller).WithCancel(ctx)
		defer cancel()
	}

	// sendToAddr is the first target address that will receive the request. If proxy is used, sendToAddr will point to
	// the proxy that will forward the request to the final target.
	sendToAddr := rpcCtx.Addr
	if rpcCtx.ProxyStore == nil {
		req.ForwardedHost = ""
	} else {
		req.ForwardedHost = rpcCtx.Addr
		sendToAddr = rpcCtx.ProxyAddr
	}

	var sessionID uint64
	if v := bo.ctx.Value(util.SessionID); v != nil {
		sessionID = v.(uint64)
	}

	injectFailOnSend := false
	failpoint.Inject("rpcFailOnSend", func(val failpoint.Value) {
		inject := true
		// Optional filters
		if s, ok := val.(string); ok {
			if s == "greengc" && !req.IsGreenGCRequest() {
				inject = false
			} else if s == "write" && !req.IsTxnWriteRequest() {
				inject = false
			}
		} else if sessionID == 0 {
			inject = false
		}

		if inject {
			logutil.Logger(ctx).Info("[failpoint] injected RPC error on send", zap.Stringer("type", req.Type),
				zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context))
			injectFailOnSend = true
			err = errors.New("injected RPC error on send")
		}
	})

	if !injectFailOnSend {
		start := time.Now()
		resp, err = s.client.SendRequest(ctx, sendToAddr, req, timeout)
		if s.Stats != nil {
			RecordRegionRequestRuntimeStats(s.Stats, req.Type, time.Since(start))
			failpoint.Inject("tikvStoreRespResult", func(val failpoint.Value) {
				if val.(bool) {
					if req.Type == tikvrpc.CmdCop && bo.totalSleep == 0 {
						failpoint.Return(&tikvrpc.Response{
							Resp: &coprocessor.Response{RegionError: &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}}},
						}, false, nil)
					}
				}
			})
		}

		failpoint.Inject("rpcFailOnRecv", func(val failpoint.Value) {
			inject := true
			// Optional filters
			if s, ok := val.(string); ok {
				if s == "greengc" && !req.IsGreenGCRequest() {
					inject = false
				} else if s == "write" && !req.IsTxnWriteRequest() {
					inject = false
				}
			} else if sessionID == 0 {
				inject = false
			}

			if inject {
				logutil.Logger(ctx).Info("[failpoint] injected RPC error on recv", zap.Stringer("type", req.Type),
					zap.Stringer("req", req.Req.(fmt.Stringer)), zap.Stringer("ctx", &req.Context))
				err = errors.New("injected RPC error on recv")
				resp = nil
			}
		})

		failpoint.Inject("rpcContextCancelErr", func(val failpoint.Value) {
			if val.(bool) {
				ctx1, cancel := context.WithCancel(context.Background())
				cancel()
				select {
				case <-ctx1.Done():
				}

				ctx = ctx1
				err = ctx.Err()
				resp = nil
			}
		})
	}

	if rpcCtx.ProxyStore != nil {
		fromStore := strconv.FormatUint(rpcCtx.ProxyStore.storeID, 10)
		toStore := strconv.FormatUint(rpcCtx.Store.storeID, 10)
		result := "ok"
		if err != nil {
			result = "fail"
		}
		metrics.TiKVForwardRequestCounter.WithLabelValues(fromStore, toStore, req.Type.String(), result).Inc()
	}

	if err != nil {
		s.rpcError = err

		// Because in rpc logic, context.Cancel() will be transferred to rpcContext.Cancel error. For rpcContext cancel,
		// we need to retry the request. But for context cancel active, for example, limitExec gets the required rows,
		// we shouldn't retry the request, it will go to backoff and hang in retry logic.
		if ctx.Err() != nil && errors.Cause(ctx.Err()) == context.Canceled {
			return nil, false, errors.Trace(ctx.Err())
		}

		failpoint.Inject("noRetryOnRpcError", func(val failpoint.Value) {
			if val.(bool) {
				failpoint.Return(nil, false, err)
			}
		})
		if e := s.onSendFail(bo, rpcCtx, err); e != nil {
			return nil, false, errors.Trace(e)
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) getStoreToken(st *Store, limit int64) error {
	// Checking limit is not thread safe, preferring this for avoiding load in loop.
	count := st.tokenCount.Load()
	if count < limit {
		// Adding tokenCount is no thread safe, preferring this for avoiding check in loop.
		st.tokenCount.Add(1)
		return nil
	}
	metrics.TiKVStoreLimitErrorCounter.WithLabelValues(st.addr, strconv.FormatUint(st.storeID, 10)).Inc()
	return tikverr.ErrTokenLimit.GenWithStackByArgs(st.storeID)

}

func (s *RegionRequestSender) releaseStoreToken(st *Store) {
	count := st.tokenCount.Load()
	// Decreasing tokenCount is no thread safe, preferring this for avoiding check in loop.
	if count > 0 {
		st.tokenCount.Sub(1)
		return
	}
	logutil.BgLogger().Warn("release store token failed, count equals to 0")
}

func (s *RegionRequestSender) onSendFail(bo *Backoffer, ctx *RPCContext, err error) error {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("regionRequest.onSendFail", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		// TODO(MyonKeminta): Make sure trace works without cloning the backoffer.
		// bo = bo.Clone()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		return errors.Trace(err)
	} else if atomic.LoadUint32(&ShuttingDown) > 0 {
		return tikverr.ErrTiDBShuttingDown
	}
	if status.Code(errors.Cause(err)) == codes.Canceled {
		select {
		case <-bo.ctx.Done():
			return errors.Trace(err)
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			logutil.BgLogger().Warn("receive a grpc cancel signal from remote", zap.Error(err))
		}
	}

	if ctx.Meta != nil {
		s.regionCache.OnSendFail(bo, ctx, s.NeedReloadRegion(ctx), err)
	}

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	if ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlash {
		err = bo.Backoff(BoTiFlashRPC, errors.Errorf("send tiflash request error: %v, ctx: %v, try next peer later", err, ctx))
	} else {
		err = bo.Backoff(BoTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %v, try next peer later", err, ctx))
	}
	return errors.Trace(err)
}

// NeedReloadRegion checks is all peers has sent failed, if so need reload.
func (s *RegionRequestSender) NeedReloadRegion(ctx *RPCContext) (need bool) {
	if s.failStoreIDs == nil {
		s.failStoreIDs = make(map[uint64]struct{})
	}
	if s.failProxyStoreIDs == nil {
		s.failProxyStoreIDs = make(map[uint64]struct{})
	}
	s.failStoreIDs[ctx.Store.storeID] = struct{}{}
	if ctx.ProxyStore != nil {
		s.failProxyStoreIDs[ctx.ProxyStore.storeID] = struct{}{}
	}

	if ctx.AccessMode == TiKVOnly && len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= ctx.TiKVNum {
		need = true
	} else if ctx.AccessMode == TiFlashOnly && len(s.failStoreIDs) >= len(ctx.Meta.Peers)-ctx.TiKVNum {
		need = true
	} else if len(s.failStoreIDs)+len(s.failProxyStoreIDs) >= len(ctx.Meta.Peers) {
		need = true
	}

	if need {
		s.failStoreIDs = nil
		s.failProxyStoreIDs = nil
	}
	return
}

func regionErrorToLabel(e *errorpb.Error) string {
	if e.GetNotLeader() != nil {
		return "not_leader"
	} else if e.GetRegionNotFound() != nil {
		return "region_not_found"
	} else if e.GetKeyNotInRegion() != nil {
		return "key_not_in_region"
	} else if e.GetEpochNotMatch() != nil {
		return "epoch_not_match"
	} else if e.GetServerIsBusy() != nil {
		return "server_is_busy"
	} else if e.GetStaleCommand() != nil {
		return "stale_command"
	} else if e.GetStoreNotMatch() != nil {
		return "store_not_match"
	}
	return "unknown"
}

func (s *RegionRequestSender) onRegionError(bo *Backoffer, ctx *RPCContext, seed *uint32, regionErr *errorpb.Error) (retry bool, err error) {
	if span := opentracing.SpanFromContext(bo.ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("tikv.onRegionError", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		// TODO(MyonKeminta): Make sure trace works without cloning the backoffer.
		// bo = bo.Clone()
		bo.ctx = opentracing.ContextWithSpan(bo.ctx, span1)
	}

	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrorToLabel(regionErr)).Inc()
	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		// Retry if error is `NotLeader`.
		logutil.BgLogger().Debug("tikv reports `NotLeader` retry later",
			zap.String("notLeader", notLeader.String()),
			zap.String("ctx", ctx.String()))

		if notLeader.GetLeader() == nil {
			// The peer doesn't know who is the current leader. Generally it's because
			// the Raft group is in an election, but it's possible that the peer is
			// isolated and removed from the Raft group. So it's necessary to reload
			// the region from PD.
			s.regionCache.InvalidateCachedRegionWithReason(ctx.Region, NoLeader)
			if err = bo.Backoff(BoRegionMiss, errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)); err != nil {
				return false, errors.Trace(err)
			}
		} else {
			// don't backoff if a new leader is returned.
			s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader().GetStoreId(), ctx.AccessIdx)
		}

		return true, nil
	}

	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		// store not match
		logutil.BgLogger().Debug("tikv reports `StoreNotMatch` retry later",
			zap.Stringer("storeNotMatch", storeNotMatch),
			zap.Stringer("ctx", ctx))
		ctx.Store.markNeedCheck(s.regionCache.notifyCheckCh)
		s.regionCache.InvalidateCachedRegion(ctx.Region)
		return true, nil
	}

	if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
		logutil.BgLogger().Debug("tikv reports `EpochNotMatch` retry later",
			zap.Stringer("EpochNotMatch", epochNotMatch),
			zap.Stringer("ctx", ctx))
		if seed != nil {
			*seed = *seed + 1
		}
		err = s.regionCache.OnRegionEpochNotMatch(bo, ctx, epochNotMatch.CurrentRegions)
		return false, errors.Trace(err)
	}
	if regionErr.GetServerIsBusy() != nil {
		logutil.BgLogger().Warn("tikv reports `ServerIsBusy` retry later",
			zap.String("reason", regionErr.GetServerIsBusy().GetReason()),
			zap.Stringer("ctx", ctx))
		if ctx != nil && ctx.Store != nil && ctx.Store.storeType == tikvrpc.TiFlash {
			err = bo.Backoff(boTiFlashServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		} else {
			err = bo.Backoff(boTiKVServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	if regionErr.GetStaleCommand() != nil {
		logutil.BgLogger().Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(boStaleCmd, errors.Errorf("stale command, ctx: %v", ctx))
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	if regionErr.GetRaftEntryTooLarge() != nil {
		logutil.BgLogger().Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", ctx))
		return false, errors.New(regionErr.String())
	}
	if regionErr.GetRegionNotFound() != nil && seed != nil {
		logutil.BgLogger().Debug("tikv reports `RegionNotFound` in follow-reader",
			zap.Stringer("ctx", ctx), zap.Uint32("seed", *seed))
		*seed = *seed + 1
	}
	if regionErr.GetMaxTimestampNotSynced() != nil {
		logutil.BgLogger().Warn("tikv reports `MaxTimestampNotSynced`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(boMaxTsNotSynced, errors.Errorf("max timestamp not synced, ctx: %v", ctx))
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	logutil.BgLogger().Debug("tikv reports region failed",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", ctx))
	// When the request is sent to TiDB, there is no region in the request, so the region id will be 0.
	// So when region id is 0, there is no business with region cache.
	if ctx.Region.id != 0 {
		s.regionCache.InvalidateCachedRegion(ctx.Region)
	}
	return false, nil
}
