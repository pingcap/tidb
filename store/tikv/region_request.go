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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	regionCache *RegionCache
	client      Client
	storeAddr   string
	rpcError    error
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(regionCache *RegionCache, client Client) *RegionRequestSender {
	return &RegionRequestSender{
		regionCache: regionCache,
		client:      client,
	}
}

// SendReq sends a request to tikv server.
func (s *RegionRequestSender) SendReq(bo *Backoffer, req *tikvrpc.Request, regionID RegionVerID, timeout time.Duration) (*tikvrpc.Response, error) {
	failpoint.Inject("tikvStoreSendReqResult", func(val failpoint.Value) {
		switch val.(string) {
		case "timeout":
			failpoint.Return(nil, errors.New("timeout"))
		case "GCNotLeader":
			if req.Type == tikvrpc.CmdGC {
				failpoint.Return(&tikvrpc.Response{
					Type: tikvrpc.CmdGC,
					GC:   &kvrpcpb.GCResponse{RegionError: &errorpb.Error{NotLeader: &errorpb.NotLeader{}}},
				}, nil)
			}
		case "GCServerIsBusy":
			if req.Type == tikvrpc.CmdGC {
				failpoint.Return(&tikvrpc.Response{
					Type: tikvrpc.CmdGC,
					GC:   &kvrpcpb.GCResponse{RegionError: &errorpb.Error{ServerIsBusy: &errorpb.ServerIsBusy{}}},
				}, nil)
			}
		}
	})

	for {
		ctx, err := s.regionCache.GetRPCContext(bo, regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ctx == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.

			// TODO: Change the returned error to something like "region missing in cache",
			// and handle this error like EpochNotMatch, which means to re-split the request and retry.
			return tikvrpc.GenRegionErrorResp(req, &errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{}})
		}

		s.storeAddr = ctx.Addr
		resp, retry, err := s.sendReqToRegion(bo, ctx, req, timeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if retry {
			continue
		}

		regionErr, err := resp.GetRegionError()
		if err != nil {
			return nil, errors.Trace(err)
		}
		if regionErr != nil {
			retry, err := s.onRegionError(bo, ctx, regionErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if retry {
				continue
			}
		}
		return resp, nil
	}
}

func (s *RegionRequestSender) sendReqToRegion(bo *Backoffer, ctx *RPCContext, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, retry bool, err error) {
	if e := tikvrpc.SetContext(req, ctx.Meta, ctx.Peer); e != nil {
		return nil, false, errors.Trace(e)
	}
	resp, err = s.client.SendRequest(bo.ctx, ctx.Addr, req, timeout)
	if err != nil {
		s.rpcError = err
		if e := s.onSendFail(bo, ctx, err); e != nil {
			return nil, false, errors.Trace(e)
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) onSendFail(bo *Backoffer, ctx *RPCContext, err error) error {
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled {
		return errors.Trace(err)
	}
	if grpc.Code(errors.Cause(err)) == codes.Canceled {
		select {
		case <-bo.ctx.Done():
			return errors.Trace(err)
		default:
			// If we don't cancel, but the error code is Canceled, it must be from grpc remote.
			// This may happen when tikv is killed and exiting.
			// Backoff and retry in this case.
			logutil.Logger(context.Background()).Warn("receive a grpc cancel signal from remote", zap.Error(err))
		}
	}

	s.regionCache.DropStoreOnSendRequestFail(ctx, err)

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	err = bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %v, try next peer later", err, ctx))
	return errors.Trace(err)
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

func (s *RegionRequestSender) onRegionError(bo *Backoffer, ctx *RPCContext, regionErr *errorpb.Error) (retry bool, err error) {
	metrics.TiKVRegionErrorCounter.WithLabelValues(regionErrorToLabel(regionErr)).Inc()
	if notLeader := regionErr.GetNotLeader(); notLeader != nil {
		// Retry if error is `NotLeader`.
		logutil.Logger(context.Background()).Debug("tikv reports `NotLeader` retry later",
			zap.String("notLeader", notLeader.String()),
			zap.String("ctx", ctx.String()))
		s.regionCache.UpdateLeader(ctx.Region, notLeader.GetLeader().GetStoreId())

		var boType backoffType
		if notLeader.GetLeader() != nil {
			boType = BoUpdateLeader
		} else {
			boType = BoRegionMiss
		}

		if err = bo.Backoff(boType, errors.Errorf("not leader: %v, ctx: %v", notLeader, ctx)); err != nil {
			return false, errors.Trace(err)
		}

		return true, nil
	}

	if storeNotMatch := regionErr.GetStoreNotMatch(); storeNotMatch != nil {
		// store not match
		logutil.Logger(context.Background()).Warn("tikv reports `StoreNotMatch` retry later",
			zap.Stringer("storeNotMatch", storeNotMatch),
			zap.Stringer("ctx", ctx))
		s.regionCache.ClearStoreByID(ctx.GetStoreID())
		return true, nil
	}

	if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
		logutil.Logger(context.Background()).Debug("tikv reports `EpochNotMatch` retry later",
			zap.Stringer("EpochNotMatch", epochNotMatch),
			zap.Stringer("ctx", ctx))
		err = s.regionCache.OnRegionEpochNotMatch(bo, ctx, epochNotMatch.CurrentRegions)
		return false, errors.Trace(err)
	}
	if regionErr.GetServerIsBusy() != nil {
		logutil.Logger(context.Background()).Warn("tikv reports `ServerIsBusy` retry later",
			zap.String("reason", regionErr.GetServerIsBusy().GetReason()),
			zap.Stringer("ctx", ctx))
		err = bo.Backoff(boServerBusy, errors.Errorf("server is busy, ctx: %v", ctx))
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	if regionErr.GetStaleCommand() != nil {
<<<<<<< HEAD
		logutil.Logger(context.Background()).Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
=======
		logutil.BgLogger().Debug("tikv reports `StaleCommand`", zap.Stringer("ctx", ctx))
		err = bo.Backoff(boStaleCmd, errors.Errorf("stale command, ctx: %v", ctx))
		if err != nil {
			return false, errors.Trace(err)
		}
>>>>>>> 14a4a4e... tikv: fix infinite retry when kv continuing to return staleCommand error (#16481)
		return true, nil
	}
	if regionErr.GetRaftEntryTooLarge() != nil {
		logutil.Logger(context.Background()).Warn("tikv reports `RaftEntryTooLarge`", zap.Stringer("ctx", ctx))
		return false, errors.New(regionErr.String())
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	logutil.Logger(context.Background()).Debug("tikv reports region error",
		zap.Stringer("regionErr", regionErr),
		zap.Stringer("ctx", ctx))
	s.regionCache.DropRegion(ctx.Region)
	return false, nil
}

func pbIsolationLevel(level kv.IsoLevel) kvrpcpb.IsolationLevel {
	switch level {
	case kv.RC:
		return kvrpcpb.IsolationLevel_RC
	case kv.SI:
		return kvrpcpb.IsolationLevel_SI
	default:
		return kvrpcpb.IsolationLevel_SI
	}
}
