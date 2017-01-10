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
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(bo *Backoffer, regionCache *RegionCache, client Client) *RegionRequestSender {
	return &RegionRequestSender{
		bo:          bo,
		regionCache: regionCache,
		client:      client,
	}
}

// SendKVReq sends a KV request to tikv server.
func (s *RegionRequestSender) SendKVReq(req *kvrpcpb.Request, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.Response, error) {
	for {
		select {
		case <-s.bo.ctx.Done():
			return nil, errors.Trace(s.bo.ctx.Err())
		default:
		}

		ctx, err := s.regionCache.GetRPCContext(s.bo, regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ctx == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			return &kvrpcpb.Response{
				Type:        req.GetType(),
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}, nil
		}

		resp, retry, err := s.sendKVReqToRegion(ctx, req, timeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if retry {
			continue
		}

		if regionErr := resp.GetRegionError(); regionErr != nil {
			retry, err := s.onRegionError(ctx, regionErr)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if retry {
				continue
			}
			return resp, nil
		}

		if resp.GetType() != req.GetType() {
			return nil, errors.Trace(errMismatch(resp, req))
		}
		return resp, nil
	}
}

// SendCopReq sends a coprocessor request to tikv server.
func (s *RegionRequestSender) SendCopReq(req *coprocessor.Request, regionID RegionVerID, timeout time.Duration) (*coprocessor.Response, error) {
	for {
		ctx, err := s.regionCache.GetRPCContext(s.bo, regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if ctx == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			return &coprocessor.Response{
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}, nil
		}

		resp, retry, err := s.sendCopReqToRegion(ctx, req, timeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if retry {
			continue
		}

		if regionErr := resp.GetRegionError(); regionErr != nil {
			retry, err := s.onRegionError(ctx, regionErr)
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

func (s *RegionRequestSender) sendKVReqToRegion(ctx *RPCContext, req *kvrpcpb.Request, timeout time.Duration) (resp *kvrpcpb.Response, retry bool, err error) {
	req.Context = ctx.KVCtx
	resp, err = s.client.SendKVReq(ctx.Addr, req, timeout)
	if err != nil {
		if e := s.onSendFail(ctx, err); e != nil {
			return nil, false, errors.Trace(e)
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) sendCopReqToRegion(ctx *RPCContext, req *coprocessor.Request, timeout time.Duration) (resp *coprocessor.Response, retry bool, err error) {
	req.Context = ctx.KVCtx
	resp, err = s.client.SendCopReq(ctx.Addr, req, timeout)
	if err != nil {
		if e := s.onSendFail(ctx, err); e != nil {
			return nil, false, errors.Trace(err)
		}
		return nil, true, nil
	}
	return
}

func (s *RegionRequestSender) onSendFail(ctx *RPCContext, err error) error {
	s.regionCache.OnRequestFail(ctx)
	err = s.bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %s, try next peer later", err, ctx.KVCtx))
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
		log.Debugf("tikv reports `ServerIsBusy`, ctx: %s, retry later", ctx.KVCtx)
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
		return false, errors.New(regionErr.String())
	}
	// For other errors, we only drop cache here.
	// Because caller may need to re-split the request.
	log.Debugf("tikv reports region error: %s, ctx: %s", regionErr, ctx.KVCtx)
	s.regionCache.DropRegion(ctx.Region)
	return false, nil
}
