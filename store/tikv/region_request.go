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
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// sendKVReq sends req to tikv server. It will retry internally to find the right
// region leader if i) fails to establish a connection to server or ii) server
// returns `NotLeader`.
func sendKVReq(regionCache *RegionCache, rpcClient Client, bo *Backoffer, req *kvrpcpb.Request, regionID RegionVerID, timeout time.Duration) (*kvrpcpb.Response, error) {
	for {
		select {
		case <-bo.ctx.Done():
			return nil, errors.Trace(bo.ctx.Err())
		default:
		}

		region := regionCache.GetRegionByVerID(regionID)
		if region == nil {
			// If the region is not found in cache, it must be out
			// of date and already be cleaned up. We can skip the
			// RPC by returning RegionError directly.
			return &kvrpcpb.Response{
				Type:        req.GetType(),
				RegionError: &errorpb.Error{StaleEpoch: &errorpb.StaleEpoch{}},
			}, nil
		}
		req.Context = region.GetContext()
		resp, err := rpcClient.SendKVReq(region.GetAddress(), req, timeout)
		if err != nil {
			regionCache.NextPeer(region.VerID())
			err = bo.Backoff(boTiKVRPC, errors.Errorf("send tikv request error: %v, ctx: %s, try next peer later", err, req.Context))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if regionErr := resp.GetRegionError(); regionErr != nil {
			reportRegionError(regionErr)
			// Retry if error is `NotLeader`.
			if notLeader := regionErr.GetNotLeader(); notLeader != nil {
				log.Warnf("tikv reports `NotLeader`: %s, ctx: %s, retry later", notLeader, req.Context)
				regionCache.UpdateLeader(region.VerID(), notLeader.GetLeader().GetId())
				if notLeader.GetLeader() == nil {
					err = bo.Backoff(boRegionMiss, errors.Errorf("not leader: %v, ctx: %s", notLeader, req.Context))
					if err != nil {
						return nil, errors.Trace(err)
					}
				}
				continue
			}
			if staleEpoch := regionErr.GetStaleEpoch(); staleEpoch != nil {
				log.Warnf("tikv reports `StaleEpoch`, ctx: %s, retry later", req.Context)
				err = regionCache.OnRegionStale(region, staleEpoch.NewRegions)
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}
			// Retry if the error is `ServerIsBusy`.
			if regionErr.GetServerIsBusy() != nil {
				log.Warnf("tikv reports `ServerIsBusy`, ctx: %s, retry later", req.Context)
				err = bo.Backoff(boServerBusy, errors.Errorf("server is busy"))
				if err != nil {
					return nil, errors.Trace(err)
				}
				continue
			}
			// For other errors, we only drop cache here.
			// Because caller may need to re-split the request.
			log.Warnf("tikv reports region error: %s, ctx: %s", resp.GetRegionError(), req.Context)
			regionCache.DropRegion(region.VerID())
			return resp, nil
		}
		if resp.GetType() != req.GetType() {
			return nil, errors.Trace(errMismatch(resp, req))
		}
		return resp, nil
	}
}
