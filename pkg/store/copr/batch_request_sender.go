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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegionInfo contains region related information for batchCopTask
type RegionInfo struct {
	Region         tikv.RegionVerID
	Meta           *metapb.Region
	Ranges         *KeyRanges
	AllStores      []uint64
	PartitionIndex int64 // used by PartitionTableScan, indicates the n-th partition of the partition table
}

func (ri *RegionInfo) toCoprocessorRegionInfo() *coprocessor.RegionInfo {
	return &coprocessor.RegionInfo{
		RegionId: ri.Region.GetID(),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: ri.Region.GetConfVer(),
			Version: ri.Region.GetVer(),
		},
		Ranges: ri.Ranges.ToPBRanges(),
	}
}

// RegionBatchRequestSender sends BatchCop requests to TiFlash server by stream way.
type RegionBatchRequestSender struct {
	*tikv.RegionRequestSender
	enableCollectExecutionInfo bool
}

// NewRegionBatchRequestSender creates a RegionBatchRequestSender object.
func NewRegionBatchRequestSender(cache *RegionCache, client tikv.Client, enableCollectExecutionInfo bool) *RegionBatchRequestSender {
	return &RegionBatchRequestSender{
		RegionRequestSender:        tikv.NewRegionRequestSender(cache.RegionCache, client),
		enableCollectExecutionInfo: enableCollectExecutionInfo,
	}
}

// SendReqToAddr send batch cop request
func (ss *RegionBatchRequestSender) SendReqToAddr(bo *Backoffer, rpcCtx *tikv.RPCContext, regionInfos []RegionInfo, req *tikvrpc.Request, timout time.Duration) (resp *tikvrpc.Response, retry bool, cancel func(), err error) {
	cancel = func() {}
	if e := tikvrpc.SetContext(req, rpcCtx.Meta, rpcCtx.Peer); e != nil {
		return nil, false, cancel, errors.Trace(e)
	}
	ctx := bo.GetCtx()
	if rawHook := ctx.Value(tikv.RPCCancellerCtxKey{}); rawHook != nil {
		ctx, cancel = rawHook.(*tikv.RPCCanceller).WithCancel(ctx)
	}
	start := time.Now()
	resp, err = ss.GetClient().SendRequest(ctx, rpcCtx.Addr, req, timout)
	if ss.Stats != nil && ss.enableCollectExecutionInfo {
		ss.Stats.RecordRPCRuntimeStats(req.Type, time.Since(start))
	}
	if err != nil {
		cancel()
		ss.SetRPCError(err)
		e := ss.onSendFailForBatchRegions(bo, rpcCtx, regionInfos, err)
		if e != nil {
			return nil, false, func() {}, errors.Trace(e)
		}
		return nil, true, func() {}, nil
	}
	// We don't need to process region error or lock error. Because TiFlash will retry by itself.
	return
}

func (ss *RegionBatchRequestSender) onSendFailForBatchRegions(bo *Backoffer, ctx *tikv.RPCContext, regionInfos []RegionInfo, err error) error {
	// If it failed because the context is cancelled by ourself, don't retry.
	if errors.Cause(err) == context.Canceled || status.Code(errors.Cause(err)) == codes.Canceled {
		return errors.Trace(err)
	} else if tikv.LoadShuttingDown() > 0 {
		return tikverr.ErrTiDBShuttingDown
	}

	if !config.GetGlobalConfig().DisaggregatedTiFlash {
		// The reload region param is always true. Because that every time we try, we must
		// re-build the range then re-create the batch sender. As a result, the len of "failStores"
		// will change. If tiflash's replica is more than two, the "reload region" will always be false.
		// Now that the batch cop and mpp has a relative low qps, it's reasonable to reload every time
		// when meeting io error.
		rc := RegionCache{ss.GetRegionCache()}
		rc.OnSendFailForBatchRegions(bo, ctx.Store, regionInfos, true, err)
	}

	// Retry on send request failure when it's not canceled.
	// When a store is not available, the leader of related region should be elected quickly.
	// TODO: the number of retry time should be limited:since region may be unavailable
	// when some unrecoverable disaster happened.
	err = bo.Backoff(tikv.BoTiFlashRPC(), errors.Errorf("send request error: %v, ctx: %v, regionInfos: %v", err, ctx, regionInfos))
	return errors.Trace(err)
}
