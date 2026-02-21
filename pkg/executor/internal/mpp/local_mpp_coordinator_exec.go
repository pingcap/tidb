// Copyright 2023 PingCAP, Inc.
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

package mpp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

func (c *localMppCoordinator) handleMPPStreamResponse(bo *backoff.Backoffer, response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
	if response.Error != nil {
		c.mu.Lock()
		firstErrMsg := c.firstErrMsg
		c.mu.Unlock()
		// firstErrMsg is only used when already received error response from root tasks, avoid confusing error messages
		if len(firstErrMsg) > 0 {
			err = errors.Errorf("other error for mpp stream: %s", firstErrMsg)
		} else {
			err = errors.Errorf("other error for mpp stream: %s", response.Error.Msg)
		}
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", req.StartTs),
			zap.String("storeAddr", req.Meta.GetAddress()),
			zap.Int64("mpp-version", req.MppVersion.ToInt64()),
			zap.Int64("task-id", req.ID),
			zap.Error(err))
		return err
	}

	resp := &mppResponse{
		pbResp: response,
		detail: new(copr.CopRuntimeStats),
	}

	backoffTimes := bo.GetBackoffTimes()
	resp.detail.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(backoffTimes))
	for backoff := range backoffTimes {
		resp.detail.BackoffTimes[backoff] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
	}
	resp.detail.CalleeAddress = req.Meta.GetAddress()
	c.sendToRespCh(resp)
	return
}

func (c *localMppCoordinator) nextImpl(ctx context.Context) (resp *mppResponse, ok bool, exit bool, err error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-c.respChan:
			return
		case <-ticker.C:
			if c.vars != nil && c.vars.Killed != nil {
				killed := atomic.LoadUint32(c.vars.Killed)
				if killed != 0 {
					logutil.Logger(ctx).Info(
						"a killed signal is received",
						zap.Uint32("signal", killed),
					)
					err = derr.ErrQueryInterrupted
					exit = true
					return
				}
			}
		case <-c.finishCh:
			exit = true
			return
		case <-ctx.Done():
			if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
				close(c.finishCh)
			}
			exit = true
			return
		}
	}
}

// Next implements MppCoordinator interface
func (c *localMppCoordinator) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp, ok, closed, err := c.nextImpl(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err = c.sessionCtx.GetMPPClient().CheckVisibility(c.startTS)
	if err != nil {
		return nil, errors.Trace(derr.ErrQueryInterrupted)
	}
	return resp, nil
}

// Execute implements MppCoordinator interface
func (c *localMppCoordinator) Execute(ctx context.Context) (kv.Response, []kv.KeyRange, error) {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	sender := c.originalPlan.(*physicalop.PhysicalExchangeSender)
	sctx := c.sessionCtx
	frags, kvRanges, nodeInfo, err := physicalop.GenerateRootMPPTasks(sctx, c.startTS, c.gatherID, c.mppQueryID, sender, c.is)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if nodeInfo == nil {
		return nil, nil, errors.New("node info should not be nil")
	}
	c.nodeCnt = len(nodeInfo)

	var allTiFlashZoneInfo map[string]string
	if c.sessionCtx.GetStore() == nil {
		allTiFlashZoneInfo = make(map[string]string)
	} else if tikvStore, ok := c.sessionCtx.GetStore().(helper.Storage); ok {
		cache := tikvStore.GetRegionCache()
		allTiFlashStores := cache.GetTiFlashStores(tikv.LabelFilterNoTiFlashWriteNode)
		allTiFlashZoneInfo = make(map[string]string, len(allTiFlashStores))
		for _, tiflashStore := range allTiFlashStores {
			tiflashStoreAddr := tiflashStore.GetAddr()
			if tiflashZone, isSet := tiflashStore.GetLabelValue(placement.DCLabelKey); isSet {
				allTiFlashZoneInfo[tiflashStoreAddr] = tiflashZone
			}
		}
	} else {
		allTiFlashZoneInfo = make(map[string]string)
	}
	for _, frag := range frags {
		err = c.appendMPPDispatchReq(frag, allTiFlashZoneInfo)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	failpoint.Inject("checkTotalMPPTasks", func(val failpoint.Value) {
		if val.(int) != len(c.mppReqs) {
			failpoint.Return(nil, nil, errors.Errorf("The number of tasks is not right, expect %d tasks but actually there are %d tasks", val.(int), len(c.mppReqs)))
		}
	})

	ctx = distsql.WithSQLKvExecCounterInterceptor(ctx, sctx.GetSessionVars().StmtCtx.KvExecCounter)
	_, allowTiFlashFallback := sctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
	ctx = distsql.SetTiFlashConfVarsInContext(ctx, sctx.GetDistSQLCtx())
	c.needTriggerFallback = allowTiFlashFallback
	c.enableCollectExecutionInfo = config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()

	var ctxChild context.Context
	ctxChild, c.cancelFunc = context.WithCancel(ctx)
	go c.dispatchAll(ctxChild)

	return c, kvRanges, nil
}

// GetNodeCnt returns the node count that involved in the mpp computation.
func (c *localMppCoordinator) GetNodeCnt() int {
	return c.nodeCnt
}
