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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/mppcoordmanager"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type ExecutorWithRetry struct {
	sctx sessionctx.Context
	// mppErrRecovery is designed for the recovery of MPP errors.
	// Basic idea:
	// 1. It attempts to hold the results of MPP. During the holding process, if an error occurs, it starts error recovery.
	//    If the recovery is successful, it discards held results and reconstructs the respIter, then re-executes the MPP task.
	//    If the recovery fails, an error is reported directly.
	// 2. If the held MPP results exceed the capacity, will starts returning results to caller.
	//    Once the results start being returned, error recovery cannot be performed anymore.
	mppErrRecovery *RecoveryHandler

	gatherID uint64
	startTS  uint64
	queryID  kv.MPPQueryID

	coord kv.MppCoordinator

	planIDs []int
	is      infoschema.InfoSchema
	plan    plannercore.PhysicalPlan

	memTracker *memory.Tracker

	// Only for MemLimit err recovery for now.
	// AutoScaler use this value as hint to scale out CN.
	nodeCnt int
}

var _ kv.Response = &ExecutorWithRetry{}

const mppErrRecoveryHoldChkCap = 4

func NewRetryer(sctx sessionctx.Context, memTracker *memory.Tracker, planIDs []int,
	plan plannercore.PhysicalPlan, startTS uint64, queryID kv.MPPQueryID,
	dummy bool) *ExecutorWithRetry {
	// TODO: After add row info in tipb.DataPacket, we can use row count as capacity.
	// For now, use the number of tipb.DataPacket as capacity.
	const holdCap = 1

	disaggTiFlashWithAutoScaler := config.GetGlobalConfig().DisaggregatedTiFlash && config.GetGlobalConfig().UseAutoScaler
	_, allowTiFlashFallback := sctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]

	// 1. For now, mpp err recovery only support MemLimit, which is only useful when AutoScaler is used.
	// 2. When enable fallback to tikv, the returned mpp err will be ErrTiFlashServerTimeout,
	//    which we cannot handle for now. Also there is no need to recovery because tikv will retry the query.
	// 3. For cached table, will not dispatch tasks to TiFlash, so no need to recovery.
	enableMPPRecovery := disaggTiFlashWithAutoScaler && !allowTiFlashFallback && !dummy
	recoveryHandler := NewRecoveryHandler(disaggTiFlashWithAutoScaler,
		uint64(holdCap), enableMPPRecovery, memTracker)
	retryer := &ExecutorWithRetry{
		sctx:           sctx,
		mppErrRecovery: recoveryHandler,
		startTS:        startTS,
		queryID:        queryID,
		// gjt todo: child tracker?
		memTracker: memTracker,
	}
	retryer.setupMPPCoordinator(false)

	return retryer
}

func (r *ExecutorWithRetry) Next(ctx context.Context) (resp kv.ResultSubset, err error) {
	if err = r.nextWithRecovery(ctx); err != nil {
		return nil, err
	}

	if r.mppErrRecovery.NumHoldResp() != 0 {
		if resp = r.mppErrRecovery.PopFrontResp(); resp == nil {
			return nil, errors.New("cannot get resp from mpp result holder")
		}
	} else if resp, err = r.coord.Next(ctx); err != nil {
		return nil, err
	}
	return resp, err
}

func (r *ExecutorWithRetry) Close() error {
	mppcoordmanager.InstanceMPPCoordinatorManager.Unregister(r.getCoordUniqueID())
	return r.coord.Close()
}

func (r *ExecutorWithRetry) setupMPPCoordinator(recoverying bool) error {
	if recoverying {
		// Sanity check.
		if r.coord == nil {
			return errors.New("mpp coordinator should not be nil when recoverying")
		}
		mppcoordmanager.InstanceMPPCoordinatorManager.Unregister(r.getCoordUniqueID())
	}

	// Make sure gatherID is updated before build coord.
	r.gatherID = allocMPPGatherID(r.sctx)

	r.coord = r.buildCoordinator()
	mppcoordmanager.InstanceMPPCoordinatorManager.Register(r.getCoordUniqueID(), r.coord)
	if r.nodeCnt = r.coord.GetNodeCnt(); r.nodeCnt <= 0 {
		return errors.Errorf("tiflash node count should be greater than zero: %v", r.nodeCnt)
	}
	return nil
}

func (r *ExecutorWithRetry) nextWithRecovery(ctx context.Context) error {
	if !r.mppErrRecovery.Enabled() {
		return nil
	}

	for r.mppErrRecovery.CanHoldResult() {
		resp, mppErr := r.coord.Next(ctx)

		// Mock recovery n times.
		failpoint.Inject("mpp_recovery_test_max_err_times", func(forceErrCnt failpoint.Value) {
			forceErrCntInt := forceErrCnt.(int)
			if r.mppErrRecovery.RecoveryCnt() < uint32(forceErrCntInt) {
				mppErr = errors.New("mock mpp error")
			}
		})

		if mppErr != nil {
			recoveryErr := r.mppErrRecovery.Recovery(&RecoveryInfo{
				MPPErr:  mppErr,
				NodeCnt: r.nodeCnt,
			})

			// Mock recovery succeed, ignore no recovery handler err.
			failpoint.Inject("mpp_recovery_test_ignore_recovery_err", func() {
				if recoveryErr == nil {
					panic("mocked mpp err should got recovery err")
				}
				if strings.Contains(recoveryErr.Error(), "no handler to recovery") {
					recoveryErr = nil
				}
			})

			if recoveryErr != nil {
				logutil.BgLogger().Error("recovery mpp error failed", zap.Any("mppErr", mppErr),
					zap.Any("recoveryErr", recoveryErr))
				return mppErr
			}

			logutil.BgLogger().Info("recovery mpp error succeed, begin next retry",
				zap.Any("mppErr", mppErr), zap.Any("recoveryCnt", r.mppErrRecovery.RecoveryCnt()))

			if err := r.setupMPPCoordinator(true); err != nil {
				logutil.BgLogger().Error("setup resp iter when recovery mpp err failed", zap.Any("err", err))
				return mppErr
			}
			r.mppErrRecovery.ResetHolder()

			continue
		}

		if resp == nil {
			break
		}

		r.mppErrRecovery.HoldResult(resp.(*mppResponse))
	}

	failpoint.Inject("mpp_recovery_test_hold_size", func(num failpoint.Value) {
		// Note: this failpoint only execute once.
		curRows := r.mppErrRecovery.NumHoldResp()
		numInt := num.(int)
		if curRows != numInt {
			panic(fmt.Sprintf("unexpected holding rows, cur: %d", curRows))
		}
	})
	return nil
}

func allocMPPGatherID(ctx sessionctx.Context) uint64 {
	mppQueryInfo := &ctx.GetSessionVars().StmtCtx.MPPQueryInfo
	return mppQueryInfo.AllocatedMPPGatherID.Add(1)
}

func (r *ExecutorWithRetry) buildCoordinator() kv.MppCoordinator {
	_, serverAddr := mppcoordmanager.InstanceMPPCoordinatorManager.GetServerAddr()
	return NewLocalMPPCoordinator(r.sctx, r.is, r.plan, r.planIDs, r.startTS, r.queryID,
		r.gatherID, serverAddr, r.memTracker)
}

func (r *ExecutorWithRetry) getCoordUniqueID() mppcoordmanager.CoordinatorUniqueID {
	return mppcoordmanager.CoordinatorUniqueID{
		MPPQueryID: r.queryID,
		GatherID:   r.gatherID,
	}
}
