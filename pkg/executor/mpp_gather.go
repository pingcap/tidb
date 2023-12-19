// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/mpp"
	"github.com/pingcap/tidb/pkg/executor/mppcoordmanager"
	"github.com/pingcap/tidb/pkg/executor/mpperr"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

func useMPPExecution(ctx sessionctx.Context, tr *plannercore.PhysicalTableReader) bool {
	if !ctx.GetSessionVars().IsMPPAllowed() {
		return false
	}
	_, ok := tr.GetTablePlan().(*plannercore.PhysicalExchangeSender)
	return ok
}

func getMPPQueryID(ctx sessionctx.Context) uint64 {
	mppQueryInfo := &ctx.GetSessionVars().StmtCtx.MPPQueryInfo
	mppQueryInfo.QueryID.CompareAndSwap(0, plannercore.AllocMPPQueryID())
	return mppQueryInfo.QueryID.Load()
}

func getMPPQueryTS(ctx sessionctx.Context) uint64 {
	mppQueryInfo := &ctx.GetSessionVars().StmtCtx.MPPQueryInfo
	mppQueryInfo.QueryTS.CompareAndSwap(0, uint64(time.Now().UnixNano()))
	return mppQueryInfo.QueryTS.Load()
}

// MPPGather dispatch MPP tasks and read data from root tasks.
type MPPGather struct {
	// following fields are construct needed
	exec.BaseExecutor
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64
	mppQueryID   kv.MPPQueryID
	gatherID     uint64 // used for mpp_gather level retry, since each time should use different gatherIDs
	respIter     distsql.SelectResult

	memTracker *memory.Tracker

	// For virtual column.
	columns                    []*model.ColumnInfo
	virtualColumnIndex         []int
	virtualColumnRetFieldTypes []*types.FieldType

	// For UnionScan.
	table    table.Table
	kvRanges []kv.KeyRange
	dummy    bool

	mppErrRecovery *mpperr.RecoveryHandler

	// Only for MemLimit err recovery for now.
	// AutoScaler use this value as hint to scale out CN.
	nodeCnt int
}

func collectPlanIDS(plan plannercore.PhysicalPlan, ids []int) []int {
	ids = append(ids, plan.ID())
	for _, child := range plan.Children() {
		ids = collectPlanIDS(child, ids)
	}
	return ids
}

func (e *MPPGather) setupRespIter(ctx context.Context, isRecoverying bool) error {
	if isRecoverying {
		// If we are trying to recovery from MPP error, needs to cleanup some resources.
		// Sanity check.
		if e.dummy {
			return errors.New("should not reset mpp resp iter for dummy table")
		}
		if e.respIter == nil {
			return errors.New("mpp resp iter should already be setup")
		}

		if err := e.respIter.Close(); err != nil {
			return err
		}
		mppcoordmanager.InstanceMPPCoordinatorManager.Unregister(mppcoordmanager.CoordinatorUniqueID{MPPQueryID: e.mppQueryID, GatherID: e.gatherID})
	}

	planIDs := collectPlanIDS(e.originalPlan, nil)
	e.gatherID = allocMPPGatherID(e.Ctx())
	coord := e.buildCoordinator(planIDs)
	err := mppcoordmanager.InstanceMPPCoordinatorManager.Register(mppcoordmanager.CoordinatorUniqueID{MPPQueryID: e.mppQueryID, GatherID: e.gatherID}, coord)
	if err != nil {
		return err
	}
	var resp kv.Response
	resp, e.kvRanges, err = coord.Execute(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.nodeCnt = coord.GetNodeCnt()
	e.respIter = distsql.GenSelectResultFromResponse(e.Ctx(), e.RetFieldTypes(), planIDs, e.ID(), resp)
	return nil
}

// allocMPPGatherID allocates mpp gather id for mpp gathers. It will reset the gather id when the query finished.
// To support mpp_gather level cancel/retry and mpp_gather under apply executors, need to generate incremental ids when Open function is invoked
func allocMPPGatherID(ctx sessionctx.Context) uint64 {
	mppQueryInfo := &ctx.GetSessionVars().StmtCtx.MPPQueryInfo
	return mppQueryInfo.AllocatedMPPGatherID.Add(1)
}

// Open builds coordinator and invoke coordinator's Execute function to execute physical plan
// If any task fails, it would cancel the rest tasks.
func (e *MPPGather) Open(ctx context.Context) (err error) {
	if e.dummy {
		sender, ok := e.originalPlan.(*plannercore.PhysicalExchangeSender)
		if !ok {
			return errors.Errorf("unexpected plan type, expect: PhysicalExchangeSender, got: %s", e.originalPlan.TP())
		}
		_, e.kvRanges, err = plannercore.GenerateRootMPPTasks(e.Ctx(), e.startTS, e.gatherID, e.mppQueryID, sender, e.is)
		return err
	}
	err = e.setupRespIter(ctx, false)
	if err != nil {
		return err
	}

	// Default hold 3 chunks.
	holdCap := 3 * e.Ctx().GetSessionVars().MaxChunkSize

	enableMPPRecovery := true
	useAutoScaler := config.GetGlobalConfig().UseAutoScaler
	disaggTiFlash := config.GetGlobalConfig().DisaggregatedTiFlash

	// For now, mpp err recovery only support MemLimit, which is only useful when AutoScaler is used.
	// So disable recovery in normal case.
	if !disaggTiFlash || !useAutoScaler {
		enableMPPRecovery = false
	}
	// For cache table, will not dispatch tasks to TiFlash, so no need to recovery.
	if e.dummy {
		enableMPPRecovery = false
	}

	e.mppErrRecovery = mpperr.NewRecoveryHandler(useAutoScaler, uint64(holdCap), enableMPPRecovery, e.memTracker)
	return nil
}

func (e *MPPGather) buildCoordinator(planIDs []int) kv.MppCoordinator {
	_, serverAddr := mppcoordmanager.InstanceMPPCoordinatorManager.GetServerAddr()
	coord := mpp.NewLocalMPPCoordinator(e.Ctx(), e.is, e.originalPlan, planIDs, e.startTS, e.mppQueryID, e.gatherID, serverAddr, e.memTracker)
	return coord
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.dummy {
		return nil
	}

	for e.mppErrRecovery.CanHoldResult(nil) {
		tmpChk := exec.NewFirstChunk(e)
		if mppErr := e.respIter.Next(ctx, tmpChk); mppErr != nil {
			err := e.mppErrRecovery.Recovery(&mpperr.RecoveryInfo{
				MPPErr:  mppErr,
				NodeCnt: e.nodeCnt,
			})
			if err != nil {
				logutil.BgLogger().Error("recovery mpp error failed", zap.Any("mppErr", mppErr),
					zap.Any("recoveryErr", err))
				return mppErr
			}

			logutil.BgLogger().Info("recovery mpp error succeed, begin next retry", zap.Any("mppErr", mppErr))
			if err = e.setupRespIter(ctx, true); err != nil {
				logutil.BgLogger().Error("setup resp iter when recovery mpp err failed", zap.Any("err", err))
				return mppErr
			}
			e.mppErrRecovery.ResetHolder()

			continue
		}

		if tmpChk.NumRows() == 0 {
			break
		}

		e.mppErrRecovery.HoldResult(tmpChk)
	}

	if e.mppErrRecovery.NumHoldChk() != 0 {
		var tmpChk *chunk.Chunk
		if tmpChk := e.mppErrRecovery.PopFrontChk(); tmpChk == nil {
			return errors.New("cannot get chunk from mpp result holder")
		}
		chk.SwapColumns(tmpChk)
	} else if err := e.respIter.Next(ctx, chk); err != nil {
		return err
	}

	err := table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.Schema().Columns, e.columns, e.Ctx(), chk)
	if err != nil {
		return err
	}
	return nil
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	var err error
	if e.dummy {
		return nil
	}
	if e.respIter != nil {
		err = e.respIter.Close()
	}
	mppcoordmanager.InstanceMPPCoordinatorManager.Unregister(mppcoordmanager.CoordinatorUniqueID{MPPQueryID: e.mppQueryID, GatherID: e.gatherID})
	if err != nil {
		return err
	}
	if e.mppErrRecovery != nil {
		e.mppErrRecovery.ResetHolder()
	}
	return nil
}

// Table implements the dataSourceExecutor interface.
func (e *MPPGather) Table() table.Table {
	return e.table
}

func (e *MPPGather) setDummy() {
	e.dummy = true
}
