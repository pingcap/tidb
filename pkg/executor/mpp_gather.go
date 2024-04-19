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
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/mpp"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

// For mpp err recovery, hold at most 4 * MaxChunkSize rows.
const mppErrRecoveryHoldChkCap = 4

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

func collectPlanIDs(plan base.PhysicalPlan, ids []int) []int {
	ids = append(ids, plan.ID())
	for _, child := range plan.Children() {
		ids = collectPlanIDs(child, ids)
	}
	return ids
}

// MPPGather dispatch MPP tasks and read data from root tasks.
type MPPGather struct {
	exec.BaseExecutor
	is           infoschema.InfoSchema
	originalPlan base.PhysicalPlan
	startTS      uint64
	mppQueryID   kv.MPPQueryID
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

	mppExec *mpp.ExecutorWithRetry
}

// Open implements the Executor Open interface.
func (e *MPPGather) Open(ctx context.Context) (err error) {
	if e.dummy {
		sender, ok := e.originalPlan.(*plannercore.PhysicalExchangeSender)
		if !ok {
			return errors.Errorf("unexpected plan type, expect: PhysicalExchangeSender, got: %s", e.originalPlan.TP())
		}
		if _, e.kvRanges, _, err = plannercore.GenerateRootMPPTasks(e.Ctx(), e.startTS, 0, e.mppQueryID, sender, e.is); err != nil {
			return nil
		}
	}
	planIDs := collectPlanIDs(e.originalPlan, nil)
	if e.mppExec, err = mpp.NewExecutorWithRetry(ctx, e.Ctx(), e.memTracker, planIDs, e.originalPlan, e.startTS, e.mppQueryID, e.is); err != nil {
		return err
	}
	e.kvRanges = e.mppExec.KVRanges
	e.respIter = distsql.GenSelectResultFromMPPResponse(e.Ctx().GetDistSQLCtx(), e.RetFieldTypes(), planIDs, e.ID(), e.mppExec)
	return nil
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.dummy {
		return nil
	}
	if err := e.respIter.Next(ctx, chk); err != nil {
		return err
	}
	if chk.NumRows() == 0 {
		return nil
	}

	return table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.Schema().Columns, e.columns, e.Ctx().GetExprCtx(), chk)
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	if e.dummy {
		return nil
	}
	if e.respIter != nil {
		return e.respIter.Close()
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
