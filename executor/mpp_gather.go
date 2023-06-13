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
	"github.com/pingcap/tidb/executor/mppcoordmanager"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/executor/internal/mpp"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
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
	baseExecutor
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64
	mppQueryID   kv.MPPQueryID
	respIter     distsql.SelectResult

	memTracker *memory.Tracker

	columns                    []*model.ColumnInfo
	virtualColumnIndex         []int
	virtualColumnRetFieldTypes []*types.FieldType
}

func collectPlanIDS(plan plannercore.PhysicalPlan, ids []int) []int {
	ids = append(ids, plan.ID())
	for _, child := range plan.Children() {
		ids = collectPlanIDS(child, ids)
	}
	return ids
}

// Open builds coordinator and invoke coordinator's Execute function to execute physical plan
// If any task fails, it would cancel the rest tasks.
func (e *MPPGather) Open(ctx context.Context) (err error) {
	planIDs := collectPlanIDS(e.originalPlan, nil)
	coord := e.buildCoordinator(planIDs)
	mppcoordmanager.InstanceMPPCoordinatorManager.Register(mppcoordmanager.CoordinatorUniqueID{MPPQueryID: e.mppQueryID, GatherId: uint64(e.id)}, coord)
	resp, err := coord.Execute(ctx)
	if err != nil {
		return errors.Trace(err)
	}


	e.respIter = distsql.GenSelectResultFromResponse(e.ctx, e.retFieldTypes, planIDs, e.id, resp)
	time.Sleep(time.Second*2)
	coord.Close()
	return nil
}

func (e *MPPGather) buildCoordinator(planIDs []int) kv.MppCoordinator {
	coord := mpp.NewLocalMPPCoordinator(e.ctx, e.is, e.originalPlan, planIDs, e.startTS, e.mppQueryID, uint64(e.id), e.memTracker)
	return coord
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.respIter.Next(ctx, chk)
	if err != nil {
		return err
	}
	err = table.FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.schema.Columns, e.columns, e.ctx, chk)
	if err != nil {
		return err
	}
	return nil
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	var err error
	if e.respIter != nil {
		e.respIter.Dummy()
		err = e.respIter.Close()
	}
	if err != nil {
		return err
	}
	mppcoordmanager.InstanceMPPCoordinatorManager.Unregister(mppcoordmanager.CoordinatorUniqueID{MPPQueryID: e.mppQueryID, GatherId: uint64(e.id)})
	return nil
}
