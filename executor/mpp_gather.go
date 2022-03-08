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

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func useMPPExecution(ctx sessionctx.Context, tr *plannercore.PhysicalTableReader) bool {
	if !ctx.GetSessionVars().IsMPPAllowed() {
		return false
	}
	_, ok := tr.GetTablePlan().(*plannercore.PhysicalExchangeSender)
	return ok
}

// MPPGather dispatch MPP tasks and read data from root tasks.
type MPPGather struct {
	// following fields are construct needed
	baseExecutor
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64

	mppReqs []*kv.MPPDispatchRequest

	respIter distsql.SelectResult
}

func (e *MPPGather) appendMPPDispatchReq(pf *plannercore.Fragment) error {
	dagReq, _, err := constructDAGReq(e.ctx, []plannercore.PhysicalPlan{pf.ExchangeSender}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.ExchangeSender.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !pf.IsRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
	}
	for _, mppTask := range pf.ExchangeSender.Tasks {
		err := updateExecutorTableID(context.Background(), dagReq.RootExecutor, mppTask.TableID, true)
		if err != nil {
			return errors.Trace(err)
		}
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", mppTask.StartTs), zap.Int64("ID", mppTask.ID), zap.String("address", mppTask.Meta.GetAddress()), zap.String("plan", plannercore.ToString(pf.ExchangeSender)))
		req := &kv.MPPDispatchRequest{
			Data:      pbData,
			Meta:      mppTask.Meta,
			ID:        mppTask.ID,
			IsRoot:    pf.IsRoot,
			Timeout:   10,
			SchemaVar: e.is.SchemaMetaVersion(),
			StartTs:   e.startTS,
			State:     kv.MppTaskReady,
		}
		e.mppReqs = append(e.mppReqs, req)
	}
	return nil
}

func collectPlanIDS(plan plannercore.PhysicalPlan, ids []int) []int {
	ids = append(ids, plan.ID())
	for _, child := range plan.Children() {
		ids = collectPlanIDS(child, ids)
	}
	return ids
}

// Open decides the task counts and locations and generate exchange operators for every plan fragment.
// Then dispatch tasks to tiflash stores. If any task fails, it would cancel the rest tasks.
func (e *MPPGather) Open(ctx context.Context) (err error) {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	sender := e.originalPlan.(*plannercore.PhysicalExchangeSender)
	planIDs := collectPlanIDS(e.originalPlan, nil)
	frags, err := plannercore.GenerateRootMPPTasks(e.ctx, e.startTS, sender, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for _, frag := range frags {
		err = e.appendMPPDispatchReq(frag)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if val, _err_ := failpoint.Eval(_curpkg_("checkTotalMPPTasks")); _err_ == nil {
		if val.(int) != len(e.mppReqs) {
			return errors.Errorf("The number of tasks is not right, expect %d tasks but actually there are %d tasks", val.(int), len(e.mppReqs))
		}
	}
	e.respIter, err = distsql.DispatchMPPTasks(ctx, e.ctx, e.mppReqs, e.retFieldTypes, planIDs, e.id, e.startTS)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.respIter.Next(ctx, chk)
	return errors.Trace(err)
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	e.mppReqs = nil
	if e.respIter != nil {
		return e.respIter.Close()
	}
	return nil
}
