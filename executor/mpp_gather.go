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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
)

// Currently we only use mpp for broadcast join.
func useMPPExecution(ctx sessionctx.Context, tr *plannercore.PhysicalTableReader) bool {
	if !ctx.GetSessionVars().AllowMPPExecution {
		return false
	}
	if tr.StoreType != kv.TiFlash {
		return false
	}
	return true
}

// MPPGather dispatch MPP tasks and read data from root tasks.
type MPPGather struct {
	// following fields are construct needed
	baseExecutor
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64

	allocTaskID int64
	mppReqs     []*kv.MPPDispatchRequest

	respIter distsql.SelectResult
}

func (e *MPPGather) constructMPPTasksImpl(ctx context.Context, p *plannercore.Fragment) ([]*kv.MPPTask, error) {
	isCommonHandle := p.TableScan.Table.IsCommonHandle
	if p.TableScan.Table.GetPartitionInfo() == nil {
		return e.constructSinglePhysicalTable(ctx, p.TableScan.Table.ID, isCommonHandle, p.TableScan.Ranges)
	}
	tmp, _ := e.is.TableByID(p.TableScan.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(e.ctx, tbl, p.TableScan.PartitionInfo.PruningConds, p.TableScan.PartitionInfo.PartitionNames, p.TableScan.PartitionInfo.Columns, p.TableScan.PartitionInfo.ColumnNames)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allTasks := make([]*kv.MPPTask, 0)
	for _, part := range partitions {
		partTasks, err := e.constructSinglePhysicalTable(ctx, part.GetPhysicalID(), isCommonHandle, p.TableScan.Ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allTasks = append(allTasks, partTasks...)
	}
	return allTasks, nil
}

// single physical table means a table without partitions or a single partition in a partition table.
func (e *MPPGather) constructSinglePhysicalTable(ctx context.Context, tableID int64, isCommonHandle bool, ranges []*ranger.Range) ([]*kv.MPPTask, error) {
	kvRanges, err := distsql.TableHandleRangesToKVRanges(e.ctx.GetSessionVars().StmtCtx, []int64{tableID}, isCommonHandle, ranges, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := &kv.MPPBuildTasksRequest{KeyRanges: kvRanges}
	metas, err := e.ctx.GetMPPClient().ConstructMPPTasks(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tasks := make([]*kv.MPPTask, 0, len(metas))
	for _, meta := range metas {
		e.allocTaskID++
		tasks = append(tasks, &kv.MPPTask{Meta: meta, ID: e.allocTaskID, StartTs: e.startTS, TableID: tableID})
	}
	return tasks, nil
}

func (e *MPPGather) appendMPPDispatchReq(pf *plannercore.Fragment, tasks []*kv.MPPTask, isRoot bool) error {
	dagReq, _, err := constructDAGReq(e.ctx, []plannercore.PhysicalPlan{pf.ExchangeSender}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !isRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
	}
	for _, mppTask := range tasks {
		err := updateExecutorTableID(context.Background(), dagReq.RootExecutor, mppTask.TableID, true)
		if err != nil {
			return errors.Trace(err)
		}
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		req := &kv.MPPDispatchRequest{
			Data:      pbData,
			Meta:      mppTask.Meta,
			ID:        mppTask.ID,
			IsRoot:    isRoot,
			Timeout:   10,
			SchemaVar: e.is.SchemaMetaVersion(),
			StartTs:   e.startTS,
		}
		e.mppReqs = append(e.mppReqs, req)
	}
	return nil
}

func (e *MPPGather) constructMPPTasks(ctx context.Context, pf *plannercore.Fragment, isRoot bool) ([]*kv.MPPTask, error) {
	tasks, err := e.constructMPPTasksImpl(ctx, pf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, client := range pf.ExchangeReceivers {
		client.ChildPf.ExchangeSender.Tasks = tasks
		client.Tasks, err = e.constructMPPTasks(ctx, client.ChildPf, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	err = e.appendMPPDispatchReq(pf, tasks, isRoot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tasks, nil
}

// Open decides the task counts and locations and generate exchange operators for every plan fragment.
// Then dispatch tasks to tiflash stores. If any task fails, it would cancel the rest tasks.
// TODO: We should retry when the request fails for pure rpc error.
func (e *MPPGather) Open(ctx context.Context) error {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	rootPf := plannercore.GetRootPlanFragments(e.ctx, e.originalPlan, e.startTS)
	_, err := e.constructMPPTasks(ctx, rootPf, true)
	if err != nil {
		return errors.Trace(err)
	}

	e.respIter, err = distsql.DispatchMPPTasks(ctx, e.ctx, e.mppReqs, e.retFieldTypes)
	if err != nil {
		return errors.Trace(err)
	}
	e.respIter.Fetch(ctx)
	return nil
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.respIter.Next(ctx, chk)
	return errors.Trace(err)
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	if e.respIter != nil {
		return e.respIter.Close()
	}
	return nil
}
