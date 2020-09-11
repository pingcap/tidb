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
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/plancodec"
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
	_, ok := tr.GetTablePlan().(*plannercore.PhysicalBroadCastJoin)
	return ok
}

type mppTask struct {
	taskInfo kv.MPPTask // on which store this task will execute
	id       int64      // mppTaskID
	startTs  uint64
	tableID  int64 // physical table id
}

// ToPB generates the pb structure.
func (t *mppTask) ToPB() *mpp.TaskMeta {
	meta := &mpp.TaskMeta{
		QueryTs: t.startTs,
		TaskId:  t.id,
	}
	if t.id != -1 {
		meta.Address = t.taskInfo.GetAddress()
	}
	return meta
}

// planFragment is cut from the whole pushed-down plan by pipeline breaker.
// Communication by pfs are always through shuffling / broadcasting / passing through.
type planFragment struct {
	p plannercore.PhysicalPlan

	/// following field are filled during getPlanFragment.
	// TODO: Strictly speaking, not all plan fragment contain table scan. we can do this assumption until more plans are supported.
	tableScan       *plannercore.PhysicalTableScan // result physical table scan
	exchangeClients []*ExchangeClient              // data receivers

	// following fields are filled after scheduling.
	exchangeServer *ExchangeServer // data exporter
}

// ExchangeClient establishes connection actively and receives data passively.
type ExchangeClient struct {
	plannercore.PhysicalExchangerBase

	tasks   []*mppTask
	childPf *planFragment
	schema  *expression.Schema
}

// ToPB generates the pb structure.
func (e *ExchangeClient) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {

	encodedTask := make([][]byte, 0, len(e.tasks))

	for _, task := range e.tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	columnInfos := make([]*model.ColumnInfo, 0, len(e.schema.Columns))
	for _, column := range e.schema.Columns {
		columnInfos = append(columnInfos, column.ToInfo())
	}
	ecExec := &tipb.ExchangeClient{
		EncodedTaskMeta: encodedTask,
		Columns:         util.ColumnsToProto(columnInfos, false),
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:             tipb.ExecType_TypeExchangeClient,
		ExchangeClient: ecExec,
		ExecutorId:     &executorID,
	}, nil
}

// ExchangeServer dispatches data to upstream tasks. That means push mode processing,
type ExchangeServer struct {
	plannercore.PhysicalExchangerBase

	tasks        []*mppTask
	exchangeType tipb.ExchangeType
}

// ToPB generates the pb structure.
func (e *ExchangeServer) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
	child, err := e.Children()[0].ToPB(ctx, kv.TiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}

	encodedTask := make([][]byte, 0, len(e.tasks))

	for _, task := range e.tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	ecExec := &tipb.ExchangeServer{
		Tp:              e.exchangeType,
		EncodedTaskMeta: encodedTask,
		Child:           child,
	}
	// TODO: Refine the executor ID. We should use integer executor id for best implementation.
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:             tipb.ExecType_TypeExchangeServer,
		ExchangeServer: ecExec,
		ExecutorId:     &executorID,
	}, nil
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

func (e *MPPGather) schedulePlanFragmentImpl(ctx context.Context, p *planFragment) ([]*mppTask, error) {
	if p.tableScan.Table.GetPartitionInfo() == nil {
		return e.scheduleSinglePhysicalTable(ctx, p.tableScan.Table.ID, p.tableScan.Ranges)
	}
	tmp, _ := e.is.TableByID(p.tableScan.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(e.ctx, tbl, p.tableScan.PartitionInfo.PruningConds, p.tableScan.PartitionInfo.PartitionNames, p.tableScan.PartitionInfo.Columns, p.tableScan.PartitionInfo.ColumnNames)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allTasks := make([]*mppTask, 0)
	for _, part := range partitions {
		partTasks, err := e.scheduleSinglePhysicalTable(ctx, part.GetPhysicalID(), p.tableScan.Ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allTasks = append(allTasks, partTasks...)
	}
	return allTasks, nil
}

func (e *MPPGather) scheduleSinglePhysicalTable(ctx context.Context, tableID int64, ranges []*ranger.Range) ([]*mppTask, error) {
	kvRanges := distsql.TableRangesToKVRanges(tableID, ranges, nil)
	req := &kv.MPPScheduleRequest{KeyRanges: kvRanges}
	stores, err := e.ctx.GetMPPClient().ScheduleMPPTasks(ctx, req)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tasks := make([]*mppTask, 0, len(stores))
	for _, store := range stores {
		e.allocTaskID++
		tasks = append(tasks, &mppTask{taskInfo: store, id: e.allocTaskID, startTs: e.startTS, tableID: tableID})
	}
	return tasks, nil
}

func (e *MPPGather) getPlanFragments(p plannercore.PhysicalPlan, pf *planFragment) {
	switch x := p.(type) {
	case *plannercore.PhysicalTableScan:
		pf.tableScan = x
	case *plannercore.PhysicalBroadCastJoin:
		// This is a pipeline breaker. So we replace broadcast side with a exchangerClient
		bcChild := x.Children()[x.InnerChildIdx]
		exc := &ExchangeServer{exchangeType: tipb.ExchangeType_Broadcast}
		exc.InitBasePlan(e.ctx, plancodec.TypeExchangeServer)
		npf := &planFragment{p: bcChild, exchangeServer: exc}
		exchangeClient := &ExchangeClient{
			childPf: npf,
			schema:  bcChild.Schema(),
		}
		exchangeClient.InitBasePlan(e.ctx, plancodec.TypeExchangeClient)
		x.Children()[x.InnerChildIdx] = exchangeClient
		pf.exchangeClients = append(pf.exchangeClients, exchangeClient)

		// For the inner side of join, we use a new plan fragment.
		e.getPlanFragments(bcChild, npf)
		e.getPlanFragments(x.Children()[1-x.InnerChildIdx], pf)
	default:
		if len(x.Children()) > 0 {
			e.getPlanFragments(x.Children()[0], pf)
		}
	}
}

func (e *MPPGather) appendMPPDispatchReq(pf *planFragment, tasks []*mppTask, isRoot bool) error {
	pf.exchangeServer.SetChildren(pf.p)
	dagReq, _, err := constructDAGReq(e.ctx, []plannercore.PhysicalPlan{pf.exchangeServer}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.p.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	for _, mppTask := range tasks {
		err := updateExecutorTableID(context.Background(), dagReq.RootExecutor, mppTask.tableID, true)
		if err != nil {
			return errors.Trace(err)
		}
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		req := &kv.MPPDispatchRequest{
			Data:      pbData,
			Task:      mppTask.taskInfo,
			ID:        mppTask.id,
			IsRoot:    isRoot,
			Timeout:   10,
			SchemaVar: e.is.SchemaMetaVersion(),
			StartTs:   e.startTS,
		}
		e.mppReqs = append(e.mppReqs, req)
	}
	return nil
}

func (e *MPPGather) schedulePlanFragment(ctx context.Context, pf *planFragment, isRoot bool) ([]*mppTask, error) {
	tasks, err := e.schedulePlanFragmentImpl(ctx, pf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, client := range pf.exchangeClients {
		client.childPf.exchangeServer.tasks = tasks
		client.tasks, err = e.schedulePlanFragment(ctx, client.childPf, false)
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
	tidbTask := &mppTask{
		startTs: e.startTS,
		id:      -1,
	}
	rootPf := &planFragment{
		p:              e.originalPlan,
		exchangeServer: &ExchangeServer{exchangeType: tipb.ExchangeType_PassThrough, tasks: []*mppTask{tidbTask}},
	}

	e.getPlanFragments(e.originalPlan, rootPf)
	_, err := e.schedulePlanFragment(ctx, rootPf, true)
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
	return e.respIter.Close()
}
