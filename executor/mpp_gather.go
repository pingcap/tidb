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
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
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
	return true
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
		StartTs: t.startTs,
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
	tableScan         *plannercore.PhysicalTableScan // result physical table scan
	exchangeReceivers []*ExchangeReceiver            // data receivers

	// following fields are filled after scheduling.
	exchangeSender *ExchangeSender // data exporter
}

// ExchangeReceiver accepts connection and receives data passively.
type ExchangeReceiver struct {
	plannercore.PhysicalExchangerBase

	tasks   []*mppTask
	childPf *planFragment
	schema  *expression.Schema
}

// ToPB generates the pb structure.
func (e *ExchangeReceiver) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {

	encodedTask := make([][]byte, 0, len(e.tasks))

	for _, task := range e.tasks {
		encodedStr, err := task.ToPB().Marshal()
		if err != nil {
			return nil, errors.Trace(err)
		}
		encodedTask = append(encodedTask, encodedStr)
	}

	fieldTypes := make([]*tipb.FieldType, 0, len(e.schema.Columns))
	for _, column := range e.schema.Columns {
		fieldTypes = append(fieldTypes, expression.ToPBFieldType(column.RetType))
	}
	ecExec := &tipb.ExchangeReceiver{
		EncodedTaskMeta: encodedTask,
		FieldTypes:      fieldTypes,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:               tipb.ExecType_TypeExchangeReceiver,
		ExchangeReceiver: ecExec,
		ExecutorId:       &executorID,
	}, nil
}

// ExchangeSender dispatches data to upstream tasks. That means push mode processing,
type ExchangeSender struct {
	plannercore.PhysicalExchangerBase

	tasks        []*mppTask
	exchangeType tipb.ExchangeType
}

// ToPB generates the pb structure.
func (e *ExchangeSender) ToPB(ctx sessionctx.Context, storeType kv.StoreType) (*tipb.Executor, error) {
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

	ecExec := &tipb.ExchangeSender{
		Tp:              e.exchangeType,
		EncodedTaskMeta: encodedTask,
		Child:           child,
	}
	executorID := e.ExplainID().String()
	return &tipb.Executor{
		Tp:             tipb.ExecType_TypeExchangeSender,
		ExchangeSender: ecExec,
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

func (e *MPPGather) constructMPPTasksImpl(ctx context.Context, p *planFragment) ([]*mppTask, error) {
	if p.tableScan.Table.GetPartitionInfo() == nil {
		return e.constructSinglePhysicalTable(ctx, p.tableScan.Table.ID, p.tableScan.Ranges)
	}
	tmp, _ := e.is.TableByID(p.tableScan.Table.ID)
	tbl := tmp.(table.PartitionedTable)
	partitions, err := partitionPruning(e.ctx, tbl, p.tableScan.PartitionInfo.PruningConds, p.tableScan.PartitionInfo.PartitionNames, p.tableScan.PartitionInfo.Columns, p.tableScan.PartitionInfo.ColumnNames)
	if err != nil {
		return nil, errors.Trace(err)
	}
	allTasks := make([]*mppTask, 0)
	for _, part := range partitions {
		partTasks, err := e.constructSinglePhysicalTable(ctx, part.GetPhysicalID(), p.tableScan.Ranges)
		if err != nil {
			return nil, errors.Trace(err)
		}
		allTasks = append(allTasks, partTasks...)
	}
	return allTasks, nil
}

func (e *MPPGather) constructSinglePhysicalTable(ctx context.Context, tableID int64, ranges []*ranger.Range) ([]*mppTask, error) {
	kvRanges := distsql.TableRangesToKVRanges(tableID, ranges, nil)
	req := &kv.MPPBuildTasksRequest{KeyRanges: kvRanges}
	stores, err := e.ctx.GetMPPClient().ConstructMPPTasks(ctx, req)
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

func getPlanFragments(ctx sessionctx.Context, p plannercore.PhysicalPlan, pf *planFragment) {
	switch x := p.(type) {
	case *plannercore.PhysicalTableScan:
		x.IsGlobalRead = false
		pf.tableScan = x
	case *plannercore.PhysicalBroadCastJoin:
		// This is a pipeline breaker. So we replace broadcast side with a exchangerClient
		bcChild := x.Children()[x.InnerChildIdx]
		exchangeSender := &ExchangeSender{exchangeType: tipb.ExchangeType_Broadcast}
		exchangeSender.InitBasePlan(ctx, plancodec.TypeExchangeSender)
		npf := &planFragment{p: bcChild, exchangeSender: exchangeSender}
		exchangeSender.SetChildren(npf.p)

		exchangeReceivers := &ExchangeReceiver{
			childPf: npf,
			schema:  bcChild.Schema(),
		}
		exchangeReceivers.InitBasePlan(ctx, plancodec.TypeExchangeReceiver)
		x.Children()[x.InnerChildIdx] = exchangeReceivers
		pf.exchangeReceivers = append(pf.exchangeReceivers, exchangeReceivers)

		// For the inner side of join, we use a new plan fragment.
		getPlanFragments(ctx, bcChild, npf)
		getPlanFragments(ctx, x.Children()[1-x.InnerChildIdx], pf)
	default:
		if len(x.Children()) > 0 {
			getPlanFragments(ctx, x.Children()[0], pf)
		}
	}
}

func (e *MPPGather) appendMPPDispatchReq(pf *planFragment, tasks []*mppTask, isRoot bool) error {
	dagReq, _, err := constructDAGReq(e.ctx, []plannercore.PhysicalPlan{pf.exchangeSender}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.p.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !isRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
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

func (e *MPPGather) constructMPPTasks(ctx context.Context, pf *planFragment, isRoot bool) ([]*mppTask, error) {
	tasks, err := e.constructMPPTasksImpl(ctx, pf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, client := range pf.exchangeReceivers {
		client.childPf.exchangeSender.tasks = tasks
		client.tasks, err = e.constructMPPTasks(ctx, client.childPf, false)
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
		exchangeSender: &ExchangeSender{exchangeType: tipb.ExchangeType_PassThrough, tasks: []*mppTask{tidbTask}},
	}
	rootPf.exchangeSender.InitBasePlan(e.ctx, plancodec.TypeExchangeSender)
	rootPf.exchangeSender.SetChildren(rootPf.p)

	getPlanFragments(e.ctx, e.originalPlan, rootPf)
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
