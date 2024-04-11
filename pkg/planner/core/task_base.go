// Copyright 2024 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

var (
	_ Task = &RootTask{}
	_ Task = &MppTask{}
)

// Task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTaskMeta or a ParallelTask.
type Task interface {
	// Count returns current task's row count.
	Count() float64
	// Copy return a shallow copy of current task with the same pointer to p.
	Copy() Task
	// Plan returns current task's plan.
	Plan() PhysicalPlan
	// Invalid returns whether current task is invalid.
	Invalid() bool
	// ConvertToRootTask will convert current task as root type.
	ConvertToRootTask(ctx PlanContext) *RootTask
	// MemoryUsage returns the memory usage of current task.
	MemoryUsage() int64
}

// RootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type RootTask struct {
	p       PhysicalPlan
	isEmpty bool // isEmpty indicates if this task contains a dual table and returns empty data.
	// TODO: The flag 'isEmpty' is only checked by Projection and UnionAll. We should support more cases in the future.
}

// GetPlan returns the root task's plan.
func (t *RootTask) GetPlan() PhysicalPlan {
	return t.p
}

// SetPlan sets the root task' plan.
func (t *RootTask) SetPlan(p PhysicalPlan) {
	t.p = p
}

// IsEmpty indicates whether root task is empty.
func (t *RootTask) IsEmpty() bool {
	return t.isEmpty
}

// SetEmpty set the root task as empty.
func (t *RootTask) SetEmpty(x bool) {
	t.isEmpty = x
}

// Copy implements Task interface.
func (t *RootTask) Copy() Task {
	return &RootTask{
		p: t.p,
	}
}

// ConvertToRootTask implements Task interface.
func (t *RootTask) ConvertToRootTask(_ PlanContext) *RootTask {
	return t.Copy().(*RootTask)
}

// Invalid implements Task interface.
func (t *RootTask) Invalid() bool {
	return t.p == nil
}

// Count implements Task interface.
func (t *RootTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Plan implements Task interface.
func (t *RootTask) Plan() PhysicalPlan {
	return t.p
}

// MemoryUsage return the memory usage of rootTask
func (t *RootTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}
	sum = size.SizeOfInterface + size.SizeOfBool
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return sum
}

// MppTask can not :
// 1. keep order
// 2. support double read
// 3. consider virtual columns.
// 4. TODO: partition prune after close
type MppTask struct {
	p PhysicalPlan

	partTp   property.MPPPartitionType
	hashCols []*property.MPPPartitionColumn

	// rootTaskConds record filters of TableScan that cannot be pushed down to TiFlash.

	// For logical plan like: HashAgg -> Selection -> TableScan, if filters in Selection cannot be pushed to TiFlash.
	// Planner will generate physical plan like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> PhysicalTableScan(cop tiflash)
	// Because planner will make mppTask invalid directly then use copTask directly.

	// But in DisaggregatedTiFlash mode, cop and batchCop protocol is disabled, so we have to consider this situation for mppTask.
	// When generating PhysicalTableScan, if prop.TaskTp is RootTaskType, mppTask will be converted to rootTask,
	// and filters in rootTaskConds will be added in a Selection which will be executed in TiDB.
	// So physical plan be like: PhysicalHashAgg -> PhysicalSelection -> TableReader -> ExchangeSender -> PhysicalTableScan(mpp tiflash)
	rootTaskConds []expression.Expression
	tblColHists   *statistics.HistColl
}

// Count implements Task interface.
func (t *MppTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *MppTask) Copy() Task {
	nt := *t
	return &nt
}

// Plan implements Task interface.
func (t *MppTask) Plan() PhysicalPlan {
	return t.p
}

// Invalid implements Task interface.
func (t *MppTask) Invalid() bool {
	return t.p == nil
}

// ConvertToRootTask implements Task interface.
func (t *MppTask) ConvertToRootTask(ctx PlanContext) *RootTask {
	return t.Copy().(*MppTask).ConvertToRootTaskImpl(ctx)
}

// MemoryUsage return the memory usage of mppTask
func (t *MppTask) MemoryUsage() (sum int64) {
	if t == nil {
		return
	}

	sum = size.SizeOfInterface + size.SizeOfInt + size.SizeOfSlice + int64(cap(t.hashCols))*size.SizeOfPointer
	if t.p != nil {
		sum += t.p.MemoryUsage()
	}
	return
}

func (t *MppTask) ConvertToRootTaskImpl(ctx PlanContext) *RootTask {
	// In disaggregated-tiflash mode, need to consider generated column.
	tryExpandVirtualColumn(t.p)
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}.Init(ctx, t.p.StatsInfo())
	sender.SetChildren(t.p)

	p := PhysicalTableReader{
		tablePlan: sender,
		StoreType: kv.TiFlash,
	}.Init(ctx, t.p.QueryBlockOffset())
	p.SetStats(t.p.StatsInfo())
	collectPartitionInfosFromMPPPlan(p, t.p)
	rt := &RootTask{}
	rt.SetPlan(p)

	if len(t.rootTaskConds) > 0 {
		// Some Filter cannot be pushed down to TiFlash, need to add Selection in rootTask,
		// so this Selection will be executed in TiDB.
		_, isTableScan := t.p.(*PhysicalTableScan)
		_, isSelection := t.p.(*PhysicalSelection)
		if isSelection {
			_, isTableScan = t.p.Children()[0].(*PhysicalTableScan)
		}
		if !isTableScan {
			// Need to make sure oriTaskPlan is TableScan, because rootTaskConds is part of TableScan.FilterCondition.
			// It's only for TableScan. This is ensured by converting mppTask to rootTask just after TableScan is built,
			// so no other operators are added into this mppTask.
			logutil.BgLogger().Error("expect Selection or TableScan for mppTask.p", zap.String("mppTask.p", t.p.TP()))
			return invalidTask
		}
		selectivity, _, err := cardinality.Selectivity(ctx, t.tblColHists, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = SelectionFactor
		}
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, rt.GetPlan().StatsInfo().Scale(selectivity), rt.GetPlan().QueryBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(rt.GetPlan())
		rt.SetPlan(sel)
	}
	return rt
}
