package task

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/cost"
	"github.com/pingcap/tidb/pkg/planner/core/internal"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// ************************************* MPPTask Start ******************************************

// MppTask can not :
// 1. keep order
// 2. support double read
// 3. consider virtual columns.
// 4. TODO: partition prune after close
type MppTask struct {
	p base.PhysicalPlan

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

	// warnings passed through different task copy attached with more upper operator specific warnings. (not concurrent safe)
	warnings internal.SimpleWarnings
}

// Count implements Task interface.
func (t *MppTask) Count() float64 {
	return t.p.StatsInfo().RowCount
}

// Copy implements Task interface.
func (t *MppTask) Copy() base.Task {
	nt := *t
	// since *t will reuse the same warnings slice, we need to copy it out.
	// cause different task instance should have different warning slice.
	nt.warnings.Copy(&t.warnings)
	return &nt
}

// Plan implements Task interface.
func (t *MppTask) Plan() base.PhysicalPlan {
	return t.p
}

// Invalid implements Task interface.
func (t *MppTask) Invalid() bool {
	return t.p == nil
}

// ConvertToRootTask implements Task interface.
func (t *MppTask) ConvertToRootTask(ctx base.PlanContext) base.Task {
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

// AppendWarning appends a warning
func (t *MppTask) AppendWarning(err error) {
	t.warnings.AppendWarning(err)
}

// ConvertToRootTaskImpl implements Task interface.
func (t *MppTask) ConvertToRootTaskImpl(ctx base.PlanContext) (rt *RootTask) {
	defer func() {
		// mppTask should inherit the indexJoinInfo upward.
		// because mpp task bottom doesn't form the indexJoin related cop task.
		if t.warnings.WarningCount() > 0 {
			rt.warnings.CopyFrom(&t.warnings)
		}
	}()
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
	rt = &RootTask{}
	rt.SetPlan(p)

	if len(t.rootTaskConds) > 0 {
		// Some Filter cannot be pushed down to TiFlash, need to add Selection in rootTask,
		// so this Selection will be executed in TiDB.
		_, isTableScan := t.p.(*physicalop.PhysicalTableScan)
		_, isSelection := t.p.(*physicalop.PhysicalSelection)
		if isSelection {
			_, isTableScan = t.p.Children()[0].(*physicalop.PhysicalTableScan)
		}
		if !isTableScan {
			// Need to make sure oriTaskPlan is TableScan, because rootTaskConds is part of TableScan.FilterCondition.
			// It's only for TableScan. This is ensured by converting mppTask to rootTask just after TableScan is built,
			// so no other operators are added into this mppTask.
			logutil.BgLogger().Error("expect Selection or TableScan for mppTask.p", zap.String("mppTask.p", t.p.TP()))
			return base.InvalidTask.(*RootTask)
		}
		selectivity, _, err := cardinality.Selectivity(ctx, t.tblColHists, t.rootTaskConds, nil)
		if err != nil {
			logutil.BgLogger().Debug("calculate selectivity failed, use selection factor", zap.Error(err))
			selectivity = cost.SelectionFactor
		}
		sel := physicalop.PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, rt.GetPlan().StatsInfo().Scale(selectivity), rt.GetPlan().QueryBlockOffset())
		sel.FromDataSource = true
		sel.SetChildren(rt.GetPlan())
		rt.SetPlan(sel)
	}
	return rt
}

// ************************************* MPPTask End ******************************************
