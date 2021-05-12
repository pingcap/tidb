// Copyright 2017 PingCAP, Inc.
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

package core

import (
	"math"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ task = &copTask{}
	_ task = &rootTask{}
	_ task = &mppTask{}
)

// task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTaskMeta or a ParallelTask.
type task interface {
	count() float64
	addCost(cost float64)
	cost() float64
	copy() task
	plan() PhysicalPlan
	invalid() bool
	convertToRootTask(ctx sessionctx.Context) *rootTask
}

// copTask is a task that runs in a distributed kv store.
// TODO: In future, we should split copTask to indexTask and tableTask.
type copTask struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
	// keepOrder indicates if the plan scans data by order.
	keepOrder bool
	// needExtraProj means an extra prune is needed because
	// in double read / index merge cases, they may output one more column for handle(row id).
	needExtraProj bool
	// originSchema is the target schema to be projected to when needExtraProj is true.
	originSchema *expression.Schema

	extraHandleCol   *expression.Column
	commonHandleCols []*expression.Column
	// tblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tblColHists *statistics.HistColl
	// tblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tblCols           []*expression.Column
	idxMergePartPlans []PhysicalPlan
	// rootTaskConds stores select conditions containing virtual columns.
	// These conditions can't push to TiKV, so we have to add a selection for rootTask
	rootTaskConds []expression.Expression

	// For table partition.
	partitionInfo PartitionInfo
}

func (t *copTask) invalid() bool {
	return t.tablePlan == nil && t.indexPlan == nil
}

func (t *rootTask) invalid() bool {
	return t.p == nil
}

func (t *copTask) count() float64 {
	if t.indexPlanFinished {
		return t.tablePlan.statsInfo().RowCount
	}
	return t.indexPlan.statsInfo().RowCount
}

func (t *copTask) addCost(cst float64) {
	t.cst += cst
}

func (t *copTask) cost() float64 {
	return t.cst
}

func (t *copTask) copy() task {
	nt := *t
	return &nt
}

func (t *copTask) plan() PhysicalPlan {
	if t.indexPlanFinished {
		return t.tablePlan
	}
	return t.indexPlan
}

func attachPlan2Task(p PhysicalPlan, t task) task {
	switch v := t.(type) {
	case *copTask:
		if v.indexPlanFinished {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		}
	case *rootTask:
		p.SetChildren(v.p)
		v.p = p
	case *mppTask:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTask) finishIndexPlan() {
	if t.indexPlanFinished {
		return
	}
	cnt := t.count()
	t.indexPlanFinished = true
	sessVars := t.indexPlan.SCtx().GetSessionVars()
	// Network cost of transferring rows of index scan to TiDB.
	t.cst += cnt * sessVars.NetworkFactor * t.tblColHists.GetAvgRowSize(t.indexPlan.SCtx(), t.indexPlan.Schema().Columns, true, false)

	if t.tablePlan == nil {
		return
	}
	// Calculate the IO cost of table scan here because we cannot know its stats until we finish index plan.
	t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
	var p PhysicalPlan
	for p = t.indexPlan; len(p.Children()) > 0; p = p.Children()[0] {
	}
	rowSize := t.tblColHists.GetIndexAvgRowSize(t.indexPlan.SCtx(), t.tblCols, p.(*PhysicalIndexScan).Index.Unique)
	t.cst += cnt * rowSize * sessVars.ScanFactor
}

func (t *copTask) getStoreType() kv.StoreType {
	if t.tablePlan == nil {
		return kv.TiKV
	}
	tp := t.tablePlan
	for len(tp.Children()) > 0 {
		if len(tp.Children()) > 1 {
			return kv.TiFlash
		}
		tp = tp.Children()[0]
	}
	if ts, ok := tp.(*PhysicalTableScan); ok {
		return ts.StoreType
	}
	return kv.TiKV
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	t := tasks[0].convertToRootTask(p.ctx)
	p.cost = t.cost()
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalUnionScan) attach2Task(tasks ...task) task {
	p.cost = tasks[0].cost()
	if pj, ok := tasks[0].plan().(*PhysicalProjection); ok {
		// Convert unionScan->projection to projection->unionScan, because unionScan can't handle projection as its children.
		p.SetChildren(pj.children...)
		p.stats = tasks[0].plan().statsInfo()
		rt, _ := tasks[0].(*rootTask)
		rt.p = pj.children[0]
		pj.SetChildren(p)
		return pj.attach2Task(p.basePhysicalPlan.attach2Task(tasks...))
	}
	p.stats = tasks[0].plan().statsInfo()
	return p.basePhysicalPlan.attach2Task(tasks...)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	t := &rootTask{
		p:   p,
		cst: p.GetCost(lTask.count(), rTask.count(), lTask.cost(), rTask.cost()),
	}
	p.cost = t.cost()
	return t
}

// GetCost computes the cost of apply operator.
func (p *PhysicalApply) GetCost(lCount, rCount, lCost, rCost float64) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * sessVars.CPUFactor
		lCount *= SelectionFactor
	}
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.CPUFactor
		rCount *= SelectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions) > 0 {
		if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
			p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
			cpuCost += lCount * rCount * sessVars.CPUFactor * 0.5
		} else {
			cpuCost += lCount * rCount * sessVars.CPUFactor
		}
	}
	// Apply uses a NestedLoop method for execution.
	// For every row from the left(outer) side, it executes
	// the whole right(inner) plan tree. So the cost of apply
	// should be : apply cost + left cost + left count * right cost
	return cpuCost + lCost + lCount*rCost
}

func (p *PhysicalIndexMergeJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
	p.cost = t.cost()
	return t
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexMergeJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (sortFactor + 1.0) * batchSize * cpuFactor
	// If `p.NeedOuterSort` is true, the sortFactor is batchSize * Log2(batchSize).
	// Otherwise, it's 0.
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	sortFactor := 0.0
	if p.NeedOuterSort {
		sortFactor = math.Log2(float64(batchSize))
	}
	if batchSize > 2 {
		innerCPUCost += outerCnt * (sortFactor + 1.0) * sessVars.CPUFactor
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * cpuFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	innerConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of merge join in inner worker.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	avgProbeCnt := numPairs / outerCnt
	var probeCost float64
	// Inner workers do merge join in parallel, but they can only save ONE outer batch
	// results. So as the number of outer batch exceeds inner concurrency, it would fall back to
	// linear execution. In a word, the merge join only run in parallel for the first
	// `innerConcurrency` number of inner tasks.
	if outerCnt/batchSize >= innerConcurrency {
		probeCost = (numPairs - batchSize*avgProbeCnt*(innerConcurrency-1)) * sessVars.CPUFactor
	} else {
		probeCost = batchSize * avgProbeCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.ConcurrencyFactor

	// Index merge join save the join results in inner worker.
	// So the memory cost consider the results size for each batch.
	memoryCost := innerConcurrency * (batchSize * avgProbeCnt) * sessVars.MemoryFactor

	innerPlanCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerPlanCost + cpuCost + memoryCost
}

func (p *PhysicalIndexHashJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
	p.cost = t.cost()
	return t
}

// GetCost computes the cost of index merge join operator and its children.
func (p *PhysicalIndexHashJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(sessVars.IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.CPUFactor
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	concurrency := float64(sessVars.IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / concurrency
	// CPU cost of building hash table for outer results concurrently.
	// (outerCnt / batchSize) * (batchSize * CPUFactor)
	outerCPUCost := outerCnt * sessVars.CPUFactor
	cpuCost += outerCPUCost / concurrency
	// Cost of probing hash table concurrently.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	// Inner workers do hash join in parallel, but they can only save ONE outer
	// batch results. So as the number of outer batch exceeds inner concurrency,
	// it would fall back to linear execution. In a word, the hash join only runs
	// in parallel for the first `innerConcurrency` number of inner tasks.
	var probeCost float64
	if outerCnt/batchSize >= concurrency {
		probeCost = (numPairs - batchSize*innerCnt*(concurrency-1)) * sessVars.CPUFactor
	} else {
		probeCost = batchSize * innerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// Cost of additional concurrent goroutines.
	cpuCost += (concurrency + 1.0) * sessVars.ConcurrencyFactor
	// Memory cost of hash tables for outer rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := concurrency * (batchSize * distinctFactor) * innerCnt * sessVars.MemoryFactor
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerPlanCost + cpuCost + memoryCost
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := tasks[1-p.InnerChildIdx].convertToRootTask(p.ctx)
	if p.InnerChildIdx == 1 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	t := &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
	p.cost = t.cost()
	return t
}

// GetCost computes the cost of index join operator and its children.
func (p *PhysicalIndexJoin) GetCost(outerTask, innerTask task) float64 {
	var cpuCost float64
	outerCnt, innerCnt := outerTask.count(), innerTask.count()
	sessVars := p.ctx.GetSessionVars()
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += sessVars.CPUFactor * outerCnt
		outerCnt *= SelectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * CPUFactor
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * sessVars.CPUFactor
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * distinctFactor * innerCnt * sessVars.CPUFactor
	innerConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency())
	cpuCost += innerCPUCost / innerConcurrency
	// Cost of probing hash table in main thread.
	numPairs := outerCnt * innerCnt
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*sessVars.ConcurrencyFactor
	// Memory cost of hash tables for inner rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * distinctFactor) * innerCnt * sessVars.MemoryFactor
	// Cost of inner child plan, i.e, mainly I/O and network cost.
	innerPlanCost := outerCnt * innerTask.cost()
	return outerTask.cost() + innerPlanCost + cpuCost + memoryCost
}

func getAvgRowSize(stats *property.StatsInfo, schema *expression.Schema) (size float64) {
	if stats.HistColl != nil {
		size = stats.HistColl.GetAvgRowSizeListInDisk(schema.Columns)
	} else {
		// Estimate using just the type info.
		cols := schema.Columns
		for _, col := range cols {
			size += float64(chunk.EstimateTypeWidth(col.GetType()))
		}
	}
	return
}

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64) float64 {
	buildCnt, probeCnt := lCnt, rCnt
	build := p.children[0]
	// Taking the right as the inner for right join or using the outer to build a hash table.
	if (p.InnerChildIdx == 1 && !p.UseOuterToBuild) || (p.InnerChildIdx == 0 && p.UseOuterToBuild) {
		buildCnt, probeCnt = rCnt, lCnt
		build = p.children[1]
	}
	sessVars := p.ctx.GetSessionVars()
	oomUseTmpStorage := config.GetGlobalConfig().OOMUseTmpStorage
	memQuota := sessVars.StmtCtx.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(build.statsInfo(), build.Schema())
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*buildCnt > float64(memQuota) && p.storeTp != kv.TiFlash
	// Cost of building hash table.
	cpuCost := buildCnt * sessVars.CPUFactor
	memoryCost := buildCnt * sessVars.MemoryFactor
	diskCost := buildCnt * sessVars.DiskFactor * rowSize
	// Number of matched row pairs regarding the equal join conditions.
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	// For semi-join class, if `OtherConditions` is empty, we already know
	// the join results after querying hash table, otherwise, we have to
	// evaluate those resulted row pairs after querying hash table; if we
	// find one pair satisfying the `OtherConditions`, we then know the
	// join result for this given outer row, otherwise we have to iterate
	// to the end of those pairs; since we have no idea about when we can
	// terminate the iteration, we assume that we need to iterate half of
	// those pairs in average.
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	// Cost of querying hash table is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining row pairs.
	probeCost := numPairs * sessVars.CPUFactor
	probeDiskCost := numPairs * sessVars.DiskFactor * rowSize
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by SelectionFactor.
		probeCost *= SelectionFactor
		probeDiskCost *= SelectionFactor
		probeCost += probeCnt * sessVars.CPUFactor
	}
	diskCost += probeDiskCost
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*sessVars.ConcurrencyFactor
	// Cost of traveling the hash table to resolve missing matched cases when building the hash table from the outer table
	if p.UseOuterToBuild {
		if spill {
			// It runs in sequence when build data is on disk. See handleUnmatchedRowsFromHashTableInDisk
			cpuCost += buildCnt * sessVars.CPUFactor
		} else {
			cpuCost += buildCnt * sessVars.CPUFactor / float64(p.Concurrency)
		}
		diskCost += buildCnt * sessVars.DiskFactor * rowSize
	}

	if spill {
		memoryCost *= float64(memQuota) / (rowSize * buildCnt)
	} else {
		diskCost = 0
	}
	return cpuCost + memoryCost + diskCost
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	if p.storeTp == kv.TiFlash {
		return p.attach2TaskForTiFlash(tasks...)
	}
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
	p.cost = task.cost()
	return task
}

// TiDB only require that the types fall into the same catalog but TiFlash require the type to be exactly the same, so
// need to check if the conversion is a must
func needConvert(tp *types.FieldType, rtp *types.FieldType) bool {
	if tp.Tp != rtp.Tp {
		return true
	}
	if tp.Tp != mysql.TypeNewDecimal {
		return false
	}
	if tp.Decimal != rtp.Decimal {
		return true
	}
	// for Decimal type, TiFlash have 4 different impl based on the required precision
	if tp.Flen >= 0 && tp.Flen <= 9 && rtp.Flen >= 0 && rtp.Flen <= 9 {
		return false
	}
	if tp.Flen > 9 && tp.Flen <= 18 && rtp.Flen > 9 && rtp.Flen <= 18 {
		return false
	}
	if tp.Flen > 18 && tp.Flen <= 38 && rtp.Flen > 18 && rtp.Flen <= 38 {
		return false
	}
	if tp.Flen > 38 && tp.Flen <= 65 && rtp.Flen > 38 && rtp.Flen <= 65 {
		return false
	}
	return true
}

func negotiateCommonType(lType, rType *types.FieldType) (*types.FieldType, bool, bool) {
	commonType := types.AggFieldType([]*types.FieldType{lType, rType})
	if commonType.Tp == mysql.TypeNewDecimal {
		lExtend := 0
		rExtend := 0
		cDec := rType.Decimal
		if lType.Decimal < rType.Decimal {
			lExtend = rType.Decimal - lType.Decimal
		} else if lType.Decimal > rType.Decimal {
			rExtend = lType.Decimal - rType.Decimal
			cDec = lType.Decimal
		}
		lLen, rLen := lType.Flen+lExtend, rType.Flen+rExtend
		cLen := mathutil.Max(lLen, rLen)
		cLen = mathutil.Min(65, cLen)
		commonType.Decimal = cDec
		commonType.Flen = cLen
	} else if needConvert(lType, commonType) || needConvert(rType, commonType) {
		if mysql.IsIntegerType(commonType.Tp) {
			// If the target type is int, both TiFlash and Mysql only support cast to Int64
			// so we need to promote the type to Int64
			commonType.Tp = mysql.TypeLonglong
			commonType.Flen = mysql.MaxIntWidth
		}
	}
	return commonType, needConvert(lType, commonType), needConvert(rType, commonType)
}

func getProj(ctx sessionctx.Context, p PhysicalPlan) *PhysicalProjection {
	proj := PhysicalProjection{
		Exprs: make([]expression.Expression, 0, len(p.Schema().Columns)),
	}.Init(ctx, p.statsInfo(), p.SelectBlockOffset())
	for _, col := range p.Schema().Columns {
		proj.Exprs = append(proj.Exprs, col)
	}
	proj.SetSchema(p.Schema().Clone())
	proj.SetChildren(p)
	return proj
}

func appendExpr(p *PhysicalProjection, expr expression.Expression) *expression.Column {
	p.Exprs = append(p.Exprs, expr)

	col := &expression.Column{
		UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
	}
	col.SetCoercibility(expr.Coercibility())
	p.schema.Append(col)
	return col
}

// TiFlash join require that partition key has exactly the same type, while TiDB only guarantee the partition key is the same catalog,
// so if the partition key type is not exactly the same, we need add a projection below the join or exchanger if exists.
func (p *PhysicalHashJoin) convertPartitionKeysIfNeed(lTask, rTask *mppTask) (*mppTask, *mppTask) {
	lp := lTask.p
	if _, ok := lp.(*PhysicalExchangeReceiver); ok {
		lp = lp.Children()[0].Children()[0]
	}
	rp := rTask.p
	if _, ok := rp.(*PhysicalExchangeReceiver); ok {
		rp = rp.Children()[0].Children()[0]
	}
	// to mark if any partition key needs to convert
	lMask := make([]bool, len(lTask.hashCols))
	rMask := make([]bool, len(rTask.hashCols))
	cTypes := make([]*types.FieldType, len(lTask.hashCols))
	lChanged := false
	rChanged := false
	for i := range lTask.hashCols {
		lKey := lTask.hashCols[i]
		rKey := rTask.hashCols[i]
		cType, lConvert, rConvert := negotiateCommonType(lKey.RetType, rKey.RetType)
		if lConvert {
			lMask[i] = true
			cTypes[i] = cType
			lChanged = true
		}
		if rConvert {
			rMask[i] = true
			cTypes[i] = cType
			rChanged = true
		}
	}
	if !lChanged && !rChanged {
		return lTask, rTask
	}
	var lProj, rProj *PhysicalProjection
	if lChanged {
		lProj = getProj(p.ctx, lp)
		lp = lProj
	}
	if rChanged {
		rProj = getProj(p.ctx, rp)
		rp = rProj
	}

	lPartKeys := make([]*expression.Column, 0, len(rTask.hashCols))
	rPartKeys := make([]*expression.Column, 0, len(lTask.hashCols))
	for i := range lTask.hashCols {
		lKey := lTask.hashCols[i]
		rKey := rTask.hashCols[i]
		if lMask[i] {
			cType := cTypes[i].Clone()
			cType.Flag = lKey.RetType.Flag
			lCast := expression.BuildCastFunction(p.ctx, lKey, cType)
			lKey = appendExpr(lProj, lCast)
		}
		if rMask[i] {
			cType := cTypes[i].Clone()
			cType.Flag = rKey.RetType.Flag
			rCast := expression.BuildCastFunction(p.ctx, rKey, cType)
			rKey = appendExpr(rProj, rCast)
		}
		lPartKeys = append(lPartKeys, lKey)
		rPartKeys = append(rPartKeys, rKey)
	}
	// if left or right child changes, we need to add enforcer.
	if lChanged {
		nlTask := lTask.copy().(*mppTask)
		nlTask.p = lProj
		nlTask = nlTask.enforceExchangerImpl(&property.PhysicalProperty{
			TaskTp:        property.MppTaskType,
			PartitionTp:   property.HashType,
			PartitionCols: lPartKeys,
		})
		nlTask.cst = lTask.cst
		lProj.cost = nlTask.cst
		lTask = nlTask
	}
	if rChanged {
		nrTask := rTask.copy().(*mppTask)
		nrTask.p = rProj
		nrTask = nrTask.enforceExchangerImpl(&property.PhysicalProperty{
			TaskTp:        property.MppTaskType,
			PartitionTp:   property.HashType,
			PartitionCols: rPartKeys,
		})
		nrTask.cst = rTask.cst
		rProj.cost = nrTask.cst
		rTask = nrTask
	}
	return lTask, rTask
}

func (p *PhysicalHashJoin) attach2TaskForMpp(tasks ...task) task {
	lTask, lok := tasks[0].(*mppTask)
	rTask, rok := tasks[1].(*mppTask)
	if !lok || !rok {
		return invalidTask
	}
	if p.mppShuffleJoin {
		// protection check is case of some bugs
		if len(lTask.hashCols) != len(rTask.hashCols) || len(lTask.hashCols) == 0 {
			return invalidTask
		}
		lTask, rTask = p.convertPartitionKeysIfNeed(lTask, rTask)
	}
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	lCost := lTask.cost()
	rCost := rTask.cost()

	outerTask := tasks[1-p.InnerChildIdx].(*mppTask)
	task := &mppTask{
		cst:      lCost + rCost + p.GetCost(lTask.count(), rTask.count()),
		p:        p,
		partTp:   outerTask.partTp,
		hashCols: outerTask.hashCols,
	}
	p.cost = task.cst
	return task
}

func (p *PhysicalHashJoin) attach2TaskForTiFlash(tasks ...task) task {
	lTask, lok := tasks[0].(*copTask)
	rTask, rok := tasks[1].(*copTask)
	if !lok || !rok {
		return p.attach2TaskForMpp(tasks...)
	}
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = BuildPhysicalJoinSchema(p.JoinType, p)
	if !lTask.indexPlanFinished {
		lTask.finishIndexPlan()
	}
	if !rTask.indexPlanFinished {
		rTask.finishIndexPlan()
	}

	lCost := lTask.cost()
	rCost := rTask.cost()

	task := &copTask{
		tblColHists:       rTask.tblColHists,
		indexPlanFinished: true,
		tablePlan:         p,
		cst:               lCost + rCost + p.GetCost(lTask.count(), rTask.count()),
	}
	p.cost = task.cst
	return task
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64) float64 {
	outerCnt := lCnt
	innerKeys := p.RightJoinKeys
	innerSchema := p.children[1].Schema()
	innerStats := p.children[1].statsInfo()
	if p.JoinType == RightOuterJoin {
		outerCnt = rCnt
		innerKeys = p.LeftJoinKeys
		innerSchema = p.children[0].Schema()
		innerStats = p.children[0].statsInfo()
	}
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftJoinKeys,
		rightJoinKeys: p.RightJoinKeys,
		leftSchema:    p.children[0].Schema(),
		rightSchema:   p.children[1].Schema(),
	}
	numPairs := helper.estimate()
	if p.JoinType == SemiJoin || p.JoinType == AntiSemiJoin ||
		p.JoinType == LeftOuterSemiJoin || p.JoinType == AntiLeftOuterSemiJoin {
		if len(p.OtherConditions) > 0 {
			numPairs *= 0.5
		} else {
			numPairs = 0
		}
	}
	sessVars := p.ctx.GetSessionVars()
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= SelectionFactor
		cpuCost += outerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute average memory cost using estimated group size.
	NDV := getCardinality(innerKeys, innerSchema, innerStats)
	memoryCost := (innerStats.RowCount / NDV) * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := tasks[0].convertToRootTask(p.ctx)
	rTask := tasks[1].convertToRootTask(p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	t := &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
	p.cost = t.cost()
	return t
}

func buildIndexLookUpTask(ctx sessionctx.Context, t *copTask) *rootTask {
	newTask := &rootTask{cst: t.cst}
	sessVars := ctx.GetSessionVars()
	p := PhysicalIndexLookUpReader{
		tablePlan:        t.tablePlan,
		indexPlan:        t.indexPlan,
		ExtraHandleCol:   t.extraHandleCol,
		CommonHandleCols: t.commonHandleCols,
	}.Init(ctx, t.tablePlan.SelectBlockOffset())
	p.PartitionInfo = t.partitionInfo
	setTableScanToTableRowIDScan(p.tablePlan)
	p.stats = t.tablePlan.statsInfo()
	// Add cost of building table reader executors. Handles are extracted in batch style,
	// each handle is a range, the CPU cost of building copTasks should be:
	// (indexRows / batchSize) * batchSize * CPUFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	indexRows := t.indexPlan.statsInfo().RowCount
	newTask.cst += indexRows * sessVars.CPUFactor
	// Add cost of worker goroutines in index lookup.
	numTblWorkers := float64(sessVars.IndexLookupConcurrency())
	newTask.cst += (numTblWorkers + 1) * sessVars.ConcurrencyFactor
	// When building table reader executor for each batch, we would sort the handles. CPU
	// cost of sort is:
	// CPUFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
	indexLookupSize := float64(sessVars.IndexLookupSize)
	batchSize := math.Min(indexLookupSize, indexRows)
	if batchSize > 2 {
		sortCPUCost := (indexRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		newTask.cst += sortCPUCost
	}
	// Also, we need to sort the retrieved rows if index lookup reader is expected to return
	// ordered results. Note that row count of these two sorts can be different, if there are
	// operators above table scan.
	tableRows := t.tablePlan.statsInfo().RowCount
	selectivity := tableRows / indexRows
	batchSize = math.Min(indexLookupSize*selectivity, tableRows)
	if t.keepOrder && batchSize > 2 {
		sortCPUCost := (tableRows * math.Log2(batchSize) * sessVars.CPUFactor) / numTblWorkers
		newTask.cst += sortCPUCost
	}
	p.cost = newTask.cst
	if t.needExtraProj {
		schema := t.originSchema
		proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, t.tablePlan.SelectBlockOffset(), nil)
		proj.SetSchema(schema)
		proj.SetChildren(p)
		proj.cost = newTask.cst
		newTask.p = proj
	} else {
		newTask.p = p
	}
	return newTask
}

func (t *rootTask) convertToRootTask(_ sessionctx.Context) *rootTask {
	return t.copy().(*rootTask)
}

func (t *copTask) convertToRootTask(ctx sessionctx.Context) *rootTask {
	// copy one to avoid changing itself.
	return t.copy().(*copTask).convertToRootTaskImpl(ctx)
}

func (t *copTask) convertToRootTaskImpl(ctx sessionctx.Context) *rootTask {
	sessVars := ctx.GetSessionVars()
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(t.plan().SCtx().GetSessionVars().DistSQLScanConcurrency())
	t.finishIndexPlan()
	needExtraProj := false
	var prevSchema *expression.Schema
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		t.cst += t.count() * sessVars.NetworkFactor * t.tblColHists.GetAvgRowSize(ctx, t.tablePlan.Schema().Columns, false, false)

		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		prevColumnLen := len(ts.Columns)
		prevSchema = ts.schema.Clone()
		ts.Columns = ExpandVirtualColumn(ts.Columns, ts.schema, ts.Table.Columns)
		if len(ts.Columns) > prevColumnLen {
			// Add an projection to make sure not to output extract columns.
			needExtraProj = true
		}
	}
	t.cst /= copIterWorkers
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.idxMergePartPlans != nil {
		p := PhysicalIndexMergeReader{
			partialPlans: t.idxMergePartPlans,
			tablePlan:    t.tablePlan,
		}.Init(ctx, t.idxMergePartPlans[0].SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		setTableScanToTableRowIDScan(p.tablePlan)
		newTask.p = p
		p.cost = newTask.cost()
		if t.needExtraProj {
			schema := t.originSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, t.idxMergePartPlans[0].SelectBlockOffset(), nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			proj.SetCost(newTask.cost())
			newTask.p = proj
		}
		return newTask
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		newTask = buildIndexLookUpTask(ctx, t)
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx, t.indexPlan.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.indexPlan.statsInfo()
		p.cost = newTask.cost()
		newTask.p = p
	} else {
		tp := t.tablePlan
		for len(tp.Children()) > 0 {
			if len(tp.Children()) == 1 {
				tp = tp.Children()[0]
			} else {
				join := tp.(*PhysicalHashJoin)
				tp = join.children[1-join.InnerChildIdx]
			}
		}
		ts := tp.(*PhysicalTableScan)
		p := PhysicalTableReader{
			tablePlan:      t.tablePlan,
			StoreType:      ts.StoreType,
			IsCommonHandle: ts.Table.IsCommonHandle,
		}.Init(ctx, t.tablePlan.SelectBlockOffset())
		p.PartitionInfo = t.partitionInfo
		p.stats = t.tablePlan.statsInfo()
		p.cost = t.cost()

		// If agg was pushed down in attach2Task(), the partial agg was placed on the top of tablePlan, the final agg was
		// placed above the PhysicalTableReader, and the schema should have been set correctly for them, the schema of
		// partial agg contains the columns needed by the final agg.
		// If we add the projection here, the projection will be between the final agg and the partial agg, then the
		// schema will be broken, the final agg will fail to find needed columns in ResolveIndices().
		// Besides, the agg would only be pushed down if it doesn't contain virtual columns, so virtual column should not be affected.
		aggPushedDown := false
		switch p.tablePlan.(type) {
		case *PhysicalHashAgg, *PhysicalStreamAgg:
			aggPushedDown = true
		}

		if needExtraProj && !aggPushedDown {
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(prevSchema.Columns)}.Init(ts.ctx, ts.stats, ts.SelectBlockOffset(), nil)
			proj.SetSchema(prevSchema)
			proj.SetChildren(p)
			proj.cost = t.cost()
			newTask.p = proj
		} else {
			newTask.p = p
		}
	}

	if len(t.rootTaskConds) > 0 {
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.p.statsInfo(), newTask.p.SelectBlockOffset())
		sel.SetChildren(newTask.p)
		newTask.p = sel
		sel.cost = newTask.cost()
	}

	return newTask
}

// setTableScanToTableRowIDScan is to update the isChildOfIndexLookUp attribute of PhysicalTableScan child
func setTableScanToTableRowIDScan(p PhysicalPlan) {
	if ts, ok := p.(*PhysicalTableScan); ok {
		ts.SetIsChildOfIndexLookUp(true)
	} else {
		for _, child := range p.Children() {
			setTableScanToTableRowIDScan(child)
		}
	}
}

// rootTask is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTask struct {
	p   PhysicalPlan
	cst float64
}

func (t *rootTask) copy() task {
	return &rootTask{
		p:   t.p,
		cst: t.cst,
	}
}

func (t *rootTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *rootTask) addCost(cst float64) {
	t.cst += cst
}

func (t *rootTask) cost() float64 {
	return t.cst
}

func (t *rootTask) plan() PhysicalPlan {
	return t.p
}

func (p *PhysicalLimit) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	sunk := false
	if cop, ok := t.(*copTask); ok {
		// For double read which requires order being kept, the limit cannot be pushed down to the table side,
		// because handles would be reordered before being sent to table scan.
		if (!cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil) && len(cop.rootTaskConds) == 0 {
			// When limit is pushed down, we should remove its offset.
			newCount := p.Offset + p.Count
			childProfile := cop.plan().statsInfo()
			// Strictly speaking, for the row count of stats, we should multiply newCount with "regionNum",
			// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
			stats := deriveLimitStats(childProfile, float64(newCount))
			pushedDownLimit := PhysicalLimit{Count: newCount}.Init(p.ctx, stats, p.blockOffset)
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
			// Don't use clone() so that Limit and its children share the same schema. Otherwise the virtual generated column may not be resolved right.
			pushedDownLimit.SetSchema(pushedDownLimit.children[0].Schema())
			pushedDownLimit.cost = cop.cost()
		}
		t = cop.convertToRootTask(p.ctx)
		sunk = p.sinkIntoIndexLookUp(t)
	}
	p.cost = t.cost()
	if sunk {
		return t
	}
	return attachPlan2Task(p, t)
}

func (p *PhysicalLimit) sinkIntoIndexLookUp(t task) bool {
	root := t.(*rootTask)
	reader, isDoubleRead := root.p.(*PhysicalIndexLookUpReader)
	proj, isProj := root.p.(*PhysicalProjection)
	if !isDoubleRead && !isProj {
		return false
	}
	if isProj {
		reader, isDoubleRead = proj.Children()[0].(*PhysicalIndexLookUpReader)
		if !isDoubleRead {
			return false
		}
	}
	// We can sink Limit into IndexLookUpReader only if tablePlan contains no Selection.
	ts, isTableScan := reader.tablePlan.(*PhysicalTableScan)
	if !isTableScan {
		return false
	}
	reader.PushedLimit = &PushedDownLimit{
		Offset: p.Offset,
		Count:  p.Count,
	}
	ts.stats = p.stats
	reader.stats = p.stats
	if isProj {
		proj.stats = p.stats
	}
	return true
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := float64(p.Offset + p.Count)
	if heapSize < 2.0 {
		heapSize = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	// Ignore the cost of `doCompaction` in current implementation of `TopNExec`, since it is the
	// special side-effect of our Chunk format in TiDB layer, which may not exist in coprocessor's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// row.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * sessVars.CPUFactor
	} else {
		cpuCost = count * math.Log2(heapSize) * sessVars.CopCPUFactor
	}
	memoryCost := heapSize * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

// canPushDown checks if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *PhysicalTopN) canPushDown(storeTp kv.StoreType) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	return expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient(), storeTp)
}

func (p *PhysicalTopN) allColsFromSchema(schema *expression.Schema) bool {
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	return len(schema.ColumnsIndices(cols)) > 0
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64, schema *expression.Schema) float64 {
	if count < 2.0 {
		count = 2.0
	}
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * math.Log2(count) * sessVars.CPUFactor
	memoryCost := count * sessVars.MemoryFactor

	oomUseTmpStorage := config.GetGlobalConfig().OOMUseTmpStorage
	memQuota := sessVars.StmtCtx.MemTracker.GetBytesLimit() // sessVars.MemQuotaQuery && hint
	rowSize := getAvgRowSize(p.statsInfo(), schema)
	spill := oomUseTmpStorage && memQuota > 0 && rowSize*count > float64(memQuota)
	diskCost := count * sessVars.DiskFactor * rowSize
	if !spill {
		diskCost = 0
	} else {
		memoryCost *= float64(memQuota) / (rowSize * count)
	}
	return cpuCost + memoryCost + diskCost
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count(), p.Schema()))
	p.cost = t.cost()
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	if p.OnlyColumn {
		return tasks[0]
	}
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	return t
}

func (p *PhysicalTopN) getPushedDownTopN(childPlan PhysicalPlan) *PhysicalTopN {
	newByItems := make([]*util.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	newCount := p.Offset + p.Count
	childProfile := childPlan.statsInfo()
	// Strictly speaking, for the row count of pushed down TopN, we should multiply newCount with "regionNum",
	// but "regionNum" is unknown since the copTask can be a double read, so we ignore it now.
	stats := deriveLimitStats(childProfile, float64(newCount))
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   newCount,
	}.Init(p.ctx, stats, p.blockOffset, p.GetChildReqProps(0))
	topN.SetChildren(childPlan)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputCount := t.count()
	if copTask, ok := t.(*copTask); ok && p.canPushDown(copTask.getStoreType()) && len(copTask.rootTaskConds) == 0 {
		// If all columns in topN are from index plan, we push it to index plan, otherwise we finish the index plan and
		// push it to table plan.
		var pushedDownTopN *PhysicalTopN
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN = p.getPushedDownTopN(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			copTask.finishIndexPlan()
			pushedDownTopN = p.getPushedDownTopN(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
		}
		copTask.addCost(pushedDownTopN.GetCost(inputCount, false))
	} else if mppTask, ok := t.(*mppTask); ok && p.canPushDown(kv.TiFlash) {
		pushedDownTopN := p.getPushedDownTopN(mppTask.p)
		mppTask.p = pushedDownTopN
	}
	rootTask := t.convertToRootTask(p.ctx)
	rootTask.addCost(p.GetCost(rootTask.count(), true))
	p.cost = rootTask.cost()
	return attachPlan2Task(p, rootTask)
}

// GetCost computes the cost of projection operator itself.
func (p *PhysicalProjection) GetCost(count float64) float64 {
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * sessVars.CPUFactor
	concurrency := float64(sessVars.ProjectionConcurrency())
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * sessVars.ConcurrencyFactor
	return cpuCost + concurrencyCost
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		if len(cop.rootTaskConds) == 0 && cop.getStoreType() == kv.TiFlash && expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, p.Exprs, p.ctx.GetClient(), cop.getStoreType()) {
			copTask := attachPlan2Task(p, cop)
			copTask.addCost(p.GetCost(t.count()))
			p.cost = copTask.cost()
			return copTask
		}
	} else if mpp, ok := t.(*mppTask); ok {
		if expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, p.Exprs, p.ctx.GetClient(), kv.TiFlash) {
			p.SetChildren(mpp.p)
			mpp.p = p
			mpp.addCost(p.GetCost(t.count()))
			p.cost = mpp.cost()
			return mpp
		}
	}
	// TODO: support projection push down for TiKV.
	t = t.convertToRootTask(p.ctx)
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	p.cost = t.cost()
	return t
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	t := &rootTask{p: p}
	childPlans := make([]PhysicalPlan, 0, len(tasks))
	var childMaxCost float64
	for _, task := range tasks {
		task = task.convertToRootTask(p.ctx)
		childCost := task.cost()
		if childCost > childMaxCost {
			childMaxCost = childCost
		}
		childPlans = append(childPlans, task.plan())
	}
	p.SetChildren(childPlans...)
	sessVars := p.ctx.GetSessionVars()
	// Children of UnionExec are executed in parallel.
	t.cst = childMaxCost + float64(1+len(tasks))*sessVars.ConcurrencyFactor
	p.cost = t.cost()
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	sessVars := sel.ctx.GetSessionVars()
	if mppTask, _ := tasks[0].(*mppTask); mppTask != nil { // always push to mpp task.
		sc := sel.ctx.GetSessionVars().StmtCtx
		if expression.CanExprsPushDown(sc, sel.Conditions, sel.ctx.GetClient(), kv.TiFlash) {
			sel.cost = mppTask.cost()
			return attachPlan2Task(sel, mppTask.copy())
		}
	}
	t := tasks[0].convertToRootTask(sel.ctx)
	t.addCost(t.count() * sessVars.CPUFactor)
	sel.cost = t.cost()
	return attachPlan2Task(sel, t)
}

// CheckAggCanPushCop checks whether the aggFuncs and groupByItems can
// be pushed down to coprocessor.
func CheckAggCanPushCop(sctx sessionctx.Context, aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression, storeType kv.StoreType) bool {
	sc := sctx.GetSessionVars().StmtCtx
	client := sctx.GetClient()
	for _, aggFunc := range aggFuncs {
		// if the aggFunc contain VirtualColumn or CorrelatedColumn, it can not be pushed down.
		if expression.ContainVirtualColumn(aggFunc.Args) || expression.ContainCorrelatedColumn(aggFunc.Args) {
			return false
		}
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return false
		}
		if !aggregation.CheckAggPushDown(aggFunc, storeType) {
			if sc.InExplainStmt {
				storageName := storeType.Name()
				if storeType == kv.UnSpecified {
					storageName = "storage layer"
				}
				sc.AppendWarning(errors.New("Agg function '" + aggFunc.Name + "' can not be pushed to " + storageName))
			}
			return false
		}
		if !expression.CanExprsPushDown(sc, aggFunc.Args, client, storeType) {
			return false
		}
	}
	if expression.ContainVirtualColumn(groupByItems) {
		return false
	}
	return expression.CanExprsPushDown(sc, groupByItems, client, storeType)
}

// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	Schema       *expression.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the information of partial and final agg.
// partialIsCop means whether partial agg is a cop task.
func BuildFinalModeAggregation(
	sctx sessionctx.Context, original *AggInfo, partialIsCop bool, isMPPTask bool) (partial, final *AggInfo, funcMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {

	funcMap = make(map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc, len(original.AggFuncs))
	partial = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, 0, len(original.AggFuncs)),
		GroupByItems: original.GroupByItems,
		Schema:       expression.NewSchema(),
	}
	partialCursor := 0
	final = &AggInfo{
		AggFuncs:     make([]*aggregation.AggFuncDesc, len(original.AggFuncs)),
		GroupByItems: make([]expression.Expression, 0, len(original.GroupByItems)),
		Schema:       original.Schema,
	}

	partialGbySchema := expression.NewSchema()
	// add group by columns
	for _, gbyExpr := range partial.GroupByItems {
		var gbyCol *expression.Column
		if col, ok := gbyExpr.(*expression.Column); ok {
			gbyCol = col
		} else {
			gbyCol = &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  gbyExpr.GetType(),
			}
		}
		partialGbySchema.Append(gbyCol)
		final.GroupByItems = append(final.GroupByItems, gbyCol)
	}

	// TODO: Refactor the way of constructing aggregation functions.
	// This fop loop is ugly, but I do not find a proper way to reconstruct
	// it right away.
	for i, aggFunc := range original.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		args := make([]expression.Expression, 0, len(aggFunc.Args))
		if aggFunc.HasDistinct {
			/*
				eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c

				change from
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
				to
					[root] group by: c, funcs:count(distinct a), funcs:sum(b)
						[cop]: group by: c, a
			*/
			for _, distinctArg := range aggFunc.Args {
				// 1. add all args to partial.GroupByItems
				foundInGroupBy := false
				for j, gbyExpr := range partial.GroupByItems {
					if gbyExpr.Equal(sctx, distinctArg) {
						foundInGroupBy = true
						args = append(args, partialGbySchema.Columns[j])
						break
					}
				}
				if !foundInGroupBy {
					partial.GroupByItems = append(partial.GroupByItems, distinctArg)
					var gbyCol *expression.Column
					if col, ok := distinctArg.(*expression.Column); ok {
						gbyCol = col
					} else {
						gbyCol = &expression.Column{
							UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
							RetType:  distinctArg.GetType(),
						}
					}
					partialGbySchema.Append(gbyCol)
					if !partialIsCop {
						// if partial is a cop task, firstrow function is redundant since group by items are outputted
						// by group by schema, and final functions use group by schema as their arguments.
						// if partial agg is not cop, we must append firstrow function & schema, to output the group by
						// items.
						// maybe we can unify them sometime.
						firstRow, err := aggregation.NewAggFuncDesc(sctx, ast.AggFuncFirstRow, []expression.Expression{distinctArg}, false)
						if err != nil {
							panic("NewAggFuncDesc FirstRow meets error: " + err.Error())
						}
						partial.AggFuncs = append(partial.AggFuncs, firstRow)
						newCol, _ := gbyCol.Clone().(*expression.Column)
						newCol.RetType = firstRow.RetTp
						partial.Schema.Append(newCol)
						partialCursor++
					}
					args = append(args, gbyCol)
				}
			}

			finalAggFunc.HasDistinct = true
			finalAggFunc.Mode = aggregation.CompleteMode
		} else {
			if aggregation.NeedCount(finalAggFunc.Name) {
				if isMPPTask && finalAggFunc.Name == ast.AggFuncCount {
					// For MPP Task, the final count() is changed to sum().
					// Note: MPP mode does not run avg() directly, instead, avg() -> sum()/(case when count() = 0 then 1 else count() end),
					// so we do not process it here.
					finalAggFunc.Name = ast.AggFuncSum
				} else {
					ft := types.NewFieldType(mysql.TypeLonglong)
					ft.Flen, ft.Charset, ft.Collate = 21, charset.CharsetBin, charset.CollationBin
					partial.Schema.Append(&expression.Column{
						UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
						RetType:  ft,
					})
					args = append(args, partial.Schema.Columns[partialCursor])
					partialCursor++
				}
			}
			if finalAggFunc.Name == ast.AggFuncApproxCountDistinct {
				ft := types.NewFieldType(mysql.TypeString)
				ft.Charset, ft.Collate = charset.CharsetBin, charset.CollationBin
				ft.Flag |= mysql.NotNullFlag
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  ft,
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggregation.NeedValue(finalAggFunc.Name) {
				partial.Schema.Append(&expression.Column{
					UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
					RetType:  original.Schema.Columns[i].GetType(),
				})
				args = append(args, partial.Schema.Columns[partialCursor])
				partialCursor++
			}
			if aggFunc.Name == ast.AggFuncAvg {
				cntAgg := aggFunc.Clone()
				cntAgg.Name = ast.AggFuncCount
				cntAgg.RetTp = partial.Schema.Columns[partialCursor-2].GetType()
				cntAgg.RetTp.Flag = aggFunc.RetTp.Flag
				// we must call deep clone in this case, to avoid sharing the arguments.
				sumAgg := aggFunc.Clone()
				sumAgg.Name = ast.AggFuncSum
				sumAgg.RetTp = partial.Schema.Columns[partialCursor-1].GetType()
				partial.AggFuncs = append(partial.AggFuncs, cntAgg, sumAgg)
			} else if aggFunc.Name == ast.AggFuncApproxCountDistinct {
				approxCountDistinctAgg := *aggFunc
				approxCountDistinctAgg.Name = ast.AggFuncApproxCountDistinct
				approxCountDistinctAgg.RetTp = partial.Schema.Columns[partialCursor-1].GetType()
				partial.AggFuncs = append(partial.AggFuncs, &approxCountDistinctAgg)
			} else {
				partial.AggFuncs = append(partial.AggFuncs, aggFunc)
			}

			finalAggFunc.Mode = aggregation.FinalMode
			funcMap[aggFunc] = finalAggFunc
		}

		finalAggFunc.Args = args
		finalAggFunc.RetTp = aggFunc.RetTp
		final.AggFuncs[i] = finalAggFunc
	}
	partial.Schema.Append(partialGbySchema.Columns...)
	return
}

// convertAvgForMPP converts avg(arg) to sum(arg)/(case when count(arg)=0 then 1 else count(arg) end), in detail:
// 1.rewrite avg() in the final aggregation to count() and sum(), and reconstruct its schema.
// 2.replace avg() with sum(arg)/(case when count(arg)=0 then 1 else count(arg) end) and reuse the original schema of the final aggregation.
// If there is no avg, nothing is changed and return nil.
func (p *basePhysicalAgg) convertAvgForMPP() *PhysicalProjection {
	newSchema := expression.NewSchema()
	newSchema.Keys = p.schema.Keys
	newSchema.UniqueKeys = p.schema.UniqueKeys
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, 2*len(p.AggFuncs))
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft.Flen, ft.Decimal, ft.Charset, ft.Collate = 20, 0, charset.CharsetBin, charset.CollationBin
	exprs := make([]expression.Expression, 0, 2*len(p.schema.Columns))
	// add agg functions schema
	for i, aggFunc := range p.AggFuncs {
		if aggFunc.Name == ast.AggFuncAvg {
			// inset a count(column)
			avgCount := aggFunc.Clone()
			avgCount.Name = ast.AggFuncCount
			newAggFuncs = append(newAggFuncs, avgCount)
			avgCount.RetTp = ft
			avgCountCol := &expression.Column{
				UniqueID: p.SCtx().GetSessionVars().AllocPlanColumnID(),
				RetType:  ft,
			}
			newSchema.Append(avgCountCol)
			// insert a sum(column)
			avgSum := aggFunc.Clone()
			avgSum.Name = ast.AggFuncSum
			newAggFuncs = append(newAggFuncs, avgSum)
			newSchema.Append(p.schema.Columns[i])
			avgSumCol := p.schema.Columns[i]
			// avgSumCol/(case when avgCountCol=0 then 1 else avgCountCol end)
			eq := expression.NewFunctionInternal(p.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), avgCountCol, expression.NewZero())
			caseWhen := expression.NewFunctionInternal(p.ctx, ast.Case, avgCountCol.RetType, eq, expression.NewOne(), avgCountCol)
			divide := expression.NewFunctionInternal(p.ctx, ast.Div, avgSumCol.RetType, avgSumCol, caseWhen)
			divide.(*expression.ScalarFunction).RetType = avgSumCol.RetType
			exprs = append(exprs, divide)
		} else {
			newAggFuncs = append(newAggFuncs, aggFunc)
			newSchema.Append(p.schema.Columns[i])
			exprs = append(exprs, p.schema.Columns[i])
		}
	}
	// no avgs
	// for final agg, always add project due to in-compatibility between TiDB and TiFlash
	if len(p.schema.Columns) == len(newSchema.Columns) && !p.isFinalAgg() {
		return nil
	}
	// add remaining columns to exprs
	for i := len(p.AggFuncs); i < len(p.schema.Columns); i++ {
		exprs = append(exprs, p.schema.Columns[i])
	}
	proj := PhysicalProjection{
		Exprs:                exprs,
		CalculateNoDelay:     false,
		AvoidColumnEvaluator: false,
	}.Init(p.SCtx(), p.stats, p.SelectBlockOffset(), p.GetChildReqProps(0).CloneEssentialFields())
	proj.SetSchema(p.schema)

	p.AggFuncs = newAggFuncs
	p.schema = newSchema

	return proj
}

func (p *basePhysicalAgg) newPartialAggregate(copTaskType kv.StoreType, isMPPTask bool) (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.ctx, p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.self
	}
	partialPref, finalPref, funcMap := BuildFinalModeAggregation(p.ctx, &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	}, true, isMPPTask)
	if p.tp == plancodec.TypeStreamAgg && len(partialPref.GroupByItems) != len(finalPref.GroupByItems) {
		return nil, p.self
	}
	// Remove unnecessary FirstRow.
	partialPref.AggFuncs = RemoveUnnecessaryFirstRow(p.ctx,
		finalPref.AggFuncs, finalPref.GroupByItems,
		partialPref.AggFuncs, partialPref.GroupByItems, partialPref.Schema, funcMap)
	if copTaskType == kv.TiDB {
		// For partial agg of TiDB cop task, since TiDB coprocessor reuse the TiDB executor,
		// and TiDB aggregation executor won't output the group by value,
		// so we need add `firstrow` aggregation function to output the group by value.
		aggFuncs, err := genFirstRowAggForGroupBy(p.ctx, partialPref.GroupByItems)
		if err != nil {
			return nil, p.self
		}
		partialPref.AggFuncs = append(partialPref.AggFuncs, aggFuncs...)
	}
	p.AggFuncs = partialPref.AggFuncs
	p.GroupByItems = partialPref.GroupByItems
	p.schema = partialPref.Schema
	partialAgg := p.self
	// Create physical "final" aggregation.
	prop := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	if p.tp == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
			MppRunMode:   p.MppRunMode,
		}.initForStream(p.ctx, p.stats, p.blockOffset, prop)
		finalAgg.schema = finalPref.Schema
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
		MppRunMode:   p.MppRunMode,
	}.initForHash(p.ctx, p.stats, p.blockOffset, prop)
	finalAgg.schema = finalPref.Schema
	return partialAgg, finalAgg
}

func genFirstRowAggForGroupBy(ctx sessionctx.Context, groupByItems []expression.Expression) ([]*aggregation.AggFuncDesc, error) {
	aggFuncs := make([]*aggregation.AggFuncDesc, 0, len(groupByItems))
	for _, groupBy := range groupByItems {
		agg, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{groupBy}, false)
		if err != nil {
			return nil, err
		}
		aggFuncs = append(aggFuncs, agg)
	}
	return aggFuncs, nil
}

// RemoveUnnecessaryFirstRow removes unnecessary FirstRow of the aggregation. This function can be
// used for both LogicalAggregation and PhysicalAggregation.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schema is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schema to [count(b), a] , and change the index to get value.
func RemoveUnnecessaryFirstRow(
	sctx sessionctx.Context,
	finalAggFuncs []*aggregation.AggFuncDesc,
	finalGbyItems []expression.Expression,
	partialAggFuncs []*aggregation.AggFuncDesc,
	partialGbyItems []expression.Expression,
	partialSchema *expression.Schema,
	funcMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) []*aggregation.AggFuncDesc {

	partialCursor := 0
	newAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(partialAggFuncs))
	for _, aggFunc := range partialAggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range partialGbyItems {
				if j >= len(finalGbyItems) {
					// after distinct push, len(partialGbyItems) may larger than len(finalGbyItems)
					// for example,
					// select /*+ HASH_AGG() */ a, count(distinct a) from t;
					// will generate to,
					//   HashAgg root  funcs:count(distinct a), funcs:firstrow(a)"
					//     HashAgg cop  group by:a, funcs:firstrow(a)->Column#6"
					// the firstrow in root task can not be removed.
					break
				}
				if gbyExpr.Equal(sctx, aggFunc.Args[0]) {
					canOptimize = true
					funcMap[aggFunc].Args[0] = finalGbyItems[j]
					break
				}
			}
			if canOptimize {
				partialSchema.Columns = append(partialSchema.Columns[:partialCursor], partialSchema.Columns[partialCursor+1:]...)
				continue
			}
		}
		partialCursor += computePartialCursorOffset(aggFunc.Name)
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

func computePartialCursorOffset(name string) int {
	offset := 0
	if aggregation.NeedCount(name) {
		offset++
	}
	if aggregation.NeedValue(name) {
		offset++
	}
	if name == ast.AggFuncApproxCountDistinct {
		offset++
	}
	return offset
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		// We should not push agg down across double read, since the data of second read is ordered by handle instead of index.
		// The `extraHandleCol` is added if the double read needs to keep order. So we just use it to decided
		// whether the following plan is double read with order reserved.
		if cop.extraHandleCol != nil || len(cop.rootTaskConds) > 0 {
			t = cop.convertToRootTask(p.ctx)
			inputRows = t.count()
			attachPlan2Task(p, t)
		} else {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
					// If needExtraProj is true, a projection will be created above the PhysicalIndexLookUpReader to make sure
					// the schema is the same as the original DataSource schema.
					// However, we pushed down the agg here, the partial agg was placed on the top of tablePlan, and the final
					// agg will be placed above the PhysicalIndexLookUpReader, and the schema will be set correctly for them.
					// If we add the projection again, the projection will be between the PhysicalIndexLookUpReader and
					// the partial agg, and the schema will be broken.
					cop.needExtraProj = false
				} else {
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
				cop.addCost(p.GetCost(inputRows, false))
				partialAgg.SetCost(cop.cost())
			}
			t = cop.convertToRootTask(p.ctx)
			inputRows = t.count()
			attachPlan2Task(finalAgg, t)
			finalAgg.SetCost(cop.cost())
		}
	} else {
		attachPlan2Task(p, t)
	}
	t.addCost(p.GetCost(inputRows, true))
	p.SetCost(t.cost())
	return t
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot bool) float64 {
	aggFuncFactor := p.getAggFuncCostFactor()
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * aggFuncFactor
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * aggFuncFactor
	}
	rowsPerGroup := inputRows / p.statsInfo().RowCount
	memoryCost := rowsPerGroup * distinctFactor * sessVars.MemoryFactor * float64(p.numDistinctFunc())
	return cpuCost + memoryCost
}

// cpuCostDivisor computes the concurrency to which we would amortize CPU cost
// for hash aggregation.
func (p *PhysicalHashAgg) cpuCostDivisor(hasDistinct bool) (float64, float64) {
	if hasDistinct {
		return 0, 0
	}
	sessionVars := p.ctx.GetSessionVars()
	finalCon, partialCon := sessionVars.HashAggFinalConcurrency(), sessionVars.HashAggPartialConcurrency()
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

func (p *PhysicalHashAgg) attach2TaskForMpp(tasks ...task) task {
	t := tasks[0].copy()
	mpp, ok := t.(*mppTask)
	if !ok {
		return invalidTask
	}
	inputRows := mpp.count()
	switch p.MppRunMode {
	case Mpp1Phase:
		// 1-phase agg: when the partition columns can be satisfied, where the plan does not need to enforce Exchange
		// only push down the original agg
		proj := p.convertAvgForMPP()
		attachPlan2Task(p.self, mpp)
		if proj != nil {
			attachPlan2Task(proj, mpp)
		}
		mpp.addCost(p.GetCost(inputRows, false))
		p.cost = mpp.cost()
		return mpp
	case Mpp2Phase:
		proj := p.convertAvgForMPP()
		partialAgg, finalAgg := p.newPartialAggregate(kv.TiFlash, true)
		if partialAgg == nil {
			return invalidTask
		}
		attachPlan2Task(partialAgg, mpp)
		partitionCols := p.MppPartitionCols
		if len(partitionCols) == 0 {
			items := finalAgg.(*PhysicalHashAgg).GroupByItems
			partitionCols = make([]*expression.Column, 0, len(items))
			for _, expr := range items {
				col, ok := expr.(*expression.Column)
				if !ok {
					return invalidTask
				}
				partitionCols = append(partitionCols, col)
			}
		}
		partialAgg.SetCost(mpp.cost())
		prop := &property.PhysicalProperty{TaskTp: property.MppTaskType, ExpectedCnt: math.MaxFloat64, PartitionTp: property.HashType, PartitionCols: partitionCols}
		newMpp := mpp.enforceExchangerImpl(prop)
		if newMpp.invalid() {
			return newMpp
		}
		attachPlan2Task(finalAgg, newMpp)
		if proj != nil {
			attachPlan2Task(proj, newMpp)
		}
		// TODO: how to set 2-phase cost?
		newMpp.addCost(p.GetCost(inputRows, false))
		finalAgg.SetCost(mpp.cost())
		if proj != nil {
			proj.SetCost(mpp.cost())
		}
		return newMpp
	case MppTiDB:
		partialAgg, finalAgg := p.newPartialAggregate(kv.TiFlash, false)
		if partialAgg != nil {
			attachPlan2Task(partialAgg, mpp)
		}
		mpp.addCost(p.GetCost(inputRows, false))
		if partialAgg != nil {
			partialAgg.SetCost(mpp.cost())
		}
		t = mpp.convertToRootTask(p.ctx)
		inputRows = t.count()
		attachPlan2Task(finalAgg, t)
		t.addCost(p.GetCost(inputRows, true))
		finalAgg.SetCost(t.cost())
		return t
	default:
		return invalidTask
	}
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		if len(cop.rootTaskConds) == 0 {
			copTaskType := cop.getStoreType()
			partialAgg, finalAgg := p.newPartialAggregate(copTaskType, false)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
					// If needExtraProj is true, a projection will be created above the PhysicalIndexLookUpReader to make sure
					// the schema is the same as the original DataSource schema.
					// However, we pushed down the agg here, the partial agg was placed on the top of tablePlan, and the final
					// agg will be placed above the PhysicalIndexLookUpReader, and the schema will be set correctly for them.
					// If we add the projection again, the projection will be between the PhysicalIndexLookUpReader and
					// the partial agg, and the schema will be broken.
					cop.needExtraProj = false
				} else {
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
				cop.addCost(p.GetCost(inputRows, false))
			}
			// In `newPartialAggregate`, we are using stats of final aggregation as stats
			// of `partialAgg`, so the network cost of transferring result rows of `partialAgg`
			// to TiDB is normally under-estimated for hash aggregation, since the group-by
			// column may be independent of the column used for region distribution, so a closer
			// estimation of network cost for hash aggregation may multiply the number of
			// regions involved in the `partialAgg`, which is unknown however.
			t = cop.convertToRootTask(p.ctx)
			inputRows = t.count()
			attachPlan2Task(finalAgg, t)
		} else {
			t = cop.convertToRootTask(p.ctx)
			inputRows = t.count()
			attachPlan2Task(p, t)
		}
	} else if _, ok := t.(*mppTask); ok {
		return p.attach2TaskForMpp(tasks...)
	} else {
		attachPlan2Task(p, t)
	}
	// We may have 3-phase hash aggregation actually, strictly speaking, we'd better
	// calculate cost of each phase and sum the results up, but in fact we don't have
	// region level table stats, and the concurrency of the `partialAgg`,
	// i.e, max(number_of_regions, DistSQLScanConcurrency) is unknown either, so it is hard
	// to compute costs separately. We ignore region level parallelism for both hash
	// aggregation and stream aggregation when calculating cost, though this would lead to inaccuracy,
	// hopefully this inaccuracy would be imposed on both aggregation implementations,
	// so they are still comparable horizontally.
	// Also, we use the stats of `partialAgg` as the input of cost computing for TiDB layer
	// hash aggregation, it would cause under-estimation as the reason mentioned in comment above.
	// To make it simple, we also treat 2-phase parallel hash aggregation in TiDB layer as
	// 1-phase when computing cost.
	t.addCost(p.GetCost(inputRows, true))
	p.cost = t.cost()
	return t
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot bool) float64 {
	cardinality := p.statsInfo().RowCount
	numDistinctFunc := p.numDistinctFunc()
	aggFuncFactor := p.getAggFuncCostFactor()
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * aggFuncFactor
		divisor, con := p.cpuCostDivisor(numDistinctFunc > 0)
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * sessVars.ConcurrencyFactor
		}
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * aggFuncFactor
	}
	memoryCost := cardinality * sessVars.MemoryFactor * float64(len(p.AggFuncs))
	// When aggregation has distinct flag, we would allocate a map for each group to
	// check duplication.
	memoryCost += inputRows * distinctFactor * sessVars.MemoryFactor * float64(numDistinctFunc)
	return cpuCost + memoryCost
}

// mppTask can not :
// 1. keep order
// 2. support double read
// 3. consider virtual columns.
// 4. TODO: partition prune after close
type mppTask struct {
	p   PhysicalPlan
	cst float64

	partTp   property.PartitionType
	hashCols []*expression.Column
}

func (t *mppTask) count() float64 {
	return t.p.statsInfo().RowCount
}

func (t *mppTask) addCost(cst float64) {
	t.cst += cst
}

func (t *mppTask) cost() float64 {
	return t.cst
}

func (t *mppTask) copy() task {
	nt := *t
	return &nt
}

func (t *mppTask) plan() PhysicalPlan {
	return t.p
}

func (t *mppTask) invalid() bool {
	return t.p == nil
}

func (t *mppTask) convertToRootTask(ctx sessionctx.Context) *rootTask {
	return t.copy().(*mppTask).convertToRootTaskImpl(ctx)
}

func (t *mppTask) convertToRootTaskImpl(ctx sessionctx.Context) *rootTask {
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType_PassThrough,
	}.Init(ctx, t.p.statsInfo())
	sender.SetChildren(t.p)
	sender.cost = t.cost()

	p := PhysicalTableReader{
		tablePlan: sender,
		StoreType: kv.TiFlash,
	}.Init(ctx, t.p.SelectBlockOffset())
	p.stats = t.p.statsInfo()
	rt := &rootTask{
		p:   p,
		cst: t.cst / p.ctx.GetSessionVars().CopTiFlashConcurrencyFactor,
	}
	p.cost = rt.cost()
	return rt
}

func (t *mppTask) needEnforce(prop *property.PhysicalProperty) bool {
	switch prop.PartitionTp {
	case property.AnyType:
		return false
	case property.BroadcastType:
		return true
	default:
		if t.partTp != property.HashType {
			return true
		}
		// TODO: consider equalivant class
		if len(prop.PartitionCols) != len(t.hashCols) {
			return true
		}
		for i, col := range prop.PartitionCols {
			if !col.Equal(nil, t.hashCols[i]) {
				return true
			}
		}
		return false
	}
}

func (t *mppTask) enforceExchanger(prop *property.PhysicalProperty) *mppTask {
	if len(prop.SortItems) != 0 {
		return &mppTask{}
	}
	if !t.needEnforce(prop) {
		return t
	}
	return t.copy().(*mppTask).enforceExchangerImpl(prop)
}

func (t *mppTask) enforceExchangerImpl(prop *property.PhysicalProperty) *mppTask {
	if collate.NewCollationEnabled() && prop.PartitionTp == property.HashType {
		for _, col := range prop.PartitionCols {
			if types.IsString(col.RetType.Tp) {
				return &mppTask{cst: math.MaxFloat64}
			}
		}
	}
	ctx := t.p.SCtx()
	sender := PhysicalExchangeSender{
		ExchangeType: tipb.ExchangeType(prop.PartitionTp),
		HashCols:     prop.PartitionCols,
	}.Init(ctx, t.p.statsInfo())
	sender.SetChildren(t.p)
	receiver := PhysicalExchangeReceiver{}.Init(ctx, t.p.statsInfo())
	receiver.SetChildren(sender)
	cst := t.cst + t.count()*ctx.GetSessionVars().NetworkFactor
	sender.cost = cst
	receiver.cost = cst
	return &mppTask{
		p:        receiver,
		cst:      cst,
		partTp:   prop.PartitionTp,
		hashCols: prop.PartitionCols,
	}
}
