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
	"fmt"
	"math"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/plancodec"
)

// task is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type task interface {
	count() float64
	addCost(cost float64)
	cost() float64
	copy() task
	plan() PhysicalPlan
	invalid() bool
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
	// In double read case, it may output one more column for handle(row id).
	// We need to prune it, so we add a project do this.
	doubleReadNeedProj bool
	// tableStats stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tableStats *property.StatsInfo
	// tableCols store the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tableCols []*expression.Column

	rootTaskConds []expression.Expression
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
	sessVars := t.indexPlan.context().GetSessionVars()
	// Network cost of transferring rows of index scan to TiDB.
	t.cst += cnt * sessVars.NetworkFactor * t.tableStats.HistColl.GetAvgRowSize(t.indexPlan.Schema().Columns, true)
	if t.tablePlan == nil {
		return
	}
	// Calculate the IO cost of table scan here because we cannot know its stats until we finish index plan.
	t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
	var p PhysicalPlan
	for p = t.indexPlan; len(p.Children()) > 0; p = p.Children()[0] {
	}
	rowSize := t.tableStats.HistColl.GetAvgRowSize(t.tableCols, p.(*PhysicalIndexScan).Index.Unique)
	t.cst += cnt * rowSize * sessVars.ScanFactor
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	t := finishCopTask(p.ctx, tasks[0].copy())
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalUnionScan) attach2Task(tasks ...task) task {
	p.stats = tasks[0].plan().statsInfo()
	return p.basePhysicalPlan.attach2Task(tasks...)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	var cpuCost float64
	lCount := lTask.count()
	sessVars := p.ctx.GetSessionVars()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * sessVars.CPUFactor
		lCount *= selectionFactor
	}
	rCount := rTask.count()
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.CPUFactor
		rCount *= selectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions) > 0 {
		cpuCost += lCount * rCount * sessVars.CPUFactor
	}
	return &rootTask{
		p:   p,
		cst: cpuCost + lTask.cost(),
	}
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	innerTask := p.innerTask
	outerTask := finishCopTask(p.ctx, tasks[p.OuterIndex].copy())
	if p.OuterIndex == 0 {
		p.SetChildren(outerTask.plan(), innerTask.plan())
	} else {
		p.SetChildren(innerTask.plan(), outerTask.plan())
	}
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: p.GetCost(outerTask, innerTask),
	}
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
		outerCnt *= selectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := sessVars.CPUFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * cpuFactor
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(float64(batchSize)) + 1) * sessVars.CPUFactor
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * cpuFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * sessVars.CPUFactor
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * innerCnt * CPUFactor
	innerCPUCost += outerCnt * distinctFactor * innerCnt * sessVars.CPUFactor
	innerConcurrency := float64(p.ctx.GetSessionVars().IndexLookupJoinConcurrency)
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

// GetCost computes cost of hash join operator itself.
func (p *PhysicalHashJoin) GetCost(lCnt, rCnt float64) float64 {
	innerCnt, outerCnt := lCnt, rCnt
	if p.InnerChildIdx == 1 {
		innerCnt, outerCnt = rCnt, lCnt
	}
	sessVars := p.ctx.GetSessionVars()
	// Cost of building hash table.
	cpuCost := innerCnt * sessVars.CPUFactor
	memoryCost := innerCnt * sessVars.MemoryFactor
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
	// Cost of quering hash table is cheap actually, so we just compute the cost of
	// evaluating `OtherConditions` and joining row pairs.
	probeCost := numPairs * sessVars.CPUFactor
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by selectionFactor.
		probeCost *= selectionFactor
		probeCost += outerCnt * sessVars.CPUFactor
	}
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*sessVars.ConcurrencyFactor
	return cpuCost + memoryCost
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
}

// GetCost computes cost of merge join operator itself.
func (p *PhysicalMergeJoin) GetCost(lCnt, rCnt float64) float64 {
	outerCnt := lCnt
	innerKeys := p.RightKeys
	innerSchema := p.children[1].Schema()
	innerStats := p.children[1].statsInfo()
	if p.JoinType == RightOuterJoin {
		outerCnt = rCnt
		innerKeys = p.LeftKeys
		innerSchema = p.children[0].Schema()
		innerStats = p.children[0].statsInfo()
	}
	helper := &fullJoinRowCountHelper{
		cartesian:     false,
		leftProfile:   p.children[0].statsInfo(),
		rightProfile:  p.children[1].statsInfo(),
		leftJoinKeys:  p.LeftKeys,
		rightJoinKeys: p.RightKeys,
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
		probeCost *= selectionFactor
		cpuCost += outerCnt * sessVars.CPUFactor
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute averge memory cost using estimated group size.
	NDV := getCardinality(innerKeys, innerSchema, innerStats)
	memoryCost := (innerStats.RowCount / NDV) * sessVars.MemoryFactor
	return cpuCost + memoryCost
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.GetCost(lTask.count(), rTask.count()),
	}
}

// splitCopAvg2CountAndSum splits the cop avg function to count and sum.
// Now it's only used for TableReader.
func splitCopAvg2CountAndSum(p PhysicalPlan) {
	var baseAgg basePhysicalAgg
	if agg, ok := p.(*PhysicalStreamAgg); ok {
		baseAgg = agg.basePhysicalAgg
		defer func() {
			agg.basePhysicalAgg = baseAgg
		}()
	} else if agg, ok := p.(*PhysicalHashAgg); ok {
		baseAgg = agg.basePhysicalAgg
		defer func() {
			agg.basePhysicalAgg = baseAgg
		}()
	} else {
		return
	}
	oldAggFuncs := baseAgg.AggFuncs
	baseAgg.AggFuncs = make([]*aggregation.AggFuncDesc, len(oldAggFuncs))
	copy(baseAgg.AggFuncs, oldAggFuncs)

	schemaCursor := len(baseAgg.Schema().Columns) - len(baseAgg.GroupByItems)
	for i := len(baseAgg.AggFuncs) - 1; i >= 0; i-- {
		f := baseAgg.AggFuncs[i]
		schemaCursor--
		if f.Name == ast.AggFuncAvg {
			schemaCursor--
			sumAgg := *f
			sumAgg.Name = ast.AggFuncSum
			sumAgg.RetTp = baseAgg.Schema().Columns[schemaCursor+1].RetType
			cntAgg := *f
			cntAgg.Name = ast.AggFuncCount
			cntAgg.RetTp = baseAgg.Schema().Columns[schemaCursor].RetType
			cntAgg.RetTp.Flag = f.RetTp.Flag
			baseAgg.AggFuncs = append(baseAgg.AggFuncs[:i], append([]*aggregation.AggFuncDesc{&cntAgg, &sumAgg}, baseAgg.AggFuncs[i+1:]...)...)
		}
	}
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(ctx sessionctx.Context, task task) task {
	t, ok := task.(*copTask)
	if !ok {
		return task
	}
	sessVars := ctx.GetSessionVars()
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(t.plan().context().GetSessionVars().DistSQLScanConcurrency)
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		t.cst += t.count() * sessVars.NetworkFactor * t.tableStats.HistColl.GetAvgRowSize(t.tablePlan.Schema().Columns, false)
	}
	t.cst /= copIterWorkers
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		p := PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		// Add cost of building table reader executors. Handles are extracted in batch style,
		// each handle is a range, the CPU cost of building copTasks should be:
		// (indexRows / batchSize) * batchSize * cpuFactor
		// Since we don't know the number of copTasks built, ignore these network cost now.
		indexRows := t.indexPlan.statsInfo().RowCount
		newTask.cst += indexRows * sessVars.CPUFactor
		// Add cost of worker goroutines in index lookup.
		numTblWorkers := float64(t.indexPlan.context().GetSessionVars().IndexLookupConcurrency)
		newTask.cst += (numTblWorkers + 1) * sessVars.ConcurrencyFactor
		// When building table reader executor for each batch, we would sort the handles. CPU
		// cost of sort is:
		// cpuFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
		indexLookupSize := float64(t.indexPlan.context().GetSessionVars().IndexLookupSize)
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
		if t.doubleReadNeedProj {
			schema := p.IndexPlans[0].(*PhysicalIndexScan).dataSourceSchema
			proj := PhysicalProjection{Exprs: expression.Column2Exprs(schema.Columns)}.Init(ctx, p.stats, nil)
			proj.SetSchema(schema)
			proj.SetChildren(p)
			newTask.p = proj
		} else {
			newTask.p = p
		}
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.Init(ctx)
		p.stats = t.indexPlan.statsInfo()
		newTask.p = p
	} else {
		tp := t.tablePlan
		splitCopAvg2CountAndSum(tp)
		for len(tp.Children()) > 0 {
			tp = tp.Children()[0]
		}
		ts := tp.(*PhysicalTableScan)
		p := PhysicalTableReader{
			tablePlan: t.tablePlan,
			StoreType: ts.StoreType,
		}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		newTask.p = p
	}

	if len(t.rootTaskConds) > 0 {
		sel := PhysicalSelection{Conditions: t.rootTaskConds}.Init(ctx, newTask.p.statsInfo())
		sel.SetChildren(newTask.p)
		newTask.p = sel
	}

	return newTask
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
			pushedDownLimit := PhysicalLimit{Count: newCount}.Init(p.ctx, stats)
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		}
		t = finishCopTask(p.ctx, cop)
		sunk = p.sinkIntoIndexLookUp(t)
	}
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
func (p *PhysicalTopN) canPushDown(cop *copTask) bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	storeType := getStoreTypeFromCopTask(cop)
	return expression.CanExprsPushDown(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient(), storeType)
}

func (p *PhysicalTopN) allColsFromSchema(schema *expression.Schema) bool {
	cols := make([]*expression.Column, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	return len(schema.ColumnsIndices(cols)) > 0
}

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64) float64 {
	if count < 2.0 {
		count = 2.0
	}

	sessVars := p.ctx.GetSessionVars()
	return count*math.Log2(count)*sessVars.CPUFactor + count*sessVars.MemoryFactor
}

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	return tasks[0]
}

func (p *PhysicalTopN) getPushedDownTopN(childPlan PhysicalPlan) *PhysicalTopN {
	newByItems := make([]*ByItems, 0, len(p.ByItems))
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
	}.Init(p.ctx, stats)
	topN.SetChildren(childPlan)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputCount := t.count()
	if copTask, ok := t.(*copTask); ok && p.canPushDown(copTask) && len(copTask.rootTaskConds) == 0 {
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
	}
	rootTask := finishCopTask(p.ctx, t)
	rootTask.addCost(p.GetCost(rootTask.count(), true))
	rootTask = attachPlan2Task(p, rootTask)
	return rootTask
}

// GetCost computes the cost of projection operator itself.
func (p *PhysicalProjection) GetCost(count float64) float64 {
	sessVars := p.ctx.GetSessionVars()
	cpuCost := count * sessVars.CPUFactor
	concurrency := float64(sessVars.ProjectionConcurrency)
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * sessVars.ConcurrencyFactor
	return cpuCost + concurrencyCost
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if copTask, ok := t.(*copTask); ok {
		// TODO: support projection push down.
		t = finishCopTask(p.ctx, copTask)
	}
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	t := &rootTask{p: p}
	childPlans := make([]PhysicalPlan, 0, len(tasks))
	var childMaxCost float64
	for _, task := range tasks {
		task = finishCopTask(p.ctx, task)
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
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	sessVars := sel.ctx.GetSessionVars()
	t := finishCopTask(sel.ctx, tasks[0].copy())
	t.addCost(t.count() * sessVars.CPUFactor)
	t = attachPlan2Task(sel, t)
	return t
}

func (p *basePhysicalAgg) newPartialAggregate(storeType kv.StoreType) (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return nil, p.self
		}
		if !aggregation.CheckAggPushDown(aggFunc, storeType) {
			return nil, p.self
		}
		if !expression.CanExprsPushDown(sc, aggFunc.Args, client, storeType) {
			return nil, p.self
		}
	}

	if !expression.CanExprsPushDown(sc, p.GroupByItems, client, storeType) {
		return nil, p.self
	}

<<<<<<< HEAD
	finalSchema := p.schema
	partialSchema := expression.NewSchema()
	p.schema = partialSchema
	partialAgg := p.self

	// TODO: Refactor the way of constructing aggregation functions.
	partialCursor := 0
	finalAggFuncs := make([]*aggregation.AggFuncDesc, len(p.AggFuncs))
	for i, aggFun := range p.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFun.Name
		args := make([]expression.Expression, 0, len(aggFun.Args))
		if aggregation.NeedCount(finalAggFunc.Name) {
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen, ft.Charset, ft.Collate = 21, charset.CharsetBin, charset.CollationBin
			partialSchema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor)),
				RetType:  ft,
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		if aggregation.NeedValue(finalAggFunc.Name) {
			partialSchema.Append(&expression.Column{
				UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
				ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor)),
				RetType:  finalSchema.Columns[i].GetType(),
			})
			args = append(args, partialSchema.Columns[partialCursor])
			partialCursor++
		}
		finalAggFunc.Args = args
		finalAggFunc.Mode = aggregation.FinalMode
		finalAggFunc.RetTp = aggFun.RetTp
		finalAggFuncs[i] = finalAggFunc
	}

	// add group by columns
	groupByItems := make([]expression.Expression, 0, len(p.GroupByItems))
	for i, gbyExpr := range p.GroupByItems {
		gbyCol := &expression.Column{
			UniqueID: p.ctx.GetSessionVars().AllocPlanColumnID(),
			ColName:  model.NewCIStr(fmt.Sprintf("col_%d", partialCursor+i)),
			RetType:  gbyExpr.GetType(),
		}
		partialSchema.Append(gbyCol)
		groupByItems = append(groupByItems, gbyCol)
	}

	// Create physical "final" aggregation.
	if p.tp == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalAggFuncs,
			GroupByItems: groupByItems,
		}.initForStream(p.ctx, p.stats)
		finalAgg.schema = finalSchema
=======
// AggInfo stores the information of an Aggregation.
type AggInfo struct {
	AggFuncs     []*aggregation.AggFuncDesc
	GroupByItems []expression.Expression
	Schema       *expression.Schema
}

// BuildFinalModeAggregation splits either LogicalAggregation or PhysicalAggregation to finalAgg and partial1Agg,
// returns the body of finalAgg and the schema of partialAgg.
func BuildFinalModeAggregation(
	sctx sessionctx.Context, original *AggInfo) (partial, final *AggInfo, funcMap map[*aggregation.AggFuncDesc]*aggregation.AggFuncDesc) {

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
					args = append(args, gbyCol)
				}
			}
			// Just use groupBy items in Schema of partialAgg as arguments,
			// no need to spawn FirstRow function.

			finalAggFunc.HasDistinct = true
			finalAggFunc.Mode = aggregation.CompleteMode
		} else {
			if aggregation.NeedCount(finalAggFunc.Name) {
				ft := types.NewFieldType(mysql.TypeLonglong)
				ft.Flen, ft.Charset, ft.Collate = 21, charset.CharsetBin, charset.CollationBin
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
			partial.AggFuncs = append(partial.AggFuncs, aggFunc)

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

func (p *basePhysicalAgg) newPartialAggregate(copTaskType kv.StoreType) (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	if !CheckAggCanPushCop(p.ctx, p.AggFuncs, p.GroupByItems, copTaskType) {
		return nil, p.self
	}
	partialPref, finalPref, funcMap := BuildFinalModeAggregation(p.ctx, &AggInfo{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
		Schema:       p.Schema().Clone(),
	})
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
	if p.tp == plancodec.TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalPref.AggFuncs,
			GroupByItems: finalPref.GroupByItems,
		}.initForStream(p.ctx, p.stats, p.blockOffset)
		finalAgg.schema = finalPref.Schema
>>>>>>> 4eb9ca3... planner: push aggregation functions with distinct to cop (#15500)
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
<<<<<<< HEAD
		AggFuncs:     finalAggFuncs,
		GroupByItems: groupByItems,
	}.initForHash(p.ctx, p.stats)
	finalAgg.schema = finalSchema
	return partialAgg, finalAgg
}

=======
		AggFuncs:     finalPref.AggFuncs,
		GroupByItems: finalPref.GroupByItems,
	}.initForHash(p.ctx, p.stats, p.blockOffset)
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
		if aggregation.NeedCount(aggFunc.Name) {
			partialCursor++
		}
		if aggregation.NeedValue(aggFunc.Name) {
			partialCursor++
		}
		newAggFuncs = append(newAggFuncs, aggFunc)
	}
	return newAggFuncs
}

>>>>>>> 4eb9ca3... planner: push aggregation functions with distinct to cop (#15500)
func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		// We should not push agg down across double read, since the data of second read is ordered by handle instead of index.
		// The `doubleReadNeedProj` is always set if the double read needs to keep order. So we just use it to decided
		// whether the following plan is double read with order reserved.
		storeType := getStoreTypeFromCopTask(cop)
		if !cop.doubleReadNeedProj && len(cop.rootTaskConds) == 0 {
			partialAgg, finalAgg := p.newPartialAggregate(storeType)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
				} else {
					partialAgg.SetChildren(cop.indexPlan)
					cop.indexPlan = partialAgg
				}
				cop.addCost(p.GetCost(inputRows, false))
			}
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachPlan2Task(finalAgg, t)
		} else {
			t = finishCopTask(p.ctx, cop)
			attachPlan2Task(p, t)
		}
	} else {
		attachPlan2Task(p, t)
	}
	t.addCost(p.GetCost(inputRows, true))
	return t
}

func getStoreTypeFromCopTask(cop *copTask) kv.StoreType {
	storeType := kv.TiKV
	if tableScan, ok := cop.tablePlan.(*PhysicalTableScan); ok {
		storeType = tableScan.StoreType
	}
	return storeType
}

func isFlashCopTask(cop *copTask) bool {
	if cop.tablePlan == nil {
		return false
	}
	tp := cop.tablePlan
	for len(tp.Children()) > 0 {
		tp = tp.Children()[0]
	}
	if ts, ok := tp.(*PhysicalTableScan); ok {
		return ts.StoreType == kv.TiFlash
	}
	return false
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot bool) float64 {
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * float64(len(p.AggFuncs))
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * float64(len(p.AggFuncs))
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
	finalCon, partialCon := sessionVars.HashAggFinalConcurrency, sessionVars.HashAggPartialConcurrency
	// According to `ValidateSetSystemVar`, `finalCon` and `partialCon` cannot be less than or equal to 0.
	if finalCon == 1 && partialCon == 1 {
		return 0, 0
	}
	// It is tricky to decide which concurrency we should use to amortize CPU cost. Since cost of hash
	// aggregation is tend to be under-estimated as explained in `attach2Task`, we choose the smaller
	// concurrecy to make some compensation.
	return math.Min(float64(finalCon), float64(partialCon)), float64(finalCon + partialCon)
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		if len(cop.rootTaskConds) == 0 {
			storeType := getStoreTypeFromCopTask(cop)
			partialAgg, finalAgg := p.newPartialAggregate(storeType)
			if partialAgg != nil {
				if cop.tablePlan != nil {
					cop.finishIndexPlan()
					partialAgg.SetChildren(cop.tablePlan)
					cop.tablePlan = partialAgg
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
			t = finishCopTask(p.ctx, cop)
			inputRows = t.count()
			attachPlan2Task(finalAgg, t)
		} else {
			t = finishCopTask(p.ctx, cop)
			attachPlan2Task(p, t)
		}
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
	return t
}

// GetCost computes the cost of hash aggregation considering CPU/memory.
func (p *PhysicalHashAgg) GetCost(inputRows float64, isRoot bool) float64 {
	cardinality := p.statsInfo().RowCount
	numDistinctFunc := p.numDistinctFunc()
	var cpuCost float64
	sessVars := p.ctx.GetSessionVars()
	if isRoot {
		cpuCost = inputRows * sessVars.CPUFactor * float64(len(p.AggFuncs))
		divisor, con := p.cpuCostDivisor(numDistinctFunc > 0)
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * sessVars.ConcurrencyFactor
		}
	} else {
		cpuCost = inputRows * sessVars.CopCPUFactor * float64(len(p.AggFuncs))
	}
	memoryCost := cardinality * sessVars.MemoryFactor * float64(len(p.AggFuncs))
	// When aggregation has distinct flag, we would allocate a map for each group to
	// check duplication.
	memoryCost += inputRows * distinctFactor * sessVars.MemoryFactor * float64(numDistinctFunc)
	return cpuCost + memoryCost
}
