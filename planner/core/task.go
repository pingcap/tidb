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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
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

	extraHandleCol *expression.Column
	// tblColHists stores the original stats of DataSource, it is used to get
	// average row width when computing network cost.
	tblColHists *statistics.HistColl
	// tblCols stores the original columns of DataSource before being pruned, it
	// is used to compute average row width when computing scan cost.
	tblCols []*expression.Column
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
	// Network cost of transferring rows of index scan to TiDB.
	t.cst += cnt * netWorkFactor * t.tblColHists.GetAvgRowSize(t.indexPlan.Schema().Columns, true)
	if t.tablePlan == nil {
		return
	}
	// Calculate the IO cost of table scan here because we cannot know its stats until we finish index plan.
	t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
	rowSize := t.tblColHists.GetAvgRowSize(t.tblCols, false)
	t.cst += cnt * rowSize * scanFactor
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	t := finishCopTask(p.ctx, tasks[0].copy())
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	var cpuCost float64
	lCount := lTask.count()
	if len(p.LeftConditions) > 0 {
		cpuCost += lCount * cpuFactor
		lCount *= selectionFactor
	}
	rCount := rTask.count()
	if len(p.RightConditions) > 0 {
		cpuCost += lCount * rCount * cpuFactor
		rCount *= selectionFactor
	}
	if len(p.EqualConditions)+len(p.OtherConditions) > 0 {
		cpuCost += lCount * rCount * cpuFactor
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
	// Add the cost of evaluating outer filter, since inner filter of index join
	// is always empty, we can simply tell whether outer filter is empty using the
	// summed length of left/right conditions.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		cpuCost += cpuFactor * outerCnt
		outerCnt *= selectionFactor
	}
	// Cost of extracting lookup keys.
	innerCPUCost := cpuFactor * outerCnt
	// Cost of sorting and removing duplicate lookup keys:
	// (outerCnt / batchSize) * (batchSize * Log2(batchSize) + batchSize) * cpuFactor
	batchSize := math.Min(float64(p.ctx.GetSessionVars().IndexJoinBatchSize), outerCnt)
	if batchSize > 2 {
		innerCPUCost += outerCnt * (math.Log2(batchSize) + 1) * cpuFactor
	}
	// Add cost of building inner executors. CPU cost of building copTasks:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * cpuFactor
	// Since we don't know the number of copTasks built, ignore these network cost now.
	innerCPUCost += outerCnt * distinctFactor * cpuFactor
	// CPU cost of building hash table for inner results:
	// (outerCnt / batchSize) * (batchSize * distinctFactor) * innerCnt * cpuFactor
	innerCPUCost += outerCnt * distinctFactor * innerCnt * cpuFactor
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
	probeCost := numPairs * cpuFactor
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + (innerConcurrency+1.0)*concurrencyFactor
	// Memory cost of hash tables for inner rows. The computed result is the upper bound,
	// since the executor is pipelined and not all workers are always in full load.
	memoryCost := innerConcurrency * (batchSize * distinctFactor) * innerCnt * memoryFactor
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
	// Cost of building hash table.
	cpuCost := innerCnt * cpuFactor
	memoryCost := innerCnt * memoryFactor
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
	probeCost := numPairs * cpuFactor
	// Cost of evaluating outer filter.
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		// Input outer count for the above compution should be adjusted by selectionFactor.
		probeCost *= selectionFactor
		probeCost += outerCnt * cpuFactor
	}
	probeCost /= float64(p.Concurrency)
	// Cost of additional concurrent goroutines.
	cpuCost += probeCost + float64(p.Concurrency+1)*concurrencyFactor
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
	probeCost := numPairs * cpuFactor
	// Cost of evaluating outer filters.
	var cpuCost float64
	if len(p.LeftConditions)+len(p.RightConditions) > 0 {
		probeCost *= selectionFactor
		cpuCost += outerCnt * cpuFactor
	}
	cpuCost += probeCost
	// For merge join, only one group of rows with same join key(not null) are cached,
	// we compute averge memory cost using estimated group size.
	NDV := getCardinality(innerKeys, innerSchema, innerStats)
	memoryCost := (innerStats.RowCount / NDV) * memoryFactor
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

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(ctx sessionctx.Context, task task) task {
	t, ok := task.(*copTask)
	if !ok {
		return task
	}
	// copTasks are run in parallel, to make the estimated cost closer to execution time, we amortize
	// the cost to cop iterator workers. According to `CopClient::Send`, the concurrency
	// is Min(DistSQLScanConcurrency, numRegionsInvolvedInScan), since we cannot infer
	// the number of regions involved, we simply use DistSQLScanConcurrency.
	copIterWorkers := float64(t.plan().SCtx().GetSessionVars().DistSQLScanConcurrency)
	t.finishIndexPlan()
	// Network cost of transferring rows of table scan to TiDB.
	if t.tablePlan != nil {
		t.cst += t.count() * netWorkFactor * t.tblColHists.GetAvgRowSize(t.tablePlan.Schema().Columns, false)
	}
	t.cst /= copIterWorkers
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		p := PhysicalIndexLookUpReader{
			tablePlan:      t.tablePlan,
			indexPlan:      t.indexPlan,
			ExtraHandleCol: t.extraHandleCol,
		}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		// Add cost of building table reader executors. Handles are extracted in batch style,
		// each handle is a range, the CPU cost of building copTasks should be:
		// (indexRows / batchSize) * batchSize * cpuFactor
		// Since we don't know the number of copTasks built, ignore these network cost now.
		indexRows := t.indexPlan.statsInfo().RowCount
		newTask.cst += indexRows * cpuFactor
		// Add cost of worker goroutines in index lookup.
		numTblWorkers := float64(t.indexPlan.SCtx().GetSessionVars().IndexLookupConcurrency)
		newTask.cst += (numTblWorkers + 1) * concurrencyFactor
		// When building table reader executor for each batch, we would sort the handles. CPU
		// cost of sort is:
		// cpuFactor * batchSize * Log2(batchSize) * (indexRows / batchSize)
		indexLookupSize := float64(t.indexPlan.SCtx().GetSessionVars().IndexLookupSize)
		batchSize := math.Min(indexLookupSize, indexRows)
		if batchSize > 2 {
			sortCPUCost := (indexRows * math.Log2(batchSize) * cpuFactor) / numTblWorkers
			newTask.cst += sortCPUCost
		}
		// Also, we need to sort the retrieved rows if index lookup reader is expected to return
		// ordered results. Note that row count of these two sorts can be different, if there are
		// operators above table scan.
		tableRows := t.tablePlan.statsInfo().RowCount
		selectivity := tableRows / indexRows
		batchSize = math.Min(indexLookupSize*selectivity, tableRows)
		if t.keepOrder && batchSize > 2 {
			sortCPUCost := (tableRows * math.Log2(batchSize) * cpuFactor) / numTblWorkers
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
		p := PhysicalTableReader{tablePlan: t.tablePlan}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
		newTask.p = p
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
	if cop, ok := t.(*copTask); ok {
		// For double read which requires order being kept, the limit cannot be pushed down to the table side,
		// because handles would be reordered before being sent to table scan.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
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
	}
	t = attachPlan2Task(p, t)
	return t
}

// GetCost computes cost of TopN operator itself.
func (p *PhysicalTopN) GetCost(count float64, isRoot bool) float64 {
	heapSize := float64(p.Offset + p.Count)
	if heapSize < 2.0 {
		heapSize = 2.0
	}
	// Ignore the cost of `doCompaction` in current implementation of `TopNExec`, since it is the
	// special side-effect of our Chunk format in TiDB layer, which may not exist in coprocessor's
	// implementation, or may be removed in the future if we change data format.
	// Note that we are using worst complexity to compute CPU cost, because it is simpler compared with
	// considering probabilities of average complexity, i.e, we may not need adjust heap for each input
	// row.
	var cpuCost float64
	if isRoot {
		cpuCost = count * math.Log2(heapSize) * cpuFactor
	} else {
		cpuCost = count * math.Log2(heapSize) * copCPUFactor
	}
	memoryCost := heapSize * memoryFactor
	return cpuCost + memoryCost
}

// canPushDown checks if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *PhysicalTopN) canPushDown() bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	_, _, remained := expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
	return len(remained) == 0
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
	return count*math.Log2(count)*cpuFactor + count*memoryFactor
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
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
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
	cpuCost := count * cpuFactor
	concurrency := float64(p.ctx.GetSessionVars().ProjectionConcurrency)
	if concurrency <= 0 {
		return cpuCost
	}
	cpuCost /= concurrency
	concurrencyCost := (1 + concurrency) * concurrencyFactor
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
	// Children of UnionExec are executed in parallel.
	t.cst = childMaxCost + float64(1+len(tasks))*concurrencyFactor
	return t
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	t := finishCopTask(sel.ctx, tasks[0].copy())
	t.addCost(t.count() * cpuFactor)
	t = attachPlan2Task(sel, t)
	return t
}

func (p *basePhysicalAgg) newPartialAggregate() (partial, final PhysicalPlan) {
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return nil, p.self
		}
	}
	_, _, remained := expression.ExpressionsToPB(sc, p.GroupByItems, client)
	if len(remained) > 0 {
		return nil, p.self
	}

	finalSchema := p.schema
	partialSchema := expression.NewSchema()
	p.schema = partialSchema
	partialAgg := p.self

	// TODO: Refactor the way of constructing aggregation functions.
	partialCursor := 0
	finalAggFuncs := make([]*aggregation.AggFuncDesc, len(p.AggFuncs))
	for i, aggFunc := range p.AggFuncs {
		finalAggFunc := &aggregation.AggFuncDesc{HasDistinct: false}
		finalAggFunc.Name = aggFunc.Name
		args := make([]expression.Expression, 0, len(aggFunc.Args))
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
		finalAggFunc.RetTp = aggFunc.RetTp
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

	// Remove unnecessary FirstRow.
	p.removeUnnecessaryFirstRow(finalAggFuncs, groupByItems)

	// Create physical "final" aggregation.
	if p.tp == TypeStreamAgg {
		finalAgg := basePhysicalAgg{
			AggFuncs:     finalAggFuncs,
			GroupByItems: groupByItems,
		}.initForStream(p.ctx, p.stats)
		finalAgg.schema = finalSchema
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalAggFuncs,
		GroupByItems: groupByItems,
	}.initForHash(p.ctx, p.stats)
	finalAgg.schema = finalSchema
	return partialAgg, finalAgg
}

// Remove unnecessary FirstRow.
// When the select column is same with the group by key, the column can be removed and gets value from the group by key.
// e.g
// select a, count(b) from t group by a;
// The schema is [firstrow(a), count(b), a]. The column firstrow(a) is unnecessary.
// Can optimize the schema to [count(b), a] , and change the index to get value.
func (p *basePhysicalAgg) removeUnnecessaryFirstRow(finalAggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) {
	partialCursor := 0
	partialAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(p.AggFuncs))
	for i, aggFunc := range p.AggFuncs {
		if aggFunc.Name == ast.AggFuncFirstRow {
			canOptimize := false
			for j, gbyExpr := range p.GroupByItems {
				if gbyExpr.Equal(p.ctx, aggFunc.Args[0]) {
					canOptimize = true
					finalAggFuncs[i].Args[0] = groupByItems[j]
					break
				}
			}
			if canOptimize {
				p.schema.Columns = append(p.schema.Columns[:partialCursor], p.schema.Columns[partialCursor+1:]...)
				continue
			}
		}
		if aggregation.NeedCount(aggFunc.Name) {
			partialCursor++
		}
		if aggregation.NeedValue(aggFunc.Name) {
			partialCursor++
		}
		partialAggFuncs = append(partialAggFuncs, aggFunc)
	}
	p.AggFuncs = partialAggFuncs
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	inputRows := t.count()
	if cop, ok := t.(*copTask); ok {
		partialAgg, finalAgg := p.newPartialAggregate()
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
		attachPlan2Task(p, t)
	}
	t.addCost(p.GetCost(inputRows, true))
	return t
}

// GetCost computes cost of stream aggregation considering CPU/memory.
func (p *PhysicalStreamAgg) GetCost(inputRows float64, isRoot bool) float64 {
	numAggFunc := len(p.AggFuncs)
	if numAggFunc == 0 {
		numAggFunc = 1
	}
	var cpuCost float64
	if isRoot {
		cpuCost = inputRows * cpuFactor * float64(numAggFunc)
	} else {
		cpuCost = inputRows * copCPUFactor * float64(numAggFunc)
	}
	rowsPerGroup := inputRows / p.statsInfo().RowCount
	memoryCost := rowsPerGroup * distinctFactor * memoryFactor * float64(p.numDistinctFunc())
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
		partialAgg, finalAgg := p.newPartialAggregate()
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
	numAggFunc := len(p.AggFuncs)
	if numAggFunc == 0 {
		numAggFunc = 1
	}
	var cpuCost float64
	if isRoot {
		cpuCost = inputRows * cpuFactor * float64(numAggFunc)
		divisor, con := p.cpuCostDivisor(numDistinctFunc > 0)
		if divisor > 0 {
			cpuCost /= divisor
			// Cost of additional goroutines.
			cpuCost += (con + 1) * concurrencyFactor
		}
	} else {
		cpuCost = inputRows * copCPUFactor * float64(numAggFunc)
	}
	memoryCost := cardinality * memoryFactor * float64(numAggFunc)
	// When aggregation has distinct flag, we would allocate a map for each group to
	// check duplication.
	memoryCost += inputRows * distinctFactor * memoryFactor * float64(numDistinctFunc)
	return cpuCost + memoryCost
}
