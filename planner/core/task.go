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

	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/util"
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
	if !t.indexPlanFinished {
		t.cst += t.count() * netWorkFactor
		t.indexPlanFinished = true
		if t.tablePlan != nil {
			t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.statsInfo()
			t.cst += t.count() * scanFactor
		}
	}
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
	return &rootTask{
		p:   p,
		cst: lTask.cost() + lTask.count()*rTask.cost(),
	}
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	outerTask := finishCopTask(p.ctx, tasks[p.OuterIndex].copy())
	if p.OuterIndex == 0 {
		p.SetChildren(outerTask.plan(), p.innerPlan)
	} else {
		p.SetChildren(p.innerPlan, outerTask.plan())
	}
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: outerTask.cost() + p.getCost(outerTask.count()),
	}
}

func (p *PhysicalIndexJoin) getCost(lCnt float64) float64 {
	if lCnt < 1 {
		lCnt = 1
	}
	cst := lCnt * netWorkFactor
	batchSize := p.ctx.GetSessionVars().IndexJoinBatchSize
	cst += lCnt * math.Log2(math.Min(float64(batchSize), lCnt)) * 2
	cst += lCnt / float64(batchSize) * netWorkStartFactor
	return cst
}

func (p *PhysicalHashJoin) getCost(lCnt, rCnt float64) float64 {
	smallTableCnt := lCnt
	if p.InnerChildIdx == 1 {
		smallTableCnt = rCnt
	}
	if smallTableCnt <= 1 {
		smallTableCnt = 1
	}
	return (lCnt + rCnt) * (1 + math.Log2(smallTableCnt)/float64(p.Concurrency))
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

func (p *PhysicalMergeJoin) getCost(lCnt, rCnt float64) float64 {
	return lCnt + rCnt
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(p.ctx, tasks[0].copy())
	rTask := finishCopTask(p.ctx, tasks[1].copy())
	p.SetChildren(lTask.plan(), rTask.plan())
	p.schema = buildPhysicalJoinSchema(p.JoinType, p)
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(ctx sessionctx.Context, task task) task {
	t, ok := task.(*copTask)
	if !ok {
		return task
	}
	// FIXME: When it is a double reading. The cost should be more expensive. The right cost should add the
	// `NetWorkStartCost` * (totalCount / perCountIndexRead)
	t.finishIndexPlan()
	if t.tablePlan != nil {
		t.cst += t.count() * netWorkFactor
	}
	newTask := &rootTask{
		cst: t.cst,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		p := PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.Init(ctx)
		p.stats = t.tablePlan.statsInfo()
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
	sunk := false
	if cop, ok := t.(*copTask); ok {
		// If the table/index scans data by order and applies a double read, the limit cannot be pushed to the table side.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
			// When limit be pushed down, it should remove its offset.
			pushedDownLimit := PhysicalLimit{Count: p.Offset + p.Count}.Init(p.ctx, p.stats)
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

// GetCost computes the cost of in memory sort.
func (p *PhysicalSort) GetCost(count float64) float64 {
	if count < 2.0 {
		count = 2.0
	}
	return count*cpuFactor + count*memoryFactor
}

func (p *PhysicalTopN) getCost(count float64) float64 {
	return count*cpuFactor + float64(p.Count)*memoryFactor
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

func (p *PhysicalSort) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.GetCost(t.count()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	return tasks[0]
}

func (p *PhysicalTopN) getPushedDownTopN() *PhysicalTopN {
	newByItems := make([]*util.ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   p.Offset + p.Count,
	}.Init(p.ctx, p.stats)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	// This is a topN plan.
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
		pushedDownTopN := p.getPushedDownTopN()
		// If all columns in topN are from index plan, we can push it to index plan. Or we finish the index plan and
		// push it to table plan.
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN.SetChildren(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			// FIXME: When we pushed down a top-N plan to table plan branch in case of double reading. The cost should
			// be more expensive in case of single reading, because we may execute table scan multi times.
			copTask.finishIndexPlan()
			pushedDownTopN.SetChildren(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
		}
		copTask.addCost(pushedDownTopN.getCost(t.count()))
	}
	t = finishCopTask(p.ctx, t)
	t = attachPlan2Task(p, t)
	t.addCost(p.getCost(t.count()))
	return t
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	switch tp := t.(type) {
	case *copTask:
		// TODO: Support projection push down.
		t = finishCopTask(p.ctx, t)
		t = attachPlan2Task(p, t)
		return t
	case *rootTask:
		return attachPlan2Task(p, tp)
	}
	return nil
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	newTask := &rootTask{p: p}
	newChildren := make([]PhysicalPlan, 0, len(p.children))
	for _, task := range tasks {
		task = finishCopTask(p.ctx, task)
		newTask.cst += task.cost()
		newChildren = append(newChildren, task.plan())
	}
	p.SetChildren(newChildren...)
	return newTask
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
		return partialAgg, finalAgg
	}

	finalAgg := basePhysicalAgg{
		AggFuncs:     finalAggFuncs,
		GroupByItems: groupByItems,
	}.initForHash(p.ctx, p.stats)
	finalAgg.schema = finalSchema
	return partialAgg, finalAgg
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		// We should not push agg down across double read, since the data of second read is ordered by handle instead of index.
		// The `doubleReadNeedProj` is always set if the double read needs to keep order. So we just use it to decided
		// whether the following plan is double read with order reserved.
		if !cop.doubleReadNeedProj {
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
			}
			t = finishCopTask(p.ctx, cop)
			attachPlan2Task(finalAgg, t)
		} else {
			t = finishCopTask(p.ctx, cop)
			attachPlan2Task(p, t)
		}
	} else {
		attachPlan2Task(p, t)
	}
	t.addCost(t.count() * cpuFactor)
	if p.hasDistinctFunc() {
		t.addCost(t.count() * cpuFactor * distinctAggFactor)
	}
	return t
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	cardinality := p.statsInfo().RowCount
	t := tasks[0].copy()
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
		}
		t = finishCopTask(p.ctx, cop)
		attachPlan2Task(finalAgg, t)
	} else {
		attachPlan2Task(p, t)
	}
	t.addCost(t.count()*cpuFactor*hashAggFactor + cardinality*createAggCtxFactor)
	if p.hasDistinctFunc() {
		t.addCost(t.count() * cpuFactor * distinctAggFactor)
	}
	return t
}
