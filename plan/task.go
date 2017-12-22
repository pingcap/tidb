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

package plan

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
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
}

func (t *copTask) invalid() bool {
	return t.tablePlan == nil && t.indexPlan == nil
}

func (t *rootTask) invalid() bool {
	return t.p == nil
}

func (t *copTask) count() float64 {
	if t.indexPlanFinished {
		return t.tablePlan.StatsInfo().count
	}
	return t.indexPlan.StatsInfo().count
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
			t.tablePlan.(*PhysicalTableScan).stats = t.indexPlan.StatsInfo()
			t.cst += t.count() * scanFactor
		}
	}
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := finishCopTask(tasks[0].copy(), p.ctx)
	return attachPlan2Task(p.self, t)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	if tasks[0].invalid() || tasks[1].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[0].copy(), p.ctx)
	rTask := finishCopTask(tasks[1].copy(), p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	p.PhysicalJoin.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   p,
		cst: lTask.cost() + lTask.count()*rTask.cost(),
	}
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	if tasks[p.OuterIndex].invalid() {
		return invalidTask
	}
	outerTask := finishCopTask(tasks[p.OuterIndex].copy(), p.ctx)
	if p.OuterIndex == 0 {
		p.SetChildren(outerTask.plan(), p.innerPlan)
	} else {
		p.SetChildren(p.innerPlan, outerTask.plan())
	}
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
	if p.KeepOrder {
		batchSize = 1
	}
	cst += lCnt * math.Log2(math.Min(float64(batchSize), lCnt)) * 2
	cst += lCnt / float64(batchSize) * netWorkStartFactor
	if p.KeepOrder {
		return cst * 2
	}
	return cst
}

func (p *PhysicalHashJoin) getCost(lCnt, rCnt float64) float64 {
	smallTableCnt := lCnt
	if p.SmallChildIdx == 1 {
		smallTableCnt = rCnt
	}
	if smallTableCnt <= 1 {
		smallTableCnt = 1
	}
	return (lCnt + rCnt) * (1 + math.Log2(smallTableCnt)/float64(p.Concurrency))
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	if tasks[0].invalid() || tasks[1].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[0].copy(), p.ctx)
	rTask := finishCopTask(tasks[1].copy(), p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

func (p *PhysicalMergeJoin) getCost(lCnt, rCnt float64) float64 {
	return lCnt + rCnt
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	if tasks[0].invalid() || tasks[1].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[0].copy(), p.ctx)
	rTask := finishCopTask(tasks[1].copy(), p.ctx)
	p.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   p,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(task task, ctx context.Context) task {
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
		p := PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.init(ctx)
		p.stats = t.tablePlan.StatsInfo()
		newTask.p = p
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.init(ctx)
		p.stats = t.indexPlan.StatsInfo()
		newTask.p = p
	} else {
		p := PhysicalTableReader{tablePlan: t.tablePlan}.init(ctx)
		p.stats = t.tablePlan.StatsInfo()
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
	return t.p.StatsInfo().count
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
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		// If the table/index scans data by order and applies a double read, the limit cannot be pushed to the table side.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
			// When limit be pushed down, it should remove its offset.
			pushedDownLimit := PhysicalLimit{Count: p.Offset + p.Count}.init(p.ctx, p.stats)
			if cop.tablePlan != nil {
				pushedDownLimit.SetSchema(cop.tablePlan.Schema())
			} else {
				pushedDownLimit.SetSchema(cop.indexPlan.Schema())
			}
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		}
		t = finishCopTask(cop, p.ctx)
	}
	if !p.partial {
		t = attachPlan2Task(p, t)
	}
	return t
}

func (p *PhysicalSort) getCost(count float64) float64 {
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
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	t = attachPlan2Task(p, t)
	t.addCost(p.getCost(t.count()))
	return t
}

func (p *NominalSort) attach2Task(tasks ...task) task {
	return tasks[0]
}

func (p *PhysicalTopN) getPushedDownTopN() *PhysicalTopN {
	newByItems := make([]*ByItems, 0, len(p.ByItems))
	for _, expr := range p.ByItems {
		newByItems = append(newByItems, expr.Clone())
	}
	topN := PhysicalTopN{
		ByItems: newByItems,
		Count:   p.Offset + p.Count,
	}.init(p.ctx, p.stats)
	return topN
}

func (p *PhysicalTopN) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	// This is a topN plan.
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
		pushedDownTopN := p.getPushedDownTopN()
		// If all columns in topN are from index plan, we can push it to index plan. Or we finish the index plan and
		// push it to table plan.
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN.SetSchema(copTask.indexPlan.Schema())
			pushedDownTopN.SetChildren(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
		} else {
			// FIXME: When we pushed down a top-N plan to table plan branch in case of double reading. The cost should
			// be more expensive in case of single reading, because we may execute table scan multi times.
			copTask.finishIndexPlan()
			pushedDownTopN.SetSchema(copTask.tablePlan.Schema())
			pushedDownTopN.SetChildren(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
		}
		copTask.addCost(pushedDownTopN.getCost(t.count()))
	}
	t = finishCopTask(t, p.ctx)
	if !p.partial {
		t = attachPlan2Task(p, t)
		t.addCost(p.getCost(t.count()))
	}
	return t
}

func (p *PhysicalProjection) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	switch tp := t.(type) {
	case *copTask:
		// TODO: Support projection push down.
		t = finishCopTask(t, p.ctx)
		t = attachPlan2Task(p, t)
		return t
	case *rootTask:
		return attachPlan2Task(p, tp)
	}
	return nil
}

func (p *PhysicalUnionAll) attach2Task(tasks ...task) task {
	newTask := &rootTask{p: p}
	newChildren := make([]Plan, 0, len(p.children))
	for _, task := range tasks {
		if task.invalid() {
			return invalidTask
		}
		task = finishCopTask(task, p.ctx)
		newTask.cst += task.cost()
		newChildren = append(newChildren, task.plan())
	}
	p.SetChildren(newChildren...)
	return newTask
}

func (sel *PhysicalSelection) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := finishCopTask(tasks[0].copy(), sel.ctx)
	t.addCost(t.count() * cpuFactor)
	t = attachPlan2Task(sel, t)
	return t
}

func (p *PhysicalHashAgg) newPartialAggregate() (partialAgg, finalAgg *PhysicalHashAgg) {
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return nil, p
		}
	}
	_, _, remained := expression.ExpressionsToPB(sc, p.GroupByItems, client)
	if len(remained) > 0 {
		return nil, p
	}
	partialAgg = p
	originalSchema := p.schema
	// TODO: Refactor the way of constructing aggregation functions.
	partialSchema := expression.NewSchema()
	partialAgg.SetSchema(partialSchema)
	cursor := 0
	finalAggFuncs := make([]aggregation.Aggregation, len(p.AggFuncs))
	for i, aggFun := range p.AggFuncs {
		fun := aggregation.NewAggFunction(aggFun.GetName(), nil, false)
		var args []expression.Expression
		colName := model.NewCIStr(fmt.Sprintf("col_%d", cursor))
		if needCount(fun) {
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			partialSchema.Append(&expression.Column{FromID: partialAgg.id, Position: cursor, ColName: colName, RetType: ft})
			args = append(args, partialSchema.Columns[cursor].Clone())
			cursor++
		}
		if needValue(fun) {
			ft := originalSchema.Columns[i].GetType()
			partialSchema.Append(&expression.Column{FromID: partialAgg.id, Position: cursor, ColName: colName, RetType: ft})
			args = append(args, partialSchema.Columns[cursor].Clone())
			cursor++
		}
		fun.SetArgs(args)
		fun.SetMode(aggregation.FinalMode)
		finalAggFuncs[i] = fun
	}
	finalAgg = basePhysicalAgg{
		AggFuncs: finalAggFuncs,
	}.initForHash(p.ctx, p.stats)
	finalAgg.SetSchema(originalSchema)
	// add group by columns
	for i, gbyExpr := range p.GroupByItems {
		gbyCol := &expression.Column{
			FromID:   partialAgg.id,
			Position: cursor + i,
			RetType:  gbyExpr.GetType(),
		}
		partialSchema.Append(gbyCol)
		finalAgg.GroupByItems = append(finalAgg.GroupByItems, gbyCol.Clone())
	}
	return
}

func (p *PhysicalStreamAgg) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	attachPlan2Task(p, t)
	t.addCost(t.count() * cpuFactor)
	return t
}

func (p *PhysicalHashAgg) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	cardinality := p.StatsInfo().count
	task := tasks[0].copy()
	if cop, ok := task.(*copTask); ok {
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
		task = finishCopTask(cop, p.ctx)
		attachPlan2Task(finalAgg, task)
		task.addCost(task.count()*cpuFactor + cardinality*hashAggMemFactor)
	} else {
		attachPlan2Task(p, task)
		task.addCost(task.count()*cpuFactor + cardinality*hashAggMemFactor)
	}
	return task
}
