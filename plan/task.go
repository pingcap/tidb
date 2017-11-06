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
		return t.tablePlan.statsProfile().count
	}
	return t.indexPlan.statsProfile().count
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
			t.tablePlan.(*PhysicalTableScan).profile = t.indexPlan.statsProfile()
			t.cst += t.count() * scanFactor
		}
	}
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := finishCopTask(tasks[0].copy(), p.basePlan.ctx, p.basePlan.allocator)
	return attachPlan2Task(p.basePlan.self.(PhysicalPlan).Copy(), t)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
	if tasks[0].invalid() || tasks[1].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy().(*PhysicalApply)
	np.SetChildren(lTask.plan(), rTask.plan())
	np.PhysicalJoin.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   np,
		cst: lTask.cost() + lTask.count()*rTask.cost(),
	}
}

func (p *PhysicalIndexJoin) attach2Task(tasks ...task) task {
	if tasks[p.outerIndex].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[p.outerIndex].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), p.innerPlan)
	return &rootTask{
		p:   np,
		cst: lTask.cost() + p.getCost(lTask.count()),
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
	if p.SmallTable == 1 {
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
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   np,
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
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p:   np,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
}

func (p *PhysicalHashSemiJoin) getCost(lCnt, rCnt float64) float64 {
	if rCnt <= 1 {
		rCnt = 1
	}
	return (lCnt + rCnt) * (1 + math.Log2(rCnt))
}

func (p *PhysicalHashSemiJoin) attach2Task(tasks ...task) task {
	if tasks[0].invalid() || tasks[1].invalid() {
		return invalidTask
	}
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTask{
		p:   np,
		cst: lTask.cost() + rTask.cost() + p.getCost(lTask.count(), rTask.count()),
	}
	return task
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(task task, ctx context.Context, allocator *idAllocator) task {
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
		p := PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.init(allocator, ctx)
		p.profile = t.tablePlan.statsProfile()
		newTask.p = p
	} else if t.indexPlan != nil {
		p := PhysicalIndexReader{indexPlan: t.indexPlan}.init(allocator, ctx)
		p.profile = t.indexPlan.statsProfile()
		newTask.p = p
	} else {
		p := PhysicalTableReader{tablePlan: t.tablePlan}.init(allocator, ctx)
		p.profile = t.tablePlan.statsProfile()
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
	return t.p.statsProfile().count
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

func (p *Limit) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	if cop, ok := t.(*copTask); ok {
		// If the table/index scans data by order and applies a double read, the limit cannot be pushed to the table side.
		if !cop.keepOrder || !cop.indexPlanFinished || cop.indexPlan == nil {
			// When limit be pushed down, it should remove its offset.
			pushedDownLimit := Limit{Count: p.Offset + p.Count}.init(p.allocator, p.ctx)
			pushedDownLimit.profile = p.profile
			if cop.tablePlan != nil {
				pushedDownLimit.SetSchema(cop.tablePlan.Schema())
			} else {
				pushedDownLimit.SetSchema(cop.indexPlan.Schema())
			}
			cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		}
		t = finishCopTask(cop, p.ctx, p.allocator)
	}
	if !p.partial {
		t = attachPlan2Task(p.Copy(), t)
	}
	return t
}

func (p *Sort) getCost(count float64) float64 {
	if count < 2.0 {
		count = 2.0
	}
	return count*cpuFactor + count*memoryFactor
}

func (p *TopN) getCost(count float64) float64 {
	return count*cpuFactor + float64(p.Count)*memoryFactor
}

// canPushDown checks if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *TopN) canPushDown() bool {
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	_, _, remained := expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
	return len(remained) == 0
}

func (p *TopN) allColsFromSchema(schema *expression.Schema) bool {
	var cols []*expression.Column
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	return len(schema.ColumnsIndices(cols)) > 0
}

func (p *Sort) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	t = attachPlan2Task(p.Copy(), t)
	t.addCost(p.getCost(t.count()))
	return t
}

func (p *TopN) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	// This is a topN plan.
	if copTask, ok := t.(*copTask); ok && p.canPushDown() {
		pushedDownTopN := p.Copy().(*TopN)
		newByItems := make([]*ByItems, 0, len(p.ByItems))
		for _, expr := range p.ByItems {
			newByItems = append(newByItems, expr.Clone())
		}
		pushedDownTopN.ByItems = newByItems
		// When topN is pushed down, it should remove its offset.
		pushedDownTopN.Count, pushedDownTopN.Offset = p.Count+p.Offset, 0
		// If all columns in topN are from index plan, we can push it to index plan. Or we finish the index plan and
		// push it to table plan.
		if !copTask.indexPlanFinished && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN.SetChildren(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
			pushedDownTopN.SetSchema(copTask.indexPlan.Schema())
		} else {
			// FIXME: When we pushed down a top-N plan to table plan branch in case of double reading. The cost should
			// be more expensive in case of single reading, because we may execute table scan multi times.
			copTask.finishIndexPlan()
			pushedDownTopN.SetChildren(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
			pushedDownTopN.SetSchema(copTask.tablePlan.Schema())
		}
		copTask.addCost(pushedDownTopN.getCost(t.count()))
	}
	t = finishCopTask(t, p.ctx, p.allocator)
	if !p.partial {
		t = attachPlan2Task(p.Copy(), t)
		t.addCost(p.getCost(t.count()))
	}
	return t
}

func (p *Projection) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := tasks[0].copy()
	np := p.Copy()
	switch tp := t.(type) {
	case *copTask:
		// TODO: Support projection push down.
		t = finishCopTask(t, p.ctx, p.allocator)
		t = attachPlan2Task(np, t)
		return t
	case *rootTask:
		return attachPlan2Task(np, tp)
	}
	return nil
}

func (p *Union) attach2Task(tasks ...task) task {
	np := p.Copy()
	newTask := &rootTask{p: np}
	newChildren := make([]Plan, 0, len(p.children))
	for _, task := range tasks {
		if task.invalid() {
			return invalidTask
		}
		task = finishCopTask(task, p.ctx, p.allocator)
		newTask.cst += task.cost()
		newChildren = append(newChildren, task.plan())
	}
	np.SetChildren(newChildren...)
	return newTask
}

func (sel *Selection) attach2Task(tasks ...task) task {
	if tasks[0].invalid() {
		return invalidTask
	}
	t := finishCopTask(tasks[0].copy(), sel.ctx, sel.allocator)
	t.addCost(t.count() * cpuFactor)
	t = attachPlan2Task(sel.Copy(), t)
	return t
}

func (p *PhysicalAggregation) newPartialAggregate() (partialAgg, finalAgg *PhysicalAggregation) {
	finalAgg = p.Copy().(*PhysicalAggregation)
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		pb := aggregation.AggFuncToPBExpr(sc, client, aggFunc)
		if pb == nil {
			return
		}
	}
	_, _, remained := expression.ExpressionsToPB(sc, p.GroupByItems, client)
	if len(remained) > 0 {
		return
	}
	partialAgg = p.Copy().(*PhysicalAggregation)
	// TODO: It's toooooo ugly here. Refactor in the future !!
	gkType := types.NewFieldType(mysql.TypeBlob)
	gkType.Charset = charset.CharsetBin
	gkType.Collate = charset.CollationBin
	partialSchema := expression.NewSchema()
	partialAgg.SetSchema(partialSchema)
	cursor := 0
	finalAggFuncs := make([]aggregation.Aggregation, len(finalAgg.AggFuncs))
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
			ft := p.schema.Columns[i].GetType()
			partialSchema.Append(&expression.Column{FromID: partialAgg.id, Position: cursor, ColName: colName, RetType: ft})
			args = append(args, partialSchema.Columns[cursor].Clone())
			cursor++
		}
		fun.SetArgs(args)
		fun.SetMode(aggregation.FinalMode)
		finalAggFuncs[i] = fun
	}
	finalAgg = PhysicalAggregation{
		HasGby:   p.HasGby, // TODO: remove this field
		AggType:  FinalAgg,
		AggFuncs: finalAggFuncs,
	}.init(p.allocator, p.ctx)
	finalAgg.profile = p.profile
	finalAgg.SetSchema(p.schema)
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

func (p *PhysicalAggregation) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].invalid() {
		return invalidTask
	}
	cardinality := p.statsProfile().count
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
		task = finishCopTask(cop, p.ctx, p.allocator)
		task.addCost(task.count()*cpuFactor + cardinality*hashAggMemFactor)
		attachPlan2Task(finalAgg, task)
	} else {
		np := p.Copy()
		attachPlan2Task(np, task)
		if p.AggType == StreamedAgg {
			task.addCost(task.count() * cpuFactor)
		} else {
			task.addCost(task.count()*cpuFactor + cardinality*hashAggMemFactor)
		}
	}
	return task
}
