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

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
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

// TODO: In future, we should split copTask to indexTask and tableTask.
// copTask is a task that runs in a distributed kv store.
type copTask struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
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
		t.cst += t.count() * (netWorkFactor + scanFactor)
		t.indexPlanFinished = true
		if t.tablePlan != nil {
			t.tablePlan.(*PhysicalTableScan).profile = t.indexPlan.statsProfile()
		}
	}
}

func (p *basePhysicalPlan) attach2Task(tasks ...task) task {
	task := finishCopTask(tasks[0].copy(), p.basePlan.ctx, p.basePlan.allocator)
	return attachPlan2Task(p.basePlan.self.(PhysicalPlan).Copy(), task)
}

func (p *PhysicalApply) attach2Task(tasks ...task) task {
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
	lTask := finishCopTask(tasks[p.outerIndex].copy(), p.ctx, p.allocator)
	innerTask := tasks[1-p.outerIndex]
	if innerTask.invalid() {
		return invalidTask
	}
	rTask := finishCopTask(innerTask.copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost(),
	}
}

func (p *PhysicalHashJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
	}
}

func (p *PhysicalMergeJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTask{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
	}
}

func (p *PhysicalHashSemiJoin) attach2Task(tasks ...task) task {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTask{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
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
	if tasks[0].plan() == nil {
		return tasks[0]
	}
	task := tasks[0].copy()
	if cop, ok := task.(*copTask); ok {
		// If the task is copTask, the Limit can always be pushed down.
		// When limit be pushed down, it should remove its offset.
		pushedDownLimit := Limit{Count: p.Offset + p.Count}.init(p.allocator, p.ctx)
		pushedDownLimit.profile = p.profile
		if cop.tablePlan != nil {
			pushedDownLimit.SetSchema(cop.tablePlan.Schema())
		} else {
			pushedDownLimit.SetSchema(cop.indexPlan.Schema())
		}
		cop = attachPlan2Task(pushedDownLimit, cop).(*copTask)
		task = finishCopTask(cop, p.ctx, p.allocator)
	}
	task = attachPlan2Task(p.Copy(), task)
	return task
}

func (p *Sort) getCost(count float64) float64 {
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
	task := tasks[0].copy()
	task = finishCopTask(task, p.ctx, p.allocator)
	task = attachPlan2Task(p.Copy(), task)
	task.addCost(p.getCost(task.count()))
	return task
}

func (p *TopN) attach2Task(tasks ...task) task {
	// If task is invalid, keep it remained.
	if tasks[0].plan() == nil {
		return tasks[0]
	}
	task := tasks[0].copy()
	// This is a topN plan.
	if copTask, ok := task.(*copTask); ok && p.canPushDown() {
		pushedDownTopN := p.Copy().(*TopN)
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
		copTask.addCost(pushedDownTopN.getCost(task.count()))
	}
	task = finishCopTask(task, p.ctx, p.allocator)
	task = attachPlan2Task(p.Copy(), task)
	task.addCost(p.getCost(task.count()))
	return task
}

func (p *Projection) attach2Task(tasks ...task) task {
	task := tasks[0].copy()
	np := p.Copy()
	switch t := task.(type) {
	case *copTask:
		// TODO: Support projection push down.
		task := finishCopTask(task, p.ctx, p.allocator)
		task = attachPlan2Task(np, task)
		return task
	case *rootTask:
		return attachPlan2Task(np, t)
	}
	return nil
}

func (p *Union) attach2Task(tasks ...task) task {
	np := p.Copy()
	newTask := &rootTask{p: np}
	newChildren := make([]Plan, 0, len(p.children))
	for _, task := range tasks {
		task = finishCopTask(task, p.ctx, p.allocator)
		newTask.cst += task.cost()
		newChildren = append(newChildren, task.plan())
	}
	np.SetChildren(newChildren...)
	return newTask
}

func (sel *Selection) attach2Task(tasks ...task) task {
	task := finishCopTask(tasks[0].copy(), sel.ctx, sel.allocator)
	task.addCost(task.count() * cpuFactor)
	task = attachPlan2Task(sel.Copy(), task)
	return task
}

func (p *PhysicalAggregation) newPartialAggregate() (partialAgg, finalAgg *PhysicalAggregation) {
	finalAgg = p.Copy().(*PhysicalAggregation)
	// Check if this aggregation can push down.
	sc := p.ctx.GetSessionVars().StmtCtx
	client := p.ctx.GetClient()
	for _, aggFunc := range p.AggFuncs {
		pb := expression.AggFuncToPBExpr(sc, client, aggFunc)
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
	finalAggFuncs := make([]expression.AggregationFunction, len(finalAgg.AggFuncs))
	for i, aggFun := range p.AggFuncs {
		fun := expression.NewAggFunction(aggFun.GetName(), nil, false)
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
		fun.SetMode(expression.FinalMode)
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
	if tasks[0].plan() == nil {
		return tasks[0]
	}
	// TODO: We only consider hash aggregation here.
	task := tasks[0].copy()
	if cop, ok := task.(*copTask); ok {
		partialAgg, finalAgg := p.newPartialAggregate()
		if partialAgg != nil {
			if cop.tablePlan != nil {
				cop.finishIndexPlan()
				partialAgg.SetChildren(cop.tablePlan)
				cop.tablePlan = partialAgg
				cop.cst += cop.count() * cpuFactor
			} else {
				partialAgg.SetChildren(cop.indexPlan)
				cop.indexPlan = partialAgg
				cop.cst += cop.count() * cpuFactor
			}
		}
		task = finishCopTask(cop, p.ctx, p.allocator)
		attachPlan2Task(finalAgg, task)
	} else {
		np := p.Copy()
		attachPlan2Task(np, task)
		task.addCost(task.count() * cpuFactor)
	}
	return task
}
