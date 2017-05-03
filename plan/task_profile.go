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

// taskProfile is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type taskProfile interface {
	setCount(cnt float64)
	count() float64
	addCost(cost float64)
	cost() float64
	copy() taskProfile
	plan() PhysicalPlan
}

// TODO: In future, we should split copTask to indexTask and tableTask.
// copTaskProfile is a profile for a task running a distributed kv store.
type copTaskProfile struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	cnt       float64
	// indexPlanFinished means we have finished index plan.
	indexPlanFinished bool
}

func (t *copTaskProfile) setCount(cnt float64) {
	t.cnt = cnt
}

func (t *copTaskProfile) count() float64 {
	return t.cnt
}

func (t *copTaskProfile) addCost(cst float64) {
	t.cst += cst
}

func (t *copTaskProfile) cost() float64 {
	return t.cst
}

func (t *copTaskProfile) copy() taskProfile {
	nt := *t
	return &nt
}

func (t *copTaskProfile) plan() PhysicalPlan {
	if t.indexPlanFinished {
		return t.tablePlan
	}
	return t.indexPlan
}

func attachPlan2TaskProfile(p PhysicalPlan, t taskProfile) taskProfile {
	switch v := t.(type) {
	case *copTaskProfile:
		if v.indexPlanFinished {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		} else {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		}
	case *rootTaskProfile:
		p.SetChildren(v.p)
		v.p = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTaskProfile) finishIndexPlan() {
	if !t.indexPlanFinished {
		if t.tablePlan != nil {
			t.indexPlan.SetSchema(expression.NewSchema()) // we only need the handle
		}
		t.cst += t.cnt * (netWorkFactor + scanFactor)
		t.indexPlanFinished = true
	}
}

func (p *basePhysicalPlan) attach2TaskProfile(tasks ...taskProfile) taskProfile {
	profile := finishCopTask(tasks[0].copy(), p.basePlan.ctx, p.basePlan.allocator)
	return attachPlan2TaskProfile(p.basePlan.self.(PhysicalPlan).Copy(), profile)
}

func (p *PhysicalHashJoin) attach2TaskProfile(tasks ...taskProfile) taskProfile {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTaskProfile{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
		cnt: lTask.count() + rTask.count(),
	}
}

func (p *PhysicalMergeJoin) attach2TaskProfile(tasks ...taskProfile) taskProfile {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	return &rootTaskProfile{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
		cnt: lTask.count() + rTask.count(),
	}
}

func (p *PhysicalHashSemiJoin) attach2TaskProfile(tasks ...taskProfile) taskProfile {
	lTask := finishCopTask(tasks[0].copy(), p.ctx, p.allocator)
	rTask := finishCopTask(tasks[1].copy(), p.ctx, p.allocator)
	np := p.Copy()
	np.SetChildren(lTask.plan(), rTask.plan())
	task := &rootTaskProfile{
		p: np,
		// TODO: we will estimate the cost and count more precisely.
		cst: lTask.cost() + rTask.cost(),
	}
	if p.WithAux {
		task.cnt = lTask.count()
	} else {
		task.cnt = lTask.count() * selectionFactor
	}
	return task
}

// finishCopTask means we close the coprocessor task and create a root task.
func finishCopTask(task taskProfile, ctx context.Context, allocator *idAllocator) taskProfile {
	t, ok := task.(*copTaskProfile)
	if !ok {
		return task
	}
	// FIXME: When it is a double reading. The cost should be more expensive. The right cost should add the
	// `NetWorkStartCost` * (totalCount / perCountIndexRead)
	t.finishIndexPlan()
	if t.tablePlan != nil {
		t.cst += t.cnt * netWorkFactor
	}
	newTask := &rootTaskProfile{
		cst: t.cst,
		cnt: t.cnt,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		newTask.p = PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.init(allocator, ctx)
		newTask.p.SetSchema(t.tablePlan.Schema())
	} else if t.indexPlan != nil {
		newTask.p = PhysicalIndexReader{indexPlan: t.indexPlan}.init(allocator, ctx)
	} else {
		newTask.p = PhysicalTableReader{tablePlan: t.tablePlan}.init(allocator, ctx)
	}
	return newTask
}

// rootTaskProfile is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTaskProfile struct {
	p   PhysicalPlan
	cst float64
	cnt float64
}

func (t *rootTaskProfile) copy() taskProfile {
	return &rootTaskProfile{
		p:   t.p,
		cst: t.cst,
		cnt: t.cnt,
	}
}

func (t *rootTaskProfile) setCount(cnt float64) {
	t.cnt = cnt
}

func (t *rootTaskProfile) count() float64 {
	return t.cnt
}

func (t *rootTaskProfile) addCost(cst float64) {
	t.cst += cst
}

func (t *rootTaskProfile) cost() float64 {
	return t.cst
}

func (t *rootTaskProfile) plan() PhysicalPlan {
	return t.p
}

func (p *Limit) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	if cop, ok := profile.(*copTaskProfile); ok {
		// If the task is copTask, the Limit can always be pushed down.
		// When limit be pushed down, it should remove its offset.
		pushedDownLimit := Limit{Count: p.Offset + p.Count}.init(p.allocator, p.ctx)
		if cop.tablePlan != nil {
			pushedDownLimit.SetSchema(cop.tablePlan.Schema())
		} else {
			pushedDownLimit.SetSchema(cop.indexPlan.Schema())
		}
		cop = attachPlan2TaskProfile(pushedDownLimit, cop).(*copTaskProfile)
		cop.setCount(float64(pushedDownLimit.Count))
		profile = finishCopTask(cop, p.ctx, p.allocator)
	}
	profile = attachPlan2TaskProfile(p.Copy(), profile)
	profile.setCount(float64(p.Count))
	return profile
}

func (p *Sort) getCost(count float64) float64 {
	if p.ExecLimit == nil {
		return count*cpuFactor + count*memoryFactor
	}
	return count*cpuFactor + float64(p.ExecLimit.Count)*memoryFactor
}

// canPushDown check if this topN can be pushed down. If each of the expression can be converted to pb, it can be pushed.
func (p *Sort) canPushDown() bool {
	if p.ExecLimit == nil {
		return false
	}
	exprs := make([]expression.Expression, 0, len(p.ByItems))
	for _, item := range p.ByItems {
		exprs = append(exprs, item.Expr)
	}
	_, _, remained := expression.ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
	return len(remained) == 0
}

func (p *Sort) allColsFromSchema(schema *expression.Schema) bool {
	var cols []*expression.Column
	for _, item := range p.ByItems {
		cols = append(cols, expression.ExtractColumns(item.Expr)...)
	}
	return len(schema.ColumnsIndices(cols)) > 0
}

func (p *Sort) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	// If this is a Sort , we cannot push it down.
	if p.ExecLimit == nil {
		profile = finishCopTask(profile, p.ctx, p.allocator)
		profile = attachPlan2TaskProfile(p.Copy(), profile)
		profile.addCost(p.getCost(profile.count()))
		return profile
	}
	// This is a topN plan.
	if copTask, ok := profile.(*copTaskProfile); ok && p.canPushDown() {
		limit := p.ExecLimit
		pushedDownTopN := p.Copy().(*Sort)
		// When topN is pushed down, it should remove its offset.
		pushedDownTopN.ExecLimit = &Limit{Count: limit.Count + limit.Offset}
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
		copTask.addCost(pushedDownTopN.getCost(profile.count()))
		copTask.setCount(float64(pushedDownTopN.ExecLimit.Count))
	}
	profile = finishCopTask(profile, p.ctx, p.allocator)
	profile = attachPlan2TaskProfile(p.Copy(), profile)
	profile.addCost(p.getCost(profile.count()))
	profile.setCount(float64(p.ExecLimit.Count))
	return profile
}

func (p *Projection) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	np := p.Copy()
	switch t := profile.(type) {
	case *copTaskProfile:
		// TODO: Support projection push down.
		task := finishCopTask(profile, p.ctx, p.allocator)
		profile = attachPlan2TaskProfile(np, task)
		return profile
	case *rootTaskProfile:
		return attachPlan2TaskProfile(np, t)
	}
	return nil
}

func (p *Union) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	np := p.Copy()
	newTask := &rootTaskProfile{p: np}
	newChildren := make([]Plan, 0, len(p.children))
	for _, profile := range profiles {
		profile = finishCopTask(profile, p.ctx, p.allocator)
		newTask.cst += profile.cost()
		newTask.cnt += profile.count()
		newChildren = append(newChildren, profile.plan())
	}
	np.SetChildren(newChildren...)
	return newTask
}

func (sel *Selection) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := finishCopTask(profiles[0].copy(), sel.ctx, sel.allocator)
	profile.addCost(profile.count() * cpuFactor)
	profile.setCount(profile.count() * selectionFactor)
	profile = attachPlan2TaskProfile(sel.Copy(), profile)
	return profile
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
	partialSchema := expression.NewSchema(&expression.Column{RetType: gkType, FromID: p.id, Index: 0})
	partialAgg.SetSchema(partialSchema)
	cursor := 0
	finalAggFuncs := make([]expression.AggregationFunction, len(finalAgg.AggFuncs))
	for i, aggFun := range p.AggFuncs {
		fun := expression.NewAggFunction(aggFun.GetName(), nil, false)
		var args []expression.Expression
		colName := model.NewCIStr(fmt.Sprintf("col_%d", cursor+1))
		if needCount(fun) {
			cursor++
			ft := types.NewFieldType(mysql.TypeLonglong)
			ft.Flen = 21
			ft.Charset = charset.CharsetBin
			ft.Collate = charset.CollationBin
			partialSchema.Append(&expression.Column{Index: cursor, ColName: colName, RetType: ft})
			args = append(args, partialSchema.Columns[cursor].Clone())
		}
		if needValue(fun) {
			cursor++
			ft := p.schema.Columns[i].GetType()
			partialSchema.Append(&expression.Column{Index: cursor, ColName: colName, RetType: ft})
			args = append(args, partialSchema.Columns[cursor].Clone())
		}
		fun.SetArgs(args)
		fun.SetMode(expression.FinalMode)
		finalAggFuncs[i] = fun
	}
	finalAgg = PhysicalAggregation{
		HasGby:       p.HasGby,
		AggType:      FinalAgg,
		GroupByItems: []expression.Expression{partialSchema.Columns[0].Clone()},
		AggFuncs:     finalAggFuncs,
	}.init(p.allocator, p.ctx)
	finalAgg.SetSchema(p.schema)
	return
}

func (p *PhysicalAggregation) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	// TODO: We only consider hash aggregation here.
	profile := profiles[0].copy()
	if cop, ok := profile.(*copTaskProfile); ok {
		partialAgg, finalAgg := p.newPartialAggregate()
		if partialAgg != nil {
			if cop.tablePlan != nil {
				cop.finishIndexPlan()
				partialAgg.SetChildren(cop.tablePlan)
				cop.tablePlan = partialAgg
				cop.cst += cop.cnt * cpuFactor
				cop.cnt = cop.cnt * aggFactor
			} else {
				partialAgg.SetChildren(cop.indexPlan)
				cop.indexPlan = partialAgg
				cop.cst += cop.cnt * cpuFactor
				cop.cnt = cop.cnt * aggFactor
			}
		}
		profile = finishCopTask(cop, p.ctx, p.allocator)
		attachPlan2TaskProfile(finalAgg, profile)
	} else {
		np := p.Copy()
		attachPlan2TaskProfile(np, profile)
		profile.addCost(profile.count() * cpuFactor)
		profile.setCount(profile.count() * aggFactor)
	}
	return profile
}
