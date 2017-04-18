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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
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
	return attachPlan2TaskProfile(p.basePlan.self.(PhysicalPlan).Copy(), tasks[0])
}

// finishTask means we close the coprocessor task and create a root task.
func (t *copTaskProfile) finishTask(ctx context.Context, allocator *idAllocator) taskProfile {
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
		newTask.p = PhysicalIndexReader{copPlan: t.indexPlan}.init(allocator, ctx)
		newTask.p.SetSchema(t.indexPlan.Schema())
	} else {
		newTask.p = PhysicalTableReader{copPlan: t.tablePlan}.init(allocator, ctx)
		newTask.p.SetSchema(t.tablePlan.Schema())
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
		profile = cop.finishTask(p.ctx, p.allocator)
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
	_, _, remained := ExpressionsToPB(p.ctx.GetSessionVars().StmtCtx, exprs, p.ctx.GetClient())
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
		if cop, ok := profile.(*copTaskProfile); ok {
			profile = cop.finishTask(p.ctx, p.allocator)
		}
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
		profile = copTask.finishTask(p.ctx, p.allocator)
	} else if ok {
		profile = copTask.finishTask(p.ctx, p.allocator)
	}
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
		task := t.finishTask(p.ctx, p.allocator)
		profile = attachPlan2TaskProfile(np, task)
		return profile
	case *rootTaskProfile:
		return attachPlan2TaskProfile(np, t)
	}
	return nil
}

func (sel *Selection) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	switch t := profile.(type) {
	case *copTaskProfile:
		var indexConds, tableConds []expression.Expression
		conds := sel.pushDownConditions
		if t.indexPlanFinished {
			if ts, ok := t.tablePlan.(*PhysicalTableScan); ok {
				conds = ts.filterCondition
			}
		} else if is, ok := t.indexPlan.(*PhysicalIndexScan); ok {
			conds = is.filterCondition
		}
		if t.indexPlanFinished {
			tableConds = conds
		} else if t.tablePlan != nil {
			// This is the case of double read.
			tableConds, indexConds = splitConditionsByIndexColumns(conds, t.indexPlan.Schema())
		} else {
			// Index single read.
			indexConds = conds
		}
		if len(indexConds) > 0 {
			indexSel := Selection{Conditions: indexConds}.init(sel.allocator, sel.ctx)
			indexSel.SetChildren(t.indexPlan)
			indexSel.SetSchema(t.indexPlan.Schema())
			t.indexPlan = indexSel
			t.cst += t.cnt * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = t.cnt * selectionFactor
		}
		if len(tableConds) > 0 {
			t.finishIndexPlan()
			tableSel := Selection{Conditions: tableConds}.init(sel.allocator, sel.ctx)
			tableSel.SetChildren(t.tablePlan)
			tableSel.SetSchema(t.tablePlan.Schema())
			t.tablePlan = tableSel
			t.cst += t.cnt * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = t.cnt * selectionFactor
		}
		if len(sel.residualConditions) > 0 {
			profile = t.finishTask(sel.ctx, sel.allocator)
			rootSel := Selection{Conditions: sel.residualConditions}.init(sel.allocator, sel.ctx)
			rootSel.SetSchema(profile.plan().Schema())
			profile = attachPlan2TaskProfile(rootSel, profile)
			t.cst += t.cnt * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = t.cnt * selectionFactor
		}
	case *rootTaskProfile:
		t.cst += t.cnt * cpuFactor
		t.cnt = t.cnt * selectionFactor
		profile = attachPlan2TaskProfile(sel.Copy(), t)
	}
	return profile
}

// splitPushDownConditions choose the conditions to push down and the conditions to remain.
func (sel *Selection) splitPushDownConditions() {
	if len(sel.pushDownConditions)+len(sel.residualConditions) != 0 {
		return
	}
	_, sel.pushDownConditions, sel.residualConditions = ExpressionsToPB(sel.ctx.GetSessionVars().StmtCtx, sel.Conditions, sel.ctx.GetClient())
	if ds, ok := sel.children[0].(*DataSource); ok {
		ds.pushedDownConds = sel.pushDownConditions
	}
}

// splitConditionsByIndexColumns splits the conditions by index schema. If some condition only contain the index
// columns, it will be pushed to index plan.
func splitConditionsByIndexColumns(conditions []expression.Expression, schema *expression.Schema) (tableConds []expression.Expression, indexConds []expression.Expression) {
	for _, cond := range conditions {
		cols := expression.ExtractColumns(cond)
		indices := schema.ColumnsIndices(cols)
		if len(indices) == 0 {
			tableConds = append(tableConds, cond)
		} else {
			indexConds = append(indexConds, cond)
		}
	}
	return
}
