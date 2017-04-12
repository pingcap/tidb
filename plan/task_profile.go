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
	setCount(cnt uint64)
	count() uint64
	addCost(cost float64)
	cost() float64
	copy() taskProfile
}

// TODO: In future, we should split copTask to indexTask and tableTask.
// copTaskProfile is a profile for a task running a distributed kv store.
type copTaskProfile struct {
	indexPlan PhysicalPlan
	tablePlan PhysicalPlan
	cst       float64
	cnt       uint64
	// addPlan2Index means we are processing index plan.
	addPlan2Index bool
}

func (t *copTaskProfile) setCount(cnt uint64) {
	t.cnt = cnt
}

func (t *copTaskProfile) count() uint64 {
	return t.cnt
}

func (t *copTaskProfile) addCost(cst float64) {
	t.cst += cst
}

func (t *copTaskProfile) cost() float64 {
	return t.cst
}

func (t *copTaskProfile) copy() taskProfile {
	return &copTaskProfile{
		indexPlan:     t.indexPlan,
		tablePlan:     t.tablePlan,
		cst:           t.cst,
		cnt:           t.cnt,
		addPlan2Index: t.addPlan2Index,
	}
}

func attachPlan2TaskProfile(p PhysicalPlan, t taskProfile) taskProfile {
	switch v := t.(type) {
	case *copTaskProfile:
		if v.addPlan2Index {
			p.SetChildren(v.indexPlan)
			v.indexPlan = p
		} else {
			p.SetChildren(v.tablePlan)
			v.tablePlan = p
		}
	case *rootTaskProfile:
		p.SetChildren(v.plan)
		v.plan = p
	}
	return t
}

// finishIndexPlan means we no longer add plan to index plan, and compute the network cost for it.
func (t *copTaskProfile) finishIndexPlan() {
	if t.addPlan2Index {
		t.indexPlan.SetSchema(expression.NewSchema()) // we only need the handle
		t.cst += float64(t.cnt) * (netWorkFactor + scanFactor)
		t.addPlan2Index = false
	}
}

func (p *basePhysicalPlan) attach2TaskProfile(tasks ...taskProfile) taskProfile {
	return attachPlan2TaskProfile(p.basePlan.self.(PhysicalPlan).Copy(), tasks[0])
}

// finishTask means we close the coprocessor task and create a root task.
func (t *copTaskProfile) finishTask(ctx context.Context, allocator *idAllocator) taskProfile {
	if t.addPlan2Index {
		t.finishIndexPlan()
	}
	if t.tablePlan != nil {
		t.cst += float64(t.cnt) * netWorkFactor
	}
	newTask := &rootTaskProfile{
		cst: t.cst,
		cnt: t.cnt,
	}
	if t.indexPlan != nil && t.tablePlan != nil {
		newTask.plan = PhysicalIndexLookUpReader{tablePlan: t.tablePlan, indexPlan: t.indexPlan}.init(allocator, ctx)
		newTask.plan.SetSchema(t.tablePlan.Schema())
	} else if t.indexPlan != nil {
		newTask.plan = PhysicalIndexReader{copPlan: t.indexPlan}.init(allocator, ctx)
		newTask.plan.SetSchema(t.indexPlan.Schema())
	} else {
		newTask.plan = PhysicalTableReader{copPlan: t.tablePlan}.init(allocator, ctx)
		newTask.plan.SetSchema(t.tablePlan.Schema())
	}
	return newTask
}

// rootTaskProfile is the final sink node of a plan graph. It should be a single goroutine on tidb.
type rootTaskProfile struct {
	plan PhysicalPlan
	cst  float64
	cnt  uint64
}

func (t *rootTaskProfile) copy() taskProfile {
	return &rootTaskProfile{
		plan: t.plan,
		cst:  t.cst,
		cnt:  t.cnt,
	}
}

func (t *rootTaskProfile) setCount(cnt uint64) {
	t.cnt = cnt
}

func (t *rootTaskProfile) count() uint64 {
	return t.cnt
}

func (t *rootTaskProfile) addCost(cst float64) {
	t.cst += cst
}

func (t *rootTaskProfile) cost() float64 {
	return t.cst
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
		cop.setCount(pushedDownLimit.Count)
		profile = cop.finishTask(p.ctx, p.allocator)
	}
	np := p.Copy()
	np.SetSchema(profile.(*rootTaskProfile).plan.Schema())
	profile = attachPlan2TaskProfile(np, profile)
	profile.setCount(p.Count)
	return profile
}

func (p *Sort) getCost(count uint64) float64 {
	if p.ExecLimit == nil {
		return float64(count)*cpuFactor + float64(count)*memoryFactor
	}
	return float64(count)*cpuFactor + float64(p.ExecLimit.Count)*memoryFactor
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
		np := p.Copy()
		np.SetSchema(profile.(*rootTaskProfile).plan.Schema())
		profile = attachPlan2TaskProfile(np, profile)
		profile.addCost(p.getCost(profile.count()))
		return profile
	}
	// This is a topN.
	if copTask, ok := profile.(*copTaskProfile); ok && p.canPushDown() {
		limit := p.ExecLimit
		pushedDownTopN := p.Copy().(*Sort)
		// When topN is pushed down, it should remove its offset.
		pushedDownTopN.ExecLimit = &Limit{Count: limit.Count + limit.Offset}
		// If all columns in topN are from index plan, we can push it to index plan. Or we finish the index plan and
		// push it to table plan.
		if copTask.addPlan2Index && p.allColsFromSchema(copTask.indexPlan.Schema()) {
			pushedDownTopN.SetChildren(copTask.indexPlan)
			copTask.indexPlan = pushedDownTopN
			pushedDownTopN.SetSchema(copTask.indexPlan.Schema())
		} else {
			copTask.finishIndexPlan()
			pushedDownTopN.SetChildren(copTask.tablePlan)
			copTask.tablePlan = pushedDownTopN
			pushedDownTopN.SetSchema(copTask.tablePlan.Schema())
		}
		copTask.addCost(pushedDownTopN.getCost(profile.count()))
		copTask.setCount(pushedDownTopN.ExecLimit.Count)
		profile = copTask.finishTask(p.ctx, p.allocator)
	} else if ok {
		profile = copTask.finishTask(p.ctx, p.allocator)
	}
	profile = attachPlan2TaskProfile(p.Copy(), profile)
	profile.addCost(p.getCost(profile.count()))
	profile.setCount(p.ExecLimit.Count)
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
		if t.indexPlan != nil && t.addPlan2Index {
			if is, ok := t.indexPlan.(*PhysicalIndexScan); ok {
				conds = is.indexFilterConditions
			}
		}
		if t.tablePlan != nil && !t.addPlan2Index {
			if ts, ok := t.tablePlan.(*PhysicalTableScan); ok {
				conds = ts.tableFilterConditions
			}
		}
		if t.tablePlan != nil && t.addPlan2Index {
			// This is the case of double read.
			tableConds, indexConds = splitConditionsByIndexColumns(conds, t.indexPlan.Schema())
		} else if t.addPlan2Index {
			// Index single read.
			indexConds = conds
		} else {
			tableConds = conds
		}
		if len(indexConds) > 0 {
			indexSel := Selection{Conditions: indexConds}.init(sel.allocator, sel.ctx)
			indexSel.SetChildren(t.indexPlan)
			indexSel.SetSchema(t.indexPlan.Schema())
			t.indexPlan = indexSel
			t.cst += float64(t.cnt) * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = uint64(float64(t.cnt) * selectionFactor)
		}
		if len(tableConds) > 0 {
			t.finishIndexPlan()
			tableSel := Selection{Conditions: tableConds}.init(sel.allocator, sel.ctx)
			tableSel.SetChildren(t.tablePlan)
			tableSel.SetSchema(t.tablePlan.Schema())
			t.tablePlan = tableSel
			t.cst += float64(t.cnt) * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = uint64(float64(t.cnt) * selectionFactor)
		}
		if len(sel.residualConditions) > 0 {
			profile = t.finishTask(sel.ctx, sel.allocator)
			rootSel := Selection{Conditions: sel.residualConditions}.init(sel.allocator, sel.ctx)
			rootSel.SetSchema(profile.(*rootTaskProfile).plan.Schema())
			profile = attachPlan2TaskProfile(rootSel, profile)
			t.cst += float64(t.cnt) * cpuFactor
			// TODO: Estimate t.cnt by histogram.
			t.cnt = uint64(float64(t.cnt) * selectionFactor)
		}
	case *rootTaskProfile:
		t.cst += float64(t.cnt) * cpuFactor
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
		profile = attachPlan2TaskProfile(sel.Copy(), t)
	}
	return profile
}

func (sel *Selection) splitPushDownConditions() {
	if len(sel.pushDownConditions)+len(sel.residualConditions) != 0 {
		return
	}
	_, sel.pushDownConditions, sel.residualConditions = ExpressionsToPB(sel.ctx.GetSessionVars().StmtCtx, sel.Conditions, sel.ctx.GetClient())
}

// splitSelectionByIndexColumns splits the selection conditions by index schema. If some condition only contain the index
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
