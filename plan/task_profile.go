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
		t.cst += float64(t.cnt) * netWorkFactor
		t.addPlan2Index = false
	}
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
	} else if t.indexPlan != nil {
		newTask.plan = PhysicalIndexReader{copPlan: t.indexPlan}.init(allocator, ctx)
	} else {
		newTask.plan = PhysicalTableReader{copPlan: t.tablePlan}.init(allocator, ctx)
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
		cop = attachPlan2TaskProfile(pushedDownLimit, cop).(*copTaskProfile)
		cop.setCount(pushedDownLimit.Count)
		profile = cop.finishTask(p.ctx, p.allocator)
	}
	profile = attachPlan2TaskProfile(p.Copy(), profile)
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
		switch t := profile.(type) {
		case *copTaskProfile:
			profile = t.finishTask(p.ctx, p.allocator)
		}
		profile = attachPlan2TaskProfile(p.Copy(), profile)
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
		} else {
			copTask.finishIndexPlan()
			pushedDownTopN.SetChildren(copTask.tablePlan)
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
	switch t := profile.(type) {
	case *copTaskProfile:
		// TODO: Support projection push down.
		task := t.finishTask(p.ctx, p.allocator)
		return attachPlan2TaskProfile(p.Copy(), task)
	case *rootTaskProfile:
		return attachPlan2TaskProfile(p.Copy(), t)
	}
	return nil
}

func (sel *Selection) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	switch t := profile.(type) {
	case *copTaskProfile:
		if t.addPlan2Index {
			var indexSel, tableSel *Selection
			if t.tablePlan != nil {
				indexSel, tableSel = sel.splitSelectionByIndexColumns(t.indexPlan.Schema())
			} else {
				indexSel = sel.Copy().(*Selection)
			}
			if indexSel != nil {
				indexSel.SetChildren(t.indexPlan)
				t.indexPlan = indexSel
				// TODO: Update t.cnt.
				t.cst += float64(t.cnt) * cpuFactor
			}
			if tableSel != nil {
				t.finishIndexPlan()
				tableSel.SetChildren(t.tablePlan)
				t.tablePlan = tableSel
				t.cst += float64(t.cnt) * cpuFactor
			}
		} else {
			sel.SetChildren(t.tablePlan)
			t.tablePlan = sel.Copy()
			t.cst += float64(t.cnt) * cpuFactor
		}
		// TODO: Estimate t.cnt by histogram.
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
	case *rootTaskProfile:
		t.cst += float64(t.cnt) * cpuFactor
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
		profile = attachPlan2TaskProfile(sel.Copy(), t)
	}
	return profile
}

// splitSelectionByIndexColumns splits the selection conditions by index schema. If some condition only contain the index
// columns, it will be pushed to index plan.
func (sel *Selection) splitSelectionByIndexColumns(schema *expression.Schema) (indexSel *Selection, tableSel *Selection) {
	var tableConds []expression.Expression
	var indexConds []expression.Expression
	for _, cond := range sel.Conditions {
		cols := expression.ExtractColumns(cond)
		indices := schema.ColumnsIndices(cols)
		if indices == nil {
			tableConds = append(tableConds, cond)
		} else {
			indexConds = append(indexConds, cond)
		}
	}
	if len(indexConds) != 0 {
		indexSel = Selection{Conditions: indexConds}.init(sel.allocator, sel.ctx)
	}
	if len(tableConds) != 0 {
		tableSel = Selection{Conditions: tableConds}.init(sel.allocator, sel.ctx)
	}
	return
}
