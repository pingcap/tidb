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

import "github.com/pingcap/tidb/expression"

// taskProfile is a new version of `PhysicalPlanInfo`. It stores cost information for a task.
// A task may be CopTask, RootTask, MPPTask or a ParallelTask.
type taskProfile interface {
	setCount(cnt uint64)
	count() uint64
	setCost(cost float64)
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

func (t *copTaskProfile) setCost(cst float64) {
	t.cst = cst
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

func attachPlan2Task(p PhysicalPlan, t taskProfile) taskProfile {
	nt := t.copy()
	switch v := nt.(type) {
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
	return nt
}

func (t *copTaskProfile) finishIndexPlan() {
	t.cst += float64(t.cnt) * netWorkFactor
	t.addPlan2Index = false
}

func (t *copTaskProfile) finishTask() {
	if t.addPlan2Index {
		t.finishIndexPlan()
	}
	if t.tablePlan != nil {
		t.cst += float64(t.cnt) * netWorkFactor
	}
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

func (t *rootTaskProfile) setCost(cst float64) {
	t.cst = cst
}

func (t *rootTaskProfile) cost() float64 {
	return t.cst
}

func (limit *Limit) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := attachPlan2Task(limit, profiles[0])
	profile.setCount(limit.Count)
	return profile
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
				indexSel = sel
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
			t.tablePlan = sel
			t.cst += float64(t.cnt) * cpuFactor
		}
		// TODO: Estimate t.cnt by histogram.
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
	case *rootTaskProfile:
		t.cst += float64(t.cnt) * cpuFactor
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
	}
	return profile
}

// splitSelectionByIndexColumns splits the selection conditions by index schema. If some condition only contain the index
// columns, it will be pushed to index plan.
func (sel *Selection) splitSelectionByIndexColumns(schema *expression.Schema) (indexSel *Selection, tableSel *Selection) {
	conditions := sel.Conditions
	var tableConds []expression.Expression
	for i := len(conditions) - 1; i >= 0; i-- {
		cols := expression.ExtractColumns(conditions[i])
		indices := schema.ColumnsIndices(cols)
		if indices == nil {
			tableConds = append(tableConds, conditions[i])
			conditions = append(conditions[:i], conditions[i+1:]...)
		}
	}
	if len(conditions) != 0 {
		indexSel = sel
	}
	if len(tableConds) != 0 {
		tableSel = Selection{Conditions: tableConds}.init(sel.allocator, sel.ctx)
	}
	return
}
