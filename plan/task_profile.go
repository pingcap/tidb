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
// A task may be KVTask, SingletonTask, MPPTask or a ParallelTask.
type taskProfile interface {
	attachPlan(p PhysicalPlan) taskProfile
	setCount(cnt uint64)
	count() uint64
	setCost(cost float64)
	cost() float64
	copy() taskProfile
}

// TODO: In future, we should split kvTask to indexTask and tableTask.
// kvTaskProfile is a profile for a task running a distributed kv store.
type kvTaskProfile struct {
	indexPlan     PhysicalPlan
	tablePlan     PhysicalPlan
	cst           float64
	cnt           uint64
	addPlan2Index bool
}

func (t *kvTaskProfile) setCount(cnt uint64) {
	t.cnt = cnt
}

func (t *kvTaskProfile) count() uint64 {
	return t.cnt
}

func (t *kvTaskProfile) setCost(cst float64) {
	t.cst = cst
}

func (t *kvTaskProfile) cost() float64 {
	return t.cst
}

func (t *kvTaskProfile) copy() taskProfile {
	return &kvTaskProfile{
		indexPlan:     t.indexPlan,
		tablePlan:     t.tablePlan,
		cst:           t.cst,
		cnt:           t.cnt,
		addPlan2Index: t.addPlan2Index,
	}
}

func (t *kvTaskProfile) attachPlan(p PhysicalPlan) taskProfile {
	nt := t.copy().(*kvTaskProfile)
	if nt.addPlan2Index {
		p.SetChildren(nt.indexPlan)
		nt.indexPlan = p
	} else {
		p.SetChildren(nt.tablePlan)
		nt.tablePlan = p
	}
	return nt
}

func (t *kvTaskProfile) finishTask() {
	t.cst += float64(t.cnt) * netWorkFactor
	if t.tablePlan != nil && t.indexPlan != nil {
		t.cst += float64(t.cnt) * netWorkFactor
	}
}

// singletonTaskProfile is a profile running on tidb with single goroutine.
type singletonTaskProfile struct {
	plan PhysicalPlan
	cst  float64
	cnt  uint64
}

func (t *singletonTaskProfile) copy() taskProfile {
	return &singletonTaskProfile{
		plan: t.plan,
		cst:  t.cst,
		cnt:  t.cnt,
	}
}

func (t *singletonTaskProfile) attachPlan(p PhysicalPlan) taskProfile {
	nt := t.copy().(*singletonTaskProfile)
	p.SetChildren(nt.plan)
	nt.plan = p
	return nt
}

func (t *singletonTaskProfile) setCount(cnt uint64) {
	t.cnt = cnt
}

func (t *singletonTaskProfile) count() uint64 {
	return t.cnt
}

func (t *singletonTaskProfile) setCost(cst float64) {
	t.cst = cst
}

func (t *singletonTaskProfile) cost() float64 {
	return t.cst
}

func (limit *Limit) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].attachPlan(limit)
	profile.setCount(limit.Count)
	return profile
}

func (sel *Selection) attach2TaskProfile(profiles ...taskProfile) taskProfile {
	profile := profiles[0].copy()
	switch t := profile.(type) {
	case *kvTaskProfile:
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
				t.cst += float64(t.cnt) * cpuFactor
			}
			if tableSel != nil {
				tableSel.SetChildren(t.tablePlan)
				t.tablePlan = tableSel
				t.addPlan2Index = false
				t.cst += float64(t.cnt) * cpuFactor
			}
		} else {
			sel.SetChildren(t.tablePlan)
			t.tablePlan = sel
			t.cst += float64(t.cnt) * cpuFactor
		}
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
	case *singletonTaskProfile:
		t.cst += float64(t.cnt) * cpuFactor
		t.cnt = uint64(float64(t.cnt) * selectionFactor)
	}
	return profile
}

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
		tableSel = &Selection{
			baseLogicalPlan: newBaseLogicalPlan(Sel, sel.allocator),
			Conditions:      tableConds,
		}
		tableSel.self = tableSel
		tableSel.initIDAndContext(sel.ctx)
	}
	return
}
