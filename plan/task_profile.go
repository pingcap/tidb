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
	attachPlan(p PhysicalPlan)
	setCount(cnt uint64)
	count() uint64
	setCost(cost float64)
	cost() float64
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

func (t *kvTaskProfile) attachPlan(p PhysicalPlan) {
	if t.addPlan2Index {
		p.SetChildren(t.indexPlan)
		t.indexPlan = p
	} else {
		p.SetChildren(t.tablePlan)
		t.tablePlan = p
	}
}

func (t *kvTaskProfile) finishTask() {
	t.cst += float64(t.cnt) * netWorkFactor
	if t.tablePlan != nil && t.indexPlan != nil {
		t.cst += float64(t.cnt) * netWorkFactor
	}
}

// singletonTaskProfile is a profile running on tidb with single goroutine.
type singletonTaskProfile struct {
	plan   PhysicalPlan
	cst    float64
	cnt    uint64
	kvTask *kvTaskProfile
}

func (t *singletonTaskProfile) attachPlan(p PhysicalPlan) {
	p.SetChildren(t.plan)
	t.plan = p
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

func (sort *Sort) attach2TaskProfile(profile taskProfile) {
	// TODO: If index plan contains all the columns that topn needs, push it to index plan.
	profile.attachPlan(sort)
	profile.setCost(float64(profile.count()) * cpuFactor)
	profile.setCount(sort.ExecLimit.Count)
}

func (limit *Limit) attach2TaskProfile(profile taskProfile) {
	profile.attachPlan(limit)
	profile.setCount(limit.Count)
}

func (agg *PhysicalAggregation) attach2TaskProfile(profile taskProfile) {
	switch t := profile.(type) {
	case *kvTaskProfile:
		t.attachPlan(agg)
		t.cst += float64(t.cnt) * cpuFactor
		if len(agg.GroupByItems) == 0 {
			t.cnt = 1
			return
		}
		// The count should be distinct count, now we can't estimate it.
		t.cnt = uint64(float64(t.cnt) * aggFactor)
	}
}

func (sel *Selection) attach2TaskProfile(profile taskProfile) {
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
