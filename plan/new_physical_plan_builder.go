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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

func (p *requiredProp) enforceProperty(task taskProfile, ctx context.Context, allocator *idAllocator) taskProfile {
	if p.isEmpty() {
		return task
	}
	sort := Sort{ByItems: make([]*ByItems, 0, len(p.cols))}.init(allocator, ctx)
	for _, col := range p.cols {
		sort.ByItems = append(sort.ByItems, &ByItems{col, p.desc})
	}
	return sort.attach2TaskProfile(task)
}

// getPushedProp will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *Projection) getPushedProp(prop *requiredProp) (*requiredProp, bool) {
	newProp := &requiredProp{}
	if prop.isEmpty() {
		return newProp, false
	}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		if idx == -1 {
			return newProp, false
		}
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil, false
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return newProp, true
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If the Projection maps a scalar function to a sort column, it will refuse the prop.
// TODO: We can analyze the function dependence to propagate the required prop. e.g For a + 1 as b , we can take the order
// of b to a.
func (p *Projection) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// enforceProperty task.
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2TaskProfile(task)
	task = prop.enforceProperty(task, p.ctx, p.allocator)

	newProp, canPassProp := p.getPushedProp(prop)
	if canPassProp {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		orderedTask = p.attach2TaskProfile(orderedTask)
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	return task, p.storeTaskProfile(prop, task)
}

// getPushedProp will check if this sort property can be pushed or not. In order to simplify the problem, we only
// consider the case that all expression are columns and all of them are asc or desc.
func (p *Sort) getPushedProp() (*requiredProp, bool) {
	desc := false
	cols := make([]*expression.Column, len(p.ByItems))
	for i, item := range p.ByItems {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		cols = append(cols, col)
		desc = item.Desc
		if i > 0 && item.Desc != p.ByItems[i-1].Desc {
			return nil, false
		}
	}
	return &requiredProp{cols, desc}, true
}

// convert2NewPhysicalPlan implements PhysicalPlan interface.
// If this sort is a topN plan, we will try to push the sort down and leave the limit.
// TODO: If this is a sort plan and the coming prop is not nil, this plan is redundant and can be removed.
func (p *Sort) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// enforce branch
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2TaskProfile(task)
	newProp, canPassProp := p.getPushedProp()
	if canPassProp {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Leave the limit.
		if p.ExecLimit != nil {
			limit := Limit{Offset: p.ExecLimit.Offset, Count: p.ExecLimit.Count}.init(p.allocator, p.ctx)
			orderedTask = limit.attach2TaskProfile(orderedTask)
		}
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	return task, p.storeTaskProfile(prop, task)
}

// TODO: The behavior of Limit may be same with many other plans. Most plans can pass the props or refuse and enforce it.
func (p *Limit) convert2NewPhysicalPlan(prop *requiredProp) (taskProfile, error) {
	task, err := p.getTaskProfile(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if task != nil {
		return task, nil
	}
	// enforce branch
	task, err = p.children[0].(LogicalPlan).convert2NewPhysicalPlan(&requiredProp{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	task = p.attach2TaskProfile(task)
	task = prop.enforceProperty(task, p.ctx, p.allocator)
	if !prop.isEmpty() {
		orderedTask, err := p.children[0].(LogicalPlan).convert2NewPhysicalPlan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if orderedTask.cost() < task.cost() {
			task = orderedTask
		}
	}
	return task, p.storeTaskProfile(prop, task)
}
