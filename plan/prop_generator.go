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
	"math"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

func (p *requiredProp) enforceProperty(task task, ctx context.Context) task {
	if p.isEmpty() {
		return task
	}
	// If task is invalid, keep it remained.
	if task.plan() == nil {
		return task
	}
	task = finishCopTask(task, ctx)
	sort := PhysicalSort{ByItems: make([]*ByItems, 0, len(p.cols))}.init(ctx)
	for _, col := range p.cols {
		sort.ByItems = append(sort.ByItems, &ByItems{col, p.desc})
	}
	sort.SetSchema(task.plan().Schema())
	sort.profile = task.plan().statsProfile()
	return sort.attach2Task(task)
}

func (p *PhysicalUnionScan) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	return [][]*requiredProp{{prop}}
}

// getChildrenPossibleProps will check if this sort property can be pushed or not.
// When a sort column will be replaced by scalar function, we refuse it.
// When a sort column will be replaced by a constant, we just remove it.
func (p *PhysicalProjection) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	newProp := &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}
	newCols := make([]*expression.Column, 0, len(prop.cols))
	for _, col := range prop.cols {
		idx := p.schema.ColumnIndex(col)
		if idx == -1 {
			return nil
		}
		switch expr := p.Exprs[idx].(type) {
		case *expression.Column:
			newCols = append(newCols, expr)
		case *expression.ScalarFunction:
			return nil
		}
	}
	newProp.cols = newCols
	newProp.desc = prop.desc
	return [][]*requiredProp{{newProp}}
}

// getChildrenPossibleProps gets children possible props.:
// For index join, we shouldn't require a root task which may let CBO framework select a sort operator in fact.
// We are not sure which way of index scanning we should choose, so we try both single read and double read and finally
// it will result in a best one.
func (p *PhysicalIndexJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() && !p.KeepOrder {
		return nil
	}
	for _, col := range prop.cols {
		if p.outerSchema.ColumnIndex(col) == -1 {
			return nil
		}
	}
	requiredProps1 := make([]*requiredProp, 2)
	requiredProps1[p.OuterIndex] = &requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt, cols: prop.cols, desc: prop.desc}
	requiredProps1[1-p.OuterIndex] = &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}
	return [][]*requiredProp{requiredProps1}
}

func (p *PhysicalMergeJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: p.leftKeys, expectedCnt: math.MaxFloat64}
	rProp := &requiredProp{taskTp: rootTaskType, cols: p.rightKeys, expectedCnt: math.MaxFloat64}
	if !prop.isEmpty() {
		if prop.desc {
			return nil
		}
		if !prop.isPrefix(lProp) && !prop.isPrefix(rProp) {
			return nil
		}
		if prop.isPrefix(rProp) && p.JoinType == LeftOuterJoin {
			return nil
		}
		if prop.isPrefix(lProp) && p.JoinType == RightOuterJoin {
			return nil
		}
	}

	return [][]*requiredProp{{lProp, rProp}}
}

func (p *basePhysicalPlan) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.basePlan.expectedCnt = prop.expectedCnt
	// By default, physicalPlan can always match the orders.
	props := make([]*requiredProp, 0, len(p.basePlan.children))
	for range p.basePlan.children {
		props = append(props, prop)
	}
	return [][]*requiredProp{props}
}

func (p *PhysicalSelection) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	return [][]*requiredProp{{prop}}
}

func (p *PhysicalLock) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	return [][]*requiredProp{{prop}}
}

func (p *PhysicalUnionAll) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	props := make([]*requiredProp, p.childNum)
	for i := range props {
		props[i] = prop
	}
	return [][]*requiredProp{props}
}

func (p *PhysicalHashJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	return [][]*requiredProp{{&requiredProp{taskTp: rootTaskType, expectedCnt: prop.expectedCnt}, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *PhysicalHashSemiJoin) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols, expectedCnt: prop.expectedCnt, desc: prop.desc}
	for _, col := range lProp.cols {
		idx := p.Schema().ColumnIndex(col)
		if idx == -1 || idx >= p.rightChOffset {
			return nil
		}
	}
	return [][]*requiredProp{{lProp, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *PhysicalApply) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	lProp := &requiredProp{taskTp: rootTaskType, cols: prop.cols, expectedCnt: prop.expectedCnt, desc: prop.desc}
	for _, col := range lProp.cols {
		idx := p.Schema().ColumnIndex(col)
		if idx == -1 || idx >= p.rightChOffset {
			return nil
		}
	}
	return [][]*requiredProp{{lProp, &requiredProp{taskTp: rootTaskType, expectedCnt: math.MaxFloat64}}}
}

func (p *PhysicalLimit) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		newProp := &requiredProp{taskTp: tp, expectedCnt: float64(p.Count + p.Offset)}
		if p.expectedProp != nil {
			newProp.cols = p.expectedProp.cols
			newProp.desc = p.expectedProp.desc
		}
		props = append(props, []*requiredProp{newProp})
	}
	return props
}

func (p *PhysicalTopN) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		props = append(props, []*requiredProp{{taskTp: tp, expectedCnt: math.MaxFloat64}})
	}
	return props
}

func (p *PhysicalHashAgg) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if !prop.isEmpty() {
		return nil
	}
	props := make([][]*requiredProp, 0, len(wholeTaskTypes))
	for _, tp := range wholeTaskTypes {
		props = append(props, []*requiredProp{{taskTp: tp, expectedCnt: math.MaxFloat64}})
	}
	return props
}

func (p *PhysicalStreamAgg) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	reqProp := &requiredProp{
		taskTp:      rootTaskType,
		cols:        p.propKeys,
		expectedCnt: prop.expectedCnt * p.inputCount / p.profile.count,
		desc:        prop.desc,
	}
	if !prop.isEmpty() && !prop.isPrefix(reqProp) {
		return nil
	}
	return [][]*requiredProp{{reqProp}}
}

func (p *PhysicalSort) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	p.expectedCnt = prop.expectedCnt
	if len(p.ByItems) >= len(prop.cols) {
		for i, col := range prop.cols {
			sortItem := p.ByItems[i]
			if sortItem.Desc != prop.desc || !sortItem.Expr.Equal(col, p.ctx) {
				return nil
			}
		}
		return [][]*requiredProp{{{expectedCnt: math.MaxFloat64}}}
	}
	return nil
}

func (p *NominalSort) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if prop.isPrefix(p.prop) {
		p.prop.expectedCnt = prop.expectedCnt
		return [][]*requiredProp{{p.prop}}
	}
	return nil
}

func (p *PhysicalExists) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if prop.isEmpty() {
		return [][]*requiredProp{{{expectedCnt: math.MaxFloat64}}}
	}
	return nil
}

func (p *PhysicalMaxOneRow) getChildrenPossibleProps(prop *requiredProp) [][]*requiredProp {
	if prop.isEmpty() {
		return [][]*requiredProp{{{expectedCnt: math.MaxFloat64}}}
	}
	return nil
}
