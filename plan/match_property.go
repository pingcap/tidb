// Copyright 2016 PingCAP, Inc.
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
)

// matchProperty implements PhysicalPlan matchProperty interface.
func (ts *PhysicalTableScan) matchProperty(prop requiredProperty, rowCounts []uint64, _ ...*planInfo) *planInfo {
	rowCount := float64(rowCounts[0])
	cost := rowCount * netWorkFactor
	if len(prop) == 0 {
		return &planInfo{p: ts, cost: cost}
	}
	if len(prop) == 1 && ts.pkCol != nil && ts.pkCol == prop[0].col {
		sortedTs := *ts
		sortedTs.Desc = prop[0].desc
		return &planInfo{p: &sortedTs, cost: cost}
	}
	return &planInfo{p: ts, cost: math.MaxFloat64}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (is *PhysicalIndexScan) matchProperty(prop requiredProperty, rowCounts []uint64, _ ...*planInfo) *planInfo {
	rowCount := float64(rowCounts[0])
	// currently index read from kv 2 times.
	cost := rowCount * netWorkFactor * 2
	if len(prop) == 0 {
		return &planInfo{p: is, cost: cost}
	}
	matched := 0
	allDesc, allAsc := true, true
	for i, indexCol := range is.Index.Columns {
		if prop[matched].col.ColName.L != indexCol.Name.L {
			if matched != 0 || i >= is.accessEqualCount {
				break
			}
			continue
		}
		if prop[matched].desc {
			allAsc = false
		} else {
			allDesc = false
		}
		matched++
		if matched == len(prop) {
			break
		}
	}
	if matched == len(prop) {
		sortedCost := cost + rowCount*math.Log2(rowCount)
		if allDesc {
			sortedIs := *is
			sortedIs.Desc = true
			sortedIs.OutOfOrder = false
			return &planInfo{p: &sortedIs, cost: sortedCost}
		}
		if allAsc {
			sortedIs := *is
			sortedIs.OutOfOrder = false
			return &planInfo{p: &sortedIs, cost: sortedCost}
		}
	}
	return &planInfo{p: is, cost: math.MaxFloat64}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalHashSemiJoin) matchProperty(prop requiredProperty, _ []uint64, response ...*planInfo) *planInfo {
	lRes, rRes := response[0], response[1]
	np := *p
	np.SetChildren(lRes.p, rRes.p)
	cost := lRes.cost + rRes.cost
	return &planInfo{p: &np, cost: cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalApply) matchProperty(prop requiredProperty, rowCounts []uint64, response ...*planInfo) *planInfo {
	np := *p
	np.SetChildren(response[0].p)
	return &planInfo{p: &np, cost: response[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalHashJoin) matchProperty(prop requiredProperty, rowCounts []uint64, response ...*planInfo) *planInfo {
	lRes, rRes := response[0], response[1]
	lCount, rCount := float64(rowCounts[0]), float64(rowCounts[1])
	np := *p
	np.SetChildren(lRes.p, rRes.p)
	cost := lRes.cost + rRes.cost
	if p.SmallTable == 1 {
		cost += lCount + memoryFactor*rCount
	} else {
		cost += rCount + memoryFactor*lCount
	}
	return &planInfo{p: &np, cost: cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *NewUnion) matchProperty(prop requiredProperty, _ []uint64, response ...*planInfo) *planInfo {
	np := *p
	children := make([]Plan, 0, len(response))
	cost := float64(0)
	for _, res := range response {
		children = append(children, res.p)
		cost += res.cost
	}
	np.SetChildren(children...)
	return &planInfo{p: &np, cost: cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Selection) matchProperty(prop requiredProperty, rowCounts []uint64, response ...*planInfo) *planInfo {
	if len(response) == 0 {
		res := p.GetChildByIndex(0).(PhysicalPlan).matchProperty(prop, rowCounts)
		sel := *p
		sel.SetChildren(res.p)
		res.p = &sel
		return res
	}
	np := *p
	np.SetChildren(response[0].p)
	return &planInfo{p: &np, cost: response[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Projection) matchProperty(_ requiredProperty, _ []uint64, response ...*planInfo) *planInfo {
	np := *p
	np.SetChildren(response[0].p)
	return &planInfo{p: &np, cost: response[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *MaxOneRow) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Exists) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Trim) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Aggregation) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Limit) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Distinct) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *NewTableDual) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *NewSort) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Insert) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *SelectLock) matchProperty(_ requiredProperty, _ []uint64, _ ...*planInfo) *planInfo {
	panic("You can't call this function!")
}
