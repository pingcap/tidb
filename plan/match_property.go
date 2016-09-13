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
	"github.com/pingcap/tidb/util/types"
	"math"
)

// matchProperty implements PhysicalPlan matchProperty interface.
func (ts *PhysicalTableScan) matchProperty(prop *requiredProperty, infos ...*physicalPlanInfo) *physicalPlanInfo {
	rowCount := float64(infos[0].count)
	cost := rowCount * netWorkFactor
	if len(prop.props) == 0 {
		return &physicalPlanInfo{p: ts, cost: cost, count: infos[0].count}
	}
	if len(prop.props) == 1 && ts.pkCol != nil && ts.pkCol == prop.props[0].col {
		sortedTs := *ts
		sortedTs.Desc = prop.props[0].desc
		sortedTs.KeepOrder = true
		return &physicalPlanInfo{p: &sortedTs, cost: cost, count: infos[0].count}
	}
	return &physicalPlanInfo{p: ts, cost: math.MaxFloat64, count: infos[0].count}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (is *PhysicalIndexScan) matchProperty(prop *requiredProperty, infos ...*physicalPlanInfo) *physicalPlanInfo {
	rowCount := float64(infos[0].count)
	cost := rowCount * netWorkFactor
	if is.DoubleRead {
		cost *= 2
	}
	if len(prop.props) == 0 {
		return &physicalPlanInfo{p: is, cost: cost, count: infos[0].count}
	}
	matchedIdx := 0
	allDesc, allAsc := true, true
	matchedList := make([]bool, len(prop.props))
	for i, idxCol := range is.Index.Columns {
		if idxCol.Length != types.UnspecifiedLength {
			break
		}
		findMatched := false
		if matchedIdx < prop.sortKeyLen {
			propCol := prop.props[matchedIdx]
			if idxCol.Name.L == propCol.col.ColName.L {
				findMatched = true
				matchedList[matchedIdx] = true
				if propCol.desc {
					allAsc = false
				} else {
					allDesc = false
				}
				matchedIdx++
			}
		} else {
			for j, propCol := range prop.props {
				if idxCol.Name.L == propCol.col.ColName.L {
					matchedList[j] = true
					findMatched = true
					break
				}
			}
		}
		if !findMatched && i >= is.accessEqualCount {
			break
		}
	}
	isMatch := true
	for _, matched := range matchedList {
		if !matched {
			isMatch = false
		}
	}
	if isMatch {
		sortedCost := cost + rowCount*cpuFactor
		if allAsc {
			sortedIs := *is
			sortedIs.OutOfOrder = false
			return &physicalPlanInfo{p: &sortedIs, cost: sortedCost, count: infos[0].count}
		}
		if allDesc {
			sortedIs := *is
			sortedIs.Desc = true
			sortedIs.OutOfOrder = false
			return &physicalPlanInfo{p: &sortedIs, cost: sortedCost, count: infos[0].count}
		}
	}
	return &physicalPlanInfo{p: is, cost: math.MaxFloat64, count: infos[0].count}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalHashSemiJoin) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	lRes, rRes := childPlanInfo[0], childPlanInfo[1]
	np := *p
	np.SetChildren(lRes.p, rRes.p)
	cost := lRes.cost + rRes.cost
	return &physicalPlanInfo{p: &np, cost: cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalApply) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	np := *p
	np.SetChildren(childPlanInfo[0].p)
	return &physicalPlanInfo{p: &np, cost: childPlanInfo[0].cost}
}

func estimateJoinCount(lc uint64, rc uint64) uint64 {
	return lc * rc / 3
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalHashJoin) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	lRes, rRes := childPlanInfo[0], childPlanInfo[1]
	lCount, rCount := float64(lRes.count), float64(rRes.count)
	np := *p
	np.SetChildren(lRes.p, rRes.p)
	if len(prop.props) != 0 {
		np.Concurrency = 1
	}
	cost := lRes.cost + rRes.cost
	if p.SmallTable == 1 {
		cost += lCount + memoryFactor*rCount
	} else {
		cost += rCount + memoryFactor*lCount
	}
	return &physicalPlanInfo{p: &np, cost: cost, count: estimateJoinCount(lRes.count, rRes.count)}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Union) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	np := *p
	children := make([]Plan, 0, len(childPlanInfo))
	cost := float64(0)
	for _, res := range childPlanInfo {
		children = append(children, res.p)
		cost += res.cost
	}
	np.SetChildren(children...)
	return &physicalPlanInfo{p: &np, cost: cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Selection) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	if childPlanInfo[0].p == nil {
		res := p.GetChildByIndex(0).(PhysicalPlan).matchProperty(prop, childPlanInfo...)
		sel := *p
		sel.SetChildren(res.p)
		res.p = &sel
		res.count = uint64(float64(res.count) * selectionFactor)
		return res
	}
	np := *p
	np.SetChildren(childPlanInfo[0].p)
	count := uint64(float64(childPlanInfo[0].count) * selectionFactor)
	return &physicalPlanInfo{p: &np, cost: childPlanInfo[0].cost, count: count}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalUnionScan) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	if childPlanInfo[0].p == nil {
		res := p.GetChildByIndex(0).(PhysicalPlan).matchProperty(prop, childPlanInfo...)
		np := *p
		np.SetChildren(res.p)
		res.p = &np
		return res
	}
	np := *p
	np.SetChildren(childPlanInfo[0].p)
	return &physicalPlanInfo{p: &np, cost: childPlanInfo[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Projection) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	np := *p
	np.SetChildren(childPlanInfo[0].p)
	return &physicalPlanInfo{p: &np, cost: childPlanInfo[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *MaxOneRow) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Exists) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Trim) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalAggregation) matchProperty(prop *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Limit) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Distinct) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *TableDual) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Sort) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Insert) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *SelectLock) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Update) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalDummyScan) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Delete) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}
