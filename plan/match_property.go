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

	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

// matchProperty implements PhysicalPlan matchProperty interface.
func (ts *PhysicalTableScan) matchProperty(prop *requiredProperty, infos ...*physicalPlanInfo) *physicalPlanInfo {
	rowCount := float64(infos[0].count)
	cost := rowCount * netWorkFactor
	if prop.limit != nil {
		cost = float64(prop.limit.Count+prop.limit.Offset) * netWorkFactor
	}
	if len(prop.props) == 0 {
		newTS := *ts
		newTS.addLimit(prop.limit)
		p := newTS.tryToAddUnionScan(&newTS)
		return enforceProperty(prop, &physicalPlanInfo{p: p, cost: cost, count: infos[0].count})
	}
	if len(prop.props) == 1 && ts.pkCol != nil && ts.pkCol.Equal(prop.props[0].col, ts.ctx) {
		sortedTS := *ts
		sortedTS.Desc = prop.props[0].desc
		sortedTS.KeepOrder = true
		sortedTS.addLimit(prop.limit)
		// If there exists a table filter, we should calculate the filter scan cost.
		if len(sortedTS.tableFilterConditions) > 0 {
			cost += rowCount * cpuFactor
		}
		p := sortedTS.tryToAddUnionScan(&sortedTS)
		return enforceProperty(&requiredProperty{limit: prop.limit}, &physicalPlanInfo{
			p:     p,
			cost:  cost,
			count: infos[0].count})
	}
	if prop.limit != nil {
		sortedTS := *ts
		success := sortedTS.addTopN(ts.ctx, prop)
		if success {
			cost += rowCount * cpuFactor
		} else {
			cost = rowCount * netWorkFactor
		}
		sortedTS.KeepOrder = true
		p := sortedTS.tryToAddUnionScan(&sortedTS)
		return enforceProperty(prop, &physicalPlanInfo{
			p:     p,
			cost:  cost,
			count: infos[0].count})
	}
	return &physicalPlanInfo{p: nil, cost: math.MaxFloat64, count: infos[0].count}
}

func allMatch(matchedList []bool) bool {
	for _, matched := range matchedList {
		if !matched {
			return false
		}
	}
	return true
}

// matchPropColumn checks if the idxCol match one of columns in required property and return the matched index.
// If no column is matched, return -1.
func matchPropColumn(prop *requiredProperty, matchedIdx int, idxCol *model.IndexColumn) int {
	if matchedIdx < prop.sortKeyLen {
		// When walking through the first sorKeyLen column,
		// we should make sure to match them as the columns order exactly.
		// So we must check the column in position of matchedIdx.
		propCol := prop.props[matchedIdx]
		if idxCol.Name.L == propCol.col.ColName.L {
			return matchedIdx
		}
	} else {
		// When walking outside the first sorKeyLen column, we can match the columns as any order.
		for j, propCol := range prop.props {
			if idxCol.Name.L == propCol.col.ColName.L {
				return j
			}
		}
	}
	return -1
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (is *PhysicalIndexScan) matchProperty(prop *requiredProperty, infos ...*physicalPlanInfo) *physicalPlanInfo {
	rowCount := float64(infos[0].count)
	if prop.limit != nil {
		rowCount = float64(prop.limit.Count)
	}
	cost := rowCount * netWorkFactor
	if is.DoubleRead {
		cost *= 2
	}
	if len(prop.props) == 0 {
		p := is.tryToAddUnionScan(is)
		return enforceProperty(&requiredProperty{limit: prop.limit}, &physicalPlanInfo{p: p, cost: cost, count: infos[0].count})
	}
	matchedIdx := 0
	matchedList := make([]bool, len(prop.props))
	for i, idxCol := range is.Index.Columns {
		if idxCol.Length != types.UnspecifiedLength {
			break
		}
		if idx := matchPropColumn(prop, matchedIdx, idxCol); idx >= 0 {
			matchedList[idx] = true
			matchedIdx++
		} else if i >= is.accessEqualCount {
			break
		}
	}
	if allMatch(matchedList) {
		allDesc, allAsc := true, true
		for i := 0; i < prop.sortKeyLen; i++ {
			if prop.props[i].desc {
				allAsc = false
			} else {
				allDesc = false
			}
		}
		sortedCost := cost + rowCount*cpuFactor
		if allAsc || allDesc {
			sortedIS := *is
			sortedIS.OutOfOrder = false
			sortedIS.Desc = allDesc && !allAsc
			sortedIS.addLimit(prop.limit)
			p := sortedIS.tryToAddUnionScan(&sortedIS)
			return enforceProperty(&requiredProperty{limit: prop.limit}, &physicalPlanInfo{
				p:     p,
				cost:  sortedCost,
				count: infos[0].count})
		}
	}
	if prop.limit != nil {
		sortedIS := *is
		success := sortedIS.addTopN(is.ctx, prop)
		if success {
			cost += float64(infos[0].count) * cpuFactor
		} else {
			cost = float64(infos[0].count) * netWorkFactor
		}
		sortedIS.OutOfOrder = true
		p := sortedIS.tryToAddUnionScan(&sortedIS)
		return enforceProperty(prop, &physicalPlanInfo{
			p:     p,
			cost:  cost,
			count: infos[0].count})
	}
	return &physicalPlanInfo{p: nil, cost: math.MaxFloat64, count: infos[0].count}
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
	count := float64(lc) * float64(rc) * joinFactor
	if count > math.MaxInt32 {
		return math.MaxInt32
	}
	return uint64(count)
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
	count := uint64(0)
	for _, res := range childPlanInfo {
		children = append(children, res.p)
		cost += res.cost
		count += res.count
	}
	np.SetChildren(children...)
	return &physicalPlanInfo{p: &np, cost: cost, count: count}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Selection) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	if p.onTable {
		res := p.children[0].(PhysicalPlan).matchProperty(prop, childPlanInfo...)
		sel := *p
		sel.SetChildren(res.p)
		res.p = &sel
		res.count = uint64(float64(res.count) * selectionFactor)
		return res
	}
	return childPlanInfo[0]
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalUnionScan) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	limit := prop.limit
	res := p.children[0].(PhysicalPlan).matchProperty(convertLimitOffsetToCount(prop), childPlanInfo...)
	np := *p
	np.SetChildren(res.p)
	res.p = &np
	if limit != nil {
		res = addPlanToResponse(limit, res)
	}
	return res
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Projection) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	np := *p
	np.SetChildren(childPlanInfo[0].p)
	return &physicalPlanInfo{p: &np, cost: childPlanInfo[0].cost}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Analyze) matchProperty(_ *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	children := make([]Plan, 0, len(childPlanInfo))
	for _, res := range childPlanInfo {
		children = append(children, res.p)
	}
	np := *p
	np.SetChildren(children...)
	return &physicalPlanInfo{p: &np}
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Cache) matchProperty(prop *requiredProperty, childPlanInfo ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
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
func (p *PhysicalAggregation) matchProperty(prop *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Limit) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
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

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *Show) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}

// matchProperty implements PhysicalPlan matchProperty interface.
func (p *PhysicalMemTable) matchProperty(_ *requiredProperty, _ ...*physicalPlanInfo) *physicalPlanInfo {
	panic("You can't call this function!")
}
