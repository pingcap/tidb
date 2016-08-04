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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/util/types"
)

const (
	netWorkFactor   = 1.5
	memoryFactor    = 5.0
	selectionFactor = 0.8
	distinctFactor  = 0.7
	cpuFactor       = 0.9
)

func getRowCountByIndexRange(table *statistics.Table, indexRange *IndexRange, indexInfo *model.IndexInfo) (uint64, error) {
	count := float64(table.Count)
	for i := 0; i < len(indexRange.LowVal); i++ {
		l := indexRange.LowVal[i]
		r := indexRange.HighVal[i]
		var rowCount int64
		var err error
		offset := indexInfo.Columns[i].Offset
		if l.Kind() == types.KindNull && r.Kind() == types.KindMaxValue {
			break
		} else if l.Kind() == types.KindMinNotNull {
			rowCount, err = table.Columns[offset].LessRowCount(r)
		} else if r.Kind() == types.KindMaxValue {
			rowCount, err = table.Columns[offset].LessRowCount(l)
			rowCount = table.Count - rowCount
		} else {
			compare, err1 := l.CompareDatum(r)
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			if compare == 0 {
				rowCount, err = table.Columns[offset].EqualRowCount(l)
			} else {
				rowCount, err = table.Columns[offset].BetweenRowCount(l, r)
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / float64(table.Count) * float64(rowCount)
	}
	return uint64(count), nil
}

func (p *DataSource) handleTableScan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, error) {
	table := p.Table
	var resultPlan PhysicalPlan
	ts := &PhysicalTableScan{
		Table:       p.Table,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		DBName:      p.DBName,
	}
	ts.SetSchema(p.GetSchema())
	resultPlan = ts
	if sel, ok := p.GetParentByIndex(0).(*Selection); ok {
		newSel := *sel
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.DeepCopy())
		}
		ts.AccessCondition, newSel.Conditions = detachTableScanConditions(conds, table)
		err := buildNewTableRange(ts)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if len(newSel.Conditions) > 0 {
			newSel.SetChildren(ts)
			resultPlan = &newSel
		}
	} else {
		ts.Ranges = []TableRange{{math.MinInt64, math.MaxInt64}}
	}

	statsTbl := p.statisticTable
	rowCount := uint64(statsTbl.Count)
	if table.PKIsHandle {
		for i, colInfo := range ts.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				ts.pkCol = p.GetSchema()[i]
				break
			}
		}
		var offset int
		for _, colInfo := range table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				offset = colInfo.Offset
				break
			}
		}
		rowCount = 0
		for _, rg := range ts.Ranges {
			var cnt int64
			var err error
			if rg.LowVal == math.MinInt64 && rg.HighVal == math.MaxInt64 {
				cnt = statsTbl.Count
			} else if rg.LowVal == math.MinInt64 {
				cnt, err = statsTbl.Columns[offset].LessRowCount(types.NewDatum(rg.HighVal))
			} else if rg.HighVal == math.MaxInt64 {
				cnt, err = statsTbl.Columns[offset].LessRowCount(types.NewDatum(rg.LowVal))
				cnt = statsTbl.Count - cnt
			} else {
				cnt, err = statsTbl.Columns[offset].BetweenRowCount(types.NewDatum(rg.LowVal), types.NewDatum(rg.HighVal))
			}
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			rowCount += uint64(cnt)
		}
	}
	rowCounts := []uint64{rowCount}
	return resultPlan.matchProperty(prop, rowCounts), resultPlan.matchProperty(nil, rowCounts), nil
}

func (p *DataSource) handleIndexScan(prop requiredProperty, index *model.IndexInfo) (*physicalPlanInfo, *physicalPlanInfo, error) {
	statsTbl := p.statisticTable
	var resultPlan PhysicalPlan
	is := &PhysicalIndexScan{
		Index:       index,
		Table:       p.Table,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		OutOfOrder:  true,
		DBName:      p.DBName,
	}
	is.SetSchema(p.schema)
	rowCount := uint64(statsTbl.Count)
	resultPlan = is
	if sel, ok := p.GetParentByIndex(0).(*Selection); ok {
		rowCount = 0
		newSel := *sel
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.DeepCopy())
		}
		is.AccessCondition, newSel.Conditions = detachIndexScanConditions(conds, is)
		err := buildNewIndexRange(is)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		for _, idxRange := range is.Ranges {
			cnt, err := getRowCountByIndexRange(statsTbl, idxRange, is.Index)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			rowCount += cnt
		}
		if len(newSel.Conditions) > 0 {
			newSel.SetChildren(is)
			resultPlan = &newSel
		}
	} else {
		rb := rangeBuilder{}
		is.Ranges = rb.buildIndexRanges(fullRange)
	}
	for _, colInfo := range is.Columns {
		for _, indexCol := range is.Index.Columns {
			if colInfo.Name.L != indexCol.Name.L {
				is.DoubleRead = true
				break
			}
		}
		if is.DoubleRead {
			break
		}
	}
	rowCounts := []uint64{rowCount}
	return resultPlan.matchProperty(prop, rowCounts), resultPlan.matchProperty(nil, rowCounts), nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *DataSource) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	sortedRes, unsortedRes, cnt := p.getPlanInfo(prop)
	if sortedRes != nil {
		return sortedRes, unsortedRes, cnt, nil
	}
	indices, includeTableScan := availableIndices(p.table)
	var err error
	if includeTableScan {
		sortedRes, unsortedRes, err = p.handleTableScan(prop)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
	}
	for _, index := range indices {
		sortedIsRes, unsortedIsRes, err := p.handleIndexScan(prop, index)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if sortedRes == nil || sortedIsRes.cost < sortedRes.cost {
			sortedRes = sortedIsRes
		}
		if unsortedRes == nil || unsortedIsRes.cost < unsortedRes.cost {
			unsortedRes = unsortedIsRes
		}
	}
	statsTbl := p.statisticTable
	p.storePlanInfo(prop, sortedRes, unsortedRes, uint64(statsTbl.Count))
	return sortedRes, unsortedRes, uint64(statsTbl.Count), nil
}

func addPlanToResponse(p PhysicalPlan, planInfo *physicalPlanInfo) *physicalPlanInfo {
	np := p.Copy()
	np.SetChildren(planInfo.p)
	return &physicalPlanInfo{p: np, cost: planInfo.cost}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Limit) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	var err error
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	sortedPlanInfo, unSortedPlanInfo, count, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if p.Offset+p.Count < count {
		count = p.Offset + p.Count
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

func estimateJoinCount(lc uint64, rc uint64) uint64 {
	return lc * rc / 3
}

func (p *Join) handleLeftJoin(prop requiredProperty, innerJoin bool) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	lChild := p.GetChildByIndex(0).(LogicalPlan)
	rChild := p.GetChildByIndex(1).(LogicalPlan)
	allLeft := true
	for _, col := range prop {
		if lChild.GetSchema().GetIndex(col.col) == -1 {
			allLeft = false
		}
	}
	join := &PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		SmallTable:      1,
	}
	join.SetSchema(p.schema)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = LeftOuterJoin
	}
	if !allLeft {
		prop = nil
	}
	lSortedPlanInfo, lUnSortedPlanInfo, lCount, err := lChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allLeft {
		lSortedPlanInfo.cost = math.MaxFloat64
	}
	rSortedPlanInfo, rUnSortedPlanInfo, rCount, err := rChild.convert2PhysicalPlan(nil)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lSortedPlanInfo, rSortedPlanInfo)
	unSortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lUnSortedPlanInfo, rUnSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, estimateJoinCount(lCount, rCount), nil
}

func (p *Join) handleRightJoin(prop requiredProperty, innerJoin bool) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	lChild := p.GetChildByIndex(0).(LogicalPlan)
	rChild := p.GetChildByIndex(1).(LogicalPlan)
	allRight := true
	for _, col := range prop {
		if rChild.GetSchema().GetIndex(col.col) == -1 {
			allRight = false
		}
	}
	join := &PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
	}
	join.SetSchema(p.schema)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = RightOuterJoin
	}
	lSortedPlanInfo, lUnSortedPlanInfo, lCount, err := lChild.convert2PhysicalPlan(nil)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		prop = nil
	}
	rSortedPlanInfo, rUnSortedPlanInfo, rCount, err := rChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		rSortedPlanInfo.cost = math.MaxFloat64
	}
	sortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lSortedPlanInfo, rSortedPlanInfo)
	unSortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lUnSortedPlanInfo, rUnSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, estimateJoinCount(lCount, rCount), nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Join) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	sortedPlanInfo, unsortedPlanInfo, cnt := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unsortedPlanInfo, cnt, nil
	}
	switch p.JoinType {
	case SemiJoin, SemiJoinWithAux:
		lChild := p.GetChildByIndex(0).(LogicalPlan)
		rChild := p.GetChildByIndex(1).(LogicalPlan)
		allLeft := true
		for _, col := range prop {
			if lChild.GetSchema().GetIndex(col.col) == -1 {
				allLeft = false
			}
		}
		join := &PhysicalHashSemiJoin{
			WithAux:         SemiJoinWithAux == p.JoinType,
			EqualConditions: p.EqualConditions,
			LeftConditions:  p.LeftConditions,
			RightConditions: p.RightConditions,
			OtherConditions: p.OtherConditions,
			Anti:            p.anti,
		}
		join.SetSchema(p.schema)
		if !allLeft {
			prop = nil
		}
		lSortedPlanInfo, lUnSortedPlanInfo, lCount, err := lChild.convert2PhysicalPlan(prop)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		rSortedPlanInfo, rUnSortedPlanInfo, rCount, err := rChild.convert2PhysicalPlan(nil)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		sortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lSortedPlanInfo, rSortedPlanInfo)
		unSortedPlanInfo := join.matchProperty(prop, []uint64{lCount, rCount}, lUnSortedPlanInfo, rUnSortedPlanInfo)
		if p.JoinType == SemiJoin {
			lCount = uint64(float64(lCount) * selectionFactor)
		}
		if !allLeft {
			sortedPlanInfo.cost = math.MaxFloat64
		}
		p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, lCount)
		return sortedPlanInfo, unSortedPlanInfo, lCount, nil
	case LeftOuterJoin:
		sortedPlanInfo, unSortedPlanInfo, count, err := p.handleLeftJoin(prop, false)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	case RightOuterJoin:
		sortedPlanInfo, unSortedPlanInfo, count, err := p.handleRightJoin(prop, false)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	default:
		lSortedPlanInfo, lunSortedPlanInfo, count, err := p.handleLeftJoin(prop, true)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		rsortedPlanInfo, runSortedPlanInfo, _, err := p.handleRightJoin(prop, true)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if rsortedPlanInfo.cost < lSortedPlanInfo.cost {
			lSortedPlanInfo = rsortedPlanInfo
		}
		if runSortedPlanInfo.cost < lunSortedPlanInfo.cost {
			lunSortedPlanInfo = runSortedPlanInfo
		}
		p.storePlanInfo(prop, lSortedPlanInfo, lunSortedPlanInfo, count)
		return lSortedPlanInfo, lunSortedPlanInfo, count, nil
	}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Aggregation) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	_, planInfo, cnt := p.getPlanInfo(prop)
	if planInfo != nil {
		return planInfo, planInfo, cnt, nil
	}
	_, planInfo, cnt, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(nil)
	if len(prop) != 0 {
		return &physicalPlanInfo{cost: math.MaxFloat64}, addPlanToResponse(p, planInfo), cnt / 3, errors.Trace(err)
	}
	planInfo = addPlanToResponse(p, planInfo)
	p.storePlanInfo(prop, planInfo, planInfo, cnt/3)
	return planInfo, planInfo, cnt / 3, errors.Trace(err)
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *NewUnion) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	count = uint64(0)
	var sortedPlanInfoCollection, unSortedPlanInfoCollection []*physicalPlanInfo
	for _, child := range p.GetChildren() {
		newProp := make(requiredProperty, 0, len(prop))
		for _, c := range prop {
			idx := p.GetSchema().GetIndex(c.col)
			newProp = append(newProp, &columnProp{col: child.GetSchema()[idx], desc: c.desc})
		}
		var cnt uint64
		sortedPlanInfo, unSortedPlanInfo, cnt, err = child.(LogicalPlan).convert2PhysicalPlan(newProp)
		count += cnt
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		sortedPlanInfoCollection = append(sortedPlanInfoCollection, sortedPlanInfo)
		unSortedPlanInfoCollection = append(unSortedPlanInfoCollection, unSortedPlanInfo)
	}
	sortedPlanInfo = p.matchProperty(prop, nil, sortedPlanInfoCollection...)
	unSortedPlanInfo = p.matchProperty(prop, nil, unSortedPlanInfoCollection...)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Selection) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	sortedPlanInfo, unSortedPlanInfo, count, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if _, ok := p.GetChildByIndex(0).(*DataSource); ok {
		count = uint64(float64(count) * selectionFactor)
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	count /= 3
	sortedPlanInfo = p.matchProperty(prop, nil, sortedPlanInfo)
	unSortedPlanInfo = p.matchProperty(prop, nil, unSortedPlanInfo)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Projection) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	newProp := make(requiredProperty, 0, len(prop))
	childSchema := p.GetChildByIndex(0).GetSchema()
	usedCols := make([]bool, len(childSchema))
	canPassSort := true
loop:
	for _, c := range prop {
		idx := p.schema.GetIndex(c.col)
		switch v := p.Exprs[idx].(type) {
		case *expression.Column:
			childIdx := childSchema.GetIndex(v)
			if !usedCols[childIdx] {
				usedCols[childIdx] = true
				newProp = append(newProp, &columnProp{col: v, desc: c.desc})
			}
		case *expression.ScalarFunction:
			newProp = nil
			canPassSort = false
			break loop
		}
	}
	sortedPlanInfo, unSortedPlanInfo, count, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(newProp)
	if err != nil {
		return nil, nil, count, errors.Trace(err)
	}
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	if !canPassSort {
		return &physicalPlanInfo{cost: math.MaxFloat64}, unSortedPlanInfo, count, nil
	}

	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

func matchProp(target, new requiredProperty) bool {
	if len(target) > len(new) {
		return false
	}
	for i := 0; i < len(target); i++ {
		if target[i].desc != new[i].desc ||
			target[i].col.FromID != new[i].col.FromID ||
			target[i].col.Index != new[i].col.Index {
			return false
		}
	}
	return true
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *NewSort) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	selfProp := make(requiredProperty, 0, len(p.ByItems))
	for _, by := range p.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			selfProp = append(selfProp, &columnProp{col: col, desc: by.Desc})
		} else {
			selfProp = nil
			break
		}
	}
	sortedPlanInfo, unSortedPlanInfo, count, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(selfProp)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	cnt := float64(count)
	sortCost := cnt*math.Log2(cnt)*cpuFactor + memoryFactor*cnt
	if len(selfProp) == 0 {
		sortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	} else if sortCost+unSortedPlanInfo.cost < sortedPlanInfo.cost {
		sortedPlanInfo.cost = sortCost + unSortedPlanInfo.cost
		sortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	}
	if matchProp(prop, selfProp) {
		return sortedPlanInfo, sortedPlanInfo, count, nil
	}
	unSortedPlanInfo = sortedPlanInfo
	sortedPlanInfo = &physicalPlanInfo{cost: math.MaxFloat64}
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Apply) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	_, innerRes, _, err := p.InnerPlan.convert2PhysicalPlan(nil)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	np := &PhysicalApply{
		OuterSchema: p.OuterSchema,
		Checker:     p.Checker,
		InnerPlan:   innerRes.p,
	}
	np.SetSchema(p.GetSchema())
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(np, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(np, unSortedPlanInfo)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Distinct) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	count = uint64(float64(count) * distinctFactor)
	p.storePlanInfo(prop, sortedPlanInfo, unSortedPlanInfo, count)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *NewTableDual) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	planInfo := &physicalPlanInfo{p: p, cost: 1.0}
	return planInfo, planInfo, 1, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *MaxOneRow) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Exists) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Trim) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *SelectLock) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo = addPlanToResponse(p, sortedPlanInfo)
	unSortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	return sortedPlanInfo, unSortedPlanInfo, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Insert) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	if len(p.GetChildren()) == 0 {
		planInfo := &physicalPlanInfo{p: p}
		return planInfo, planInfo, 0, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	sortedPlanInfo, unSortedPlanInfo, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, sortedPlanInfo), addPlanToResponse(p, unSortedPlanInfo), count, nil
}
