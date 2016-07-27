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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/plan/statistics"
	"github.com/pingcap/tidb/util/types"
	"math"
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
		if l.Kind() == types.KindMinNotNull && r.Kind() == types.KindMaxValue {
			break
		} else if l.Kind() == types.KindMinNotNull {
			rowCount, err = table.Columns[offset].LessRowCount(r)
		} else if r.Kind() == types.KindMaxValue {
			rowCount, err = table.Columns[offset].LessRowCount(r)
			rowCount = table.Count - rowCount
		} else {
			rowCount, err = table.Columns[offset].BetweenRowCount(l, r)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / float64(table.Count) * float64(rowCount)
	}
	return uint64(count), nil
}

func (p *DataSource) handleTableScan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, error) {
	statsTbl := p.statisticTable
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
				cnt, err = statsTbl.Columns[offset].LessRowCount(types.NewDatum(rg.HighVal))
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
	rowCounts := []uint64{rowCount}
	return resultPlan.matchProperty(prop, rowCounts), resultPlan.matchProperty(nil, rowCounts), nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *DataSource) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	statsTbl := p.statisticTable
	indices, includeTableScan := availableIndices(p.table)
	var sortedRes, unsortedRes *physicalPlanInfo
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
	return sortedRes, unsortedRes, uint64(statsTbl.Count), nil
}

func addPlanToResponse(p PhysicalPlan, res *physicalPlanInfo) *physicalPlanInfo {
	np := p.Copy()
	np.SetChildren(res.p)
	return &physicalPlanInfo{p: np, cost: res.cost}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Limit) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	res0, res1, count, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if p.Offset+p.Count < count {
		count = p.Offset + p.Count
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
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
	lRes0, lRes1, lCount, err := lChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allLeft {
		lRes0.cost = math.MaxFloat64
	}
	rRes0, rRes1, rCount, err := rChild.convert2PhysicalPlan(nil)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	res0 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes0, rRes0)
	res1 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes1, rRes1)
	return res0, res1, estimateJoinCount(lCount, rCount), nil
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
	lRes0, lRes1, lCount, err := lChild.convert2PhysicalPlan(nil)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		prop = nil
	}
	rRes0, rRes1, rCount, err := rChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		rRes0.cost = math.MaxFloat64
	}
	res0 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes0, rRes0)
	res1 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes1, rRes1)
	return res0, res1, estimateJoinCount(lCount, rCount), nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Join) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
		lRes0, lRes1, lCount, err := lChild.convert2PhysicalPlan(prop)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		rRes0, rRes1, rCount, err := rChild.convert2PhysicalPlan(nil)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		res0 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes0, rRes0)
		res1 := join.matchProperty(prop, []uint64{lCount, rCount}, lRes1, rRes1)
		if p.JoinType == SemiJoin {
			lCount = uint64(float64(lCount) * selectionFactor)
		}
		if !allLeft {
			res0.cost = math.MaxFloat64
		}
		return res0, res1, lCount, err
	case LeftOuterJoin:
		return p.handleLeftJoin(prop, false)
	case RightOuterJoin:
		return p.handleRightJoin(prop, false)
	default:
		lres0, lres1, count, err := p.handleLeftJoin(prop, true)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		rres0, rres1, _, err := p.handleRightJoin(prop, true)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if rres0.cost < lres0.cost {
			lres0 = rres0
		}
		if rres1.cost < lres1.cost {
			lres1 = rres1
		}
		return lres0, lres1, count, errors.Trace(err)
	}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Aggregation) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	_, res, cnt, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(nil)
	if len(prop) != 0 {
		return &physicalPlanInfo{cost: math.MaxFloat64}, addPlanToResponse(p, res), cnt / 3, errors.Trace(err)
	}
	res = addPlanToResponse(p, res)
	return res, res, cnt / 3, errors.Trace(err)
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *NewUnion) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	count := uint64(0)
	var res0Collection, res1Collection []*physicalPlanInfo
	for _, child := range p.GetChildren() {
		newProp := make(requiredProperty, 0, len(prop))
		for _, c := range prop {
			idx := p.GetSchema().GetIndex(c.col)
			newProp = append(newProp, &columnProp{col: child.GetSchema()[idx], desc: c.desc})
		}
		res0, res1, cnt, err := child.(LogicalPlan).convert2PhysicalPlan(newProp)
		count += cnt
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		res0Collection = append(res0Collection, res0)
		res1Collection = append(res1Collection, res1)
	}
	return p.matchProperty(prop, nil, res0Collection...), p.matchProperty(prop, nil, res1Collection...), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Selection) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	res0, res1, count, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if _, ok := p.GetChildByIndex(0).(*DataSource); ok {
		count = uint64(float64(count) * selectionFactor)
		return res0, res1, count, nil
	}
	return p.matchProperty(prop, nil, res0), p.matchProperty(prop, nil, res1), count / 3, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Projection) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
	res0, res1, count, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(newProp)
	if err != nil {
		return nil, nil, count, errors.Trace(err)
	}
	res1 = addPlanToResponse(p, res1)
	if !canPassSort {
		return &physicalPlanInfo{cost: math.MaxFloat64}, res1, count, nil
	}

	return addPlanToResponse(p, res0), res1, count, nil
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
	selfProp := make(requiredProperty, 0, len(p.ByItems))
	for _, by := range p.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			selfProp = append(selfProp, &columnProp{col: col, desc: by.Desc})
		} else {
			selfProp = nil
			break
		}
	}
	res0, res1, count, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(selfProp)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	cnt := float64(count)
	sortCost := cnt*math.Log2(cnt)*cpuFactor + memoryFactor*cnt
	if len(selfProp) == 0 {
		res0 = addPlanToResponse(p, res1)
	} else if sortCost+res1.cost < res0.cost {
		res0.cost = sortCost + res1.cost
		res0 = addPlanToResponse(p, res1)
	}
	if matchProp(prop, selfProp) {
		return res0, res0, count, nil
	}
	return &physicalPlanInfo{cost: math.MaxFloat64}, res0, count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Apply) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(np, res0), addPlanToResponse(np, res1), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Distinct) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), uint64(float64(count) * distinctFactor), nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *NewTableDual) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	res := &physicalPlanInfo{p: p, cost: 1.0}
	return res, res, 1, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *MaxOneRow) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Exists) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Trim) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *SelectLock) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Insert) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	if len(p.GetChildren()) == 0 {
		res := &physicalPlanInfo{p: p}
		return res, res, 0, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	res0, res1, count, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	return addPlanToResponse(p, res0), addPlanToResponse(p, res1), count, nil
}
