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

// JoinConcurrency means the number of goroutines that participate joining.
var JoinConcurrency = 5

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
			rowCount, err = table.Columns[offset].EqualRowCount(types.Datum{})
			if r.Kind() == types.KindMaxValue {
				rowCount = table.Count - rowCount
			} else if err == nil {
				lessCount, err1 := table.Columns[offset].LessRowCount(r)
				rowCount = lessCount - rowCount
				err = err1
			}
		} else if r.Kind() == types.KindMaxValue {
			rowCount, err = table.Columns[offset].GreaterRowCount(l)
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

func (p *DataSource) handleTableScan(prop *requiredProperty) (*physicalPlanInfo, error) {
	table := p.Table
	var resultPlan PhysicalPlan
	ts := &PhysicalTableScan{
		ctx:         p.ctx,
		Table:       p.Table,
		Columns:     p.Columns,
		TableAsName: p.TableAsName,
		DBName:      p.DBName,
	}
	ts.SetSchema(p.GetSchema())
	resultPlan = ts
	var oldConditions []expression.Expression
	txn, err := p.ctx.GetTxn()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if sel, ok := p.GetParentByIndex(0).(*Selection); ok {
		newSel := *sel
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		oldConditions = sel.Conditions
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.DeepCopy())
		}
		ts.AccessCondition, newSel.Conditions = detachTableScanConditions(conds, table)
		ts.conditionPBExpr, newSel.Conditions, err = expressionsToPB(newSel.Conditions, txn.GetClient())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		err = buildTableRange(ts)
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
	if !txn.IsReadOnly() {
		us := &PhysicalUnionScan{
			table:     p.table,
			desc:      p.Desc,
			condition: oldConditions,
		}
		us.SetChildren(resultPlan)
		resultPlan = us
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
				cnt, err = statsTbl.Columns[offset].GreaterRowCount(types.NewDatum(rg.LowVal))
			} else {
				cnt, err = statsTbl.Columns[offset].BetweenRowCount(types.NewDatum(rg.LowVal), types.NewDatum(rg.HighVal))
			}
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			rowCount += uint64(cnt)
		}
	}
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: &rowCount}), nil
}

func (p *DataSource) handleIndexScan(prop *requiredProperty, index *model.IndexInfo) (*physicalPlanInfo, error) {
	statsTbl := p.statisticTable
	var resultPlan PhysicalPlan
	is := &PhysicalIndexScan{
		ctx:         p.ctx,
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
	txn, err := p.ctx.GetTxn()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	var oldConditions []expression.Expression
	if sel, ok := p.GetParentByIndex(0).(*Selection); ok {
		rowCount = 0
		newSel := *sel
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.DeepCopy())
		}
		oldConditions = sel.Conditions
		is.AccessCondition, newSel.Conditions = detachIndexScanConditions(conds, is)
		is.conditionPBExpr, newSel.Conditions, err = expressionsToPB(newSel.Conditions, txn.GetClient())
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		err = buildIndexRange(is)
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
	if !txn.IsReadOnly() {
		us := &PhysicalUnionScan{
			table:     p.table,
			desc:      p.Desc,
			condition: oldConditions,
		}
		us.SetChildren(resultPlan)
		resultPlan = us
	}
	is.DoubleRead = !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle)
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: &rowCount}), nil
}

func isCoveringIndex(columns []*model.ColumnInfo, indexColumns []*model.IndexColumn, pkIsHandle bool) bool {
	for _, colInfo := range columns {
		if pkIsHandle && mysql.HasPriKeyFlag(colInfo.Flag) {
			continue
		}
		isIndexColumn := false
		for _, indexCol := range indexColumns {
			if colInfo.Name.L == indexCol.Name.L && indexCol.Length == types.UnspecifiedLength {
				isIndexColumn = true
				break
			}
		}
		if !isIndexColumn {
			return false
		}
	}
	return true
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *DataSource) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	sel, isSel := p.GetParentByIndex(0).(*Selection)
	if isSel {
		for _, cond := range sel.Conditions {
			if con, ok := cond.(*expression.Constant); ok {
				result, err := expression.EvalBool(con, nil, nil)
				if err != nil {
					return nil, errors.Trace(err)
				}
				if !result {
					dummy := &PhysicalDummyScan{}
					dummy.SetSchema(p.schema)
					info := &physicalPlanInfo{p: dummy}
					p.storePlanInfo(prop, info)
					return info, nil
				}
			}
		}
	}
	indices, includeTableScan := availableIndices(p.table)
	if includeTableScan {
		info, err = p.handleTableScan(prop)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
	}
	for _, index := range indices {
		indexInfo, err := p.handleIndexScan(prop, index)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		if indexInfo.cost < info.cost {
			info = indexInfo
		}
	}
	p.storePlanInfo(prop, info)
	return info, nil
}

func addPlanToResponse(p PhysicalPlan, planInfo *physicalPlanInfo) *physicalPlanInfo {
	np := p.Copy()
	np.SetChildren(planInfo.p)
	return &physicalPlanInfo{p: np, cost: planInfo.cost}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Limit) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if info != nil {
		return info, nil
	}
	info, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	count := p.Offset+p.Count
	if count < *info.count {
		count = p.Offset + p.Count
	}
	info = addPlanToResponse(p, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

func estimateJoinCount(lc uint64, rc uint64) uint64 {
	return lc * rc / 3
}

func (p *Join) handleLeftJoin(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	lChild := p.GetChildByIndex(0).(LogicalPlan)
	rChild := p.GetChildByIndex(1).(LogicalPlan)
	allLeft := true
	for _, col := range prop.props {
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
		// TODO: decide concurrency by data size.
		Concurrency: JoinConcurrency,
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
	lInfo, err := lChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allLeft {
		lInfo.cost = math.MaxFloat64
	}
	rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	sortedPlanInfo := join.matchProperty(prop, lInfo, rInfo)
	count := estimateJoinCount(*lInfo.count, *rInfo.count)
	return sortedPlanInfo, , nil
}

func (p *Join) handleRightJoin(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	lChild := p.GetChildByIndex(0).(LogicalPlan)
	rChild := p.GetChildByIndex(1).(LogicalPlan)
	allRight := true
	for _, col := range prop.props {
		if rChild.GetSchema().GetIndex(col.col) == -1 {
			allRight = false
		}
	}
	join := &PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		// TODO: decide concurrency by data size.
		Concurrency: JoinConcurrency,
	}
	join.SetSchema(p.schema)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = RightOuterJoin
	}
	lInfo, err := lChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		prop = nil
	}
	rInfo, err := rChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	}
	if !allRight {
		rInfo.cost = math.MaxFloat64
	}
	sortedPlanInfo := join.matchProperty(prop, lInfo, rInfo)
	return sortedPlanInfo, estimateJoinCount(lCount, rCount), nil
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
		lInfo, err := lChild.convert2PhysicalPlan(prop)
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
		if err != nil {
			return nil, nil, 0, errors.Trace(err)
		}
		sortedPlanInfo := join.matchProperty(prop, lInfo, rInfo)
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
func (p *Aggregation) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	_, planInfo, cnt := p.getPlanInfo(prop)
	if planInfo != nil {
		return planInfo, planInfo, cnt, nil
	}
	var streamedPlanInfo, hashedPlanInfo, matchPropPlanInfo, unSortedPlanInfo *physicalPlanInfo
	streamedPlanInfo, matchPropPlanInfo, cnt, err = p.handleStreamedAgg(prop)
	if err != nil {
		return nil, nil, cnt, errors.Trace(err)
	}
	hashedPlanInfo, cnt, err = p.handleHashedAgg()
	if err != nil {
		return nil, nil, cnt, errors.Trace(err)
	}
	if hashedPlanInfo.cost > streamedPlanInfo.cost {
		unSortedPlanInfo = streamedPlanInfo
	} else {
		unSortedPlanInfo = hashedPlanInfo
	}
	p.storePlanInfo(prop, matchPropPlanInfo, unSortedPlanInfo, cnt/3)
	return matchPropPlanInfo, unSortedPlanInfo, cnt / 3, errors.Trace(err)
}

func (p *Aggregation) handleSortedPlan(prop *requiredProperty) (*physicalPlanInfo, uint64, error) {
	child := p.children[0].(LogicalPlan)
	sortedPropPlanInfo, unsortedPlanInfo, cnt, err := child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	unsortedPlanInfo = addSortToPlanInfo(unsortedPlanInfo, cnt, prop)
	if unsortedPlanInfo.cost < sortedPropPlanInfo.cost {
		sortedPropPlanInfo = unsortedPlanInfo
	}
	streamed := &PhysicalAggregation{AggFuncs: p.AggFuncs, GroupByItems: p.GroupByItems, streamed: true}
	streamed.SetChildren(sortedPropPlanInfo.p)
	sortedPropPlanInfo.p = streamed
	return sortedPropPlanInfo, cnt, nil
}

func (p *Aggregation) handleStreamedAgg(prop *requiredProperty) (streamedPlanInfo *physicalPlanInfo, matchPropPlanInfo *physicalPlanInfo, cnt uint64, err error) {
	for _, item := range p.GroupByItems {
		if _, ok := item.(*expression.Column); !ok {
			info := &physicalPlanInfo{
				cost: math.MaxFloat64,
			}
			return info, info, cnt, nil
		}
	}
	matched := true
	if len(p.GroupByItems) < len(prop.props) {
		matched = false
	} else {
		for i, property := range prop.props {
			col := p.GroupByItems[i].(*expression.Column)
			if property.col.FromID != col.FromID || property.col.Index != col.Index {
				matched = false
				break
			}
		}
	}
	if !matched {
		matchPropPlanInfo = &physicalPlanInfo{
			cost: math.MaxFloat64,
		}
	} else {
		matchPropPlanInfo, cnt, err = p.handleSortedPlan(prop)
		if err != nil {
			return nil, nil, cnt, errors.Trace(err)
		}
	}
	newProp := make(requiredProperty, 0, len(p.GroupByItems))
	for _, item := range p.GroupByItems {
		newProp = append(newProp, &columnProp{col: item.(*expression.Column)})
	}
	streamedPlanInfo, cnt, err = p.handleSortedPlan(newProp)
	err = errors.Trace(err)
	return
}

func (p *Aggregation) handleHashedAgg() (*physicalPlanInfo, uint64, error) {
	child := p.children[0].(LogicalPlan)
	_, unsortedPlanInfo, cnt, err := child.convert2PhysicalPlan(nil)
	switch x := unsortedPlanInfo.p.(type) {
	case *PhysicalTableScan:
		x.addAggregation(p, x.ctx)
	case *PhysicalIndexScan:
		x.addAggregation(p, x.ctx)
	}
	unsortedPlanInfo.cost += cnt * memoryFactor
	agg := PhysicalAggregation{
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}
	agg.SetChildren(unsortedPlanInfo.p)
	unsortedPlanInfo.p = agg
	return unsortedPlanInfo, cnt, errors.Trace(err)
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Union) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
func (p *Selection) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
func (p *Projection) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	var err error
	sortedPlanInfo, unSortedPlanInfo, count := p.getPlanInfo(prop)
	if sortedPlanInfo != nil {
		return sortedPlanInfo, unSortedPlanInfo, count, nil
	}
	newProp := make([]*columnProp, 0, len(prop.props))
	childSchema := p.GetChildByIndex(0).GetSchema()
	usedCols := make([]bool, len(childSchema))
	canPassSort := true
loop:
	for _, c := range prop.props {
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
func (p *Sort) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
func (p *Apply) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
func (p *Distinct) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
func (p *TableDual) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
	planInfo := &physicalPlanInfo{p: p, cost: 1.0}
	return planInfo, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *MaxOneRow) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Update) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Delete) convert2PhysicalPlan(prop requiredProperty) (*physicalPlanInfo, *physicalPlanInfo, uint64, error) {
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
