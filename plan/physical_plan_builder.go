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
	"github.com/ngaut/log"
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
	txn, err := p.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if sel, ok := p.GetParentByIndex(0).(*Selection); ok {
		log.Warnf("got a sel!")
		newSel := *sel
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		oldConditions = sel.Conditions
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.DeepCopy())
		}
		ts.AccessCondition, newSel.Conditions = detachTableScanConditions(conds, table)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if txn != nil {
			ts.ConditionPBExpr, newSel.Conditions, err = expressionsToPB(newSel.Conditions, txn.GetClient())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		err = buildTableRange(ts)
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Warnf("conds len %d", len(newSel.Conditions))
		if len(newSel.Conditions) > 0 {
			newSel.SetChildren(ts)
			resultPlan = &newSel
		}
	} else {
		ts.Ranges = []TableRange{{math.MinInt64, math.MaxInt64}}
	}
	if txn != nil && !txn.IsReadOnly() {
		us := &PhysicalUnionScan{
			Condition: expression.ComposeCNFCondition(oldConditions),
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
				return nil, errors.Trace(err)
			}
			rowCount += uint64(cnt)
		}
	}
	if ts.ConditionPBExpr != nil {
		rowCount = uint64(float64(rowCount) * selectionFactor)
	}
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: rowCount}), nil
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
	txn, err := p.ctx.GetTxn(false)
	if err != nil {
		return nil, errors.Trace(err)
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
		if err != nil {
			return nil, errors.Trace(err)
		}
		if txn != nil {
			is.ConditionPBExpr, newSel.Conditions, err = expressionsToPB(newSel.Conditions, txn.GetClient())
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		err = buildIndexRange(is)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, idxRange := range is.Ranges {
			cnt, err := getRowCountByIndexRange(statsTbl, idxRange, is.Index)
			if err != nil {
				return nil, errors.Trace(err)
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
	if txn != nil && !txn.IsReadOnly() {
		us := &PhysicalUnionScan{
			Condition: expression.ComposeCNFCondition(oldConditions),
		}
		us.SetChildren(resultPlan)
		resultPlan = us
	}
	is.DoubleRead = !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle)
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: rowCount}), nil
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
			return nil, errors.Trace(err)
		}
	}
	for _, index := range indices {
		indexInfo, err := p.handleIndexScan(prop, index)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if indexInfo.cost < info.cost {
			info = indexInfo
		}
	}
	return info, errors.Trace(p.storePlanInfo(prop, info))
}

func addPlanToResponse(p PhysicalPlan, planInfo *physicalPlanInfo) *physicalPlanInfo {
	np := p.Copy()
	np.SetChildren(planInfo.p)
	return &physicalPlanInfo{p: np, cost: planInfo.cost, count: planInfo.count}
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Limit) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if info != nil {
		return info, nil
	}
	info, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	count := p.Offset + p.Count
	if count < info.count {
		count = p.Offset + p.Count
	}
	info = addPlanToResponse(p, info)
	info.count = count
	p.storePlanInfo(prop, info)
	return info, nil
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
		prop = &requiredProperty{}
	}
	lInfo, err := lChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !allLeft {
		lInfo.cost = math.MaxFloat64
	}
	rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	sortedPlanInfo := join.matchProperty(prop, lInfo, rInfo)

	return sortedPlanInfo, nil
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
		return nil, errors.Trace(err)
	}
	if !allRight {
		prop = &requiredProperty{}
	}
	rInfo, err := rChild.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !allRight {
		rInfo.cost = math.MaxFloat64
	}
	sortedPlanInfo := join.matchProperty(prop, lInfo, rInfo)
	return sortedPlanInfo, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Join) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	switch p.JoinType {
	case SemiJoin, SemiJoinWithAux:
		lChild := p.GetChildByIndex(0).(LogicalPlan)
		rChild := p.GetChildByIndex(1).(LogicalPlan)
		allLeft := true
		for _, col := range prop.props {
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
			prop = &requiredProperty{}
		}
		lInfo, err := lChild.convert2PhysicalPlan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		resultInfo := join.matchProperty(prop, lInfo, rInfo)
		if p.JoinType == SemiJoin {
			resultInfo.count = uint64(float64(lInfo.count) * selectionFactor)
		} else {
			resultInfo.count = lInfo.count
		}
		if !allLeft {
			resultInfo.cost = math.MaxFloat64
		}
		p.storePlanInfo(prop, resultInfo)
		return resultInfo, nil
	case LeftOuterJoin:
		info, err := p.handleLeftJoin(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.storePlanInfo(prop, info)
		return info, nil
	case RightOuterJoin:
		info, err := p.handleRightJoin(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		p.storePlanInfo(prop, info)
		return info, nil
	default:
		lInfo, err := p.handleLeftJoin(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rInfo, err := p.handleRightJoin(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rInfo.cost < lInfo.cost {
			lInfo = rInfo
		}
		p.storePlanInfo(prop, lInfo)
		return lInfo, nil
	}
}

func (p *Aggregation) handleStreamAgg(prop *requiredProperty) (*physicalPlanInfo, error) {
	agg := &PhysicalAggregation{
		AggType:      StreamedAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}
	agg.HasGby = len(p.GroupByItems) > 0
	agg.SetSchema(p.schema)
	isDistinct := false
	gbyCols := make([]*expression.Column, 0, len(p.GroupByItems)+1)
	info := &physicalPlanInfo{cost: math.MaxFloat64}
	// TODO: extract columns in monotonic functions.
	for _, item := range p.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			if !col.Correlated {
				gbyCols = append(gbyCols, col)
			}
		} else {
			// group by a + b is not interested in any orders.
			return info, nil
		}
	}
	for _, fun := range p.AggFuncs {
		if fun.IsDistinct() {
			if isDistinct {
				// sum(distinct a), count(distinct b) is not interested in any orders.
				return info, nil
			}
			for _, arg := range fun.GetArgs() {
				if col, ok := arg.(*expression.Column); ok {
					gbyCols = append(gbyCols, col)
				} else {
					// sum(distinct a + b) is not interested in any orders.
					return info, nil
				}
			}
		}
	}
	isSortKey := make([]bool, len(gbyCols))
	newProp := &requiredProperty{
		props: make([]*columnProp, 0, len(gbyCols)),
	}
	for _, pro := range prop.props {
		isMatch := false
		for i, col := range gbyCols {
			if col.ColName.L == pro.col.ColName.L {
				isSortKey[i] = true
				isMatch = true
			}
		}
		if !isMatch {
			return info, nil
		} else {
			newProp.props = append(newProp.props, pro)
		}
	}
	newProp.sortKeyLen = len(newProp.props)
	for i, col := range gbyCols {
		if !isSortKey[i] {
			newProp.props = append(newProp.props, &columnProp{col: col})
		}
	}
	childInfo, err := p.children[0].(LogicalPlan).convert2PhysicalPlan(newProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = addPlanToResponse(agg, childInfo)
	info.cost += float64(info.count) * cpuFactor
	info.count = info.count / 3
	return info, nil
}

func (p *Aggregation) handleFinalAgg(x physicalXPlan, childInfo *physicalPlanInfo) (*physicalPlanInfo, error) {
	agg := &PhysicalAggregation{
		AggType:      FinalAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}
	agg.SetSchema(p.schema)
	log.Warnf("handle final")
	agg.HasGby = len(p.GroupByItems) > 0
	schema, err := x.addAggregation(agg, p.ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(schema) == 0 {
		return nil, nil
	}
	x.(PhysicalPlan).SetSchema(schema)
	info := addPlanToResponse(agg, childInfo)
	info.count = info.count / 3
	// if we build a final agg, it must be the best plan.
	info.cost = 0
	return info, nil
}

func (p *Aggregation) handleHashAgg() (*physicalPlanInfo, error) {
	childInfo, err := p.children[0].(LogicalPlan).convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	distinct := false
	for _, fun := range p.AggFuncs {
		if fun.IsDistinct() {
			distinct = true
			break
		}
	}
	if !distinct {
		switch x := childInfo.p.(type) {
		case *PhysicalTableScan:
			info, err := p.handleFinalAgg(x, childInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if info != nil {
				return info, nil
			}
		case *PhysicalIndexScan:
			info, err := p.handleFinalAgg(x, childInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if info != nil {
				return info, nil
			}
		}
	}
	agg := &PhysicalAggregation{
		AggType:      CompleteAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}
	log.Warnf("handle complete %v %v %v", agg.AggType, CompleteAgg, FinalAgg)
	agg.HasGby = len(p.GroupByItems) > 0
	agg.SetSchema(p.schema)
	info := addPlanToResponse(agg, childInfo)
	info.cost += float64(info.count) * memoryFactor
	info.count = info.count / 3
	return info, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Aggregation) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	planInfo, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if planInfo != nil {
		return planInfo, nil
	}
	planInfo, err = p.handleHashAgg()
	if err != nil {
		return nil, errors.Trace(err)
	}
	streamInfo, err := p.handleStreamAgg(prop)
	if streamInfo.cost < planInfo.cost {
		planInfo = streamInfo
	}
	err = p.storePlanInfo(prop, planInfo)
	return planInfo, errors.Trace(err)
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Union) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	childInfos := make([]*physicalPlanInfo, 0, len(p.children))
	var count uint64
	for _, child := range p.GetChildren() {
		newProp := &requiredProperty{
			props:      make([]*columnProp, 0, len(prop.props)),
			sortKeyLen: prop.sortKeyLen,
		}
		for _, c := range prop.props {
			idx := p.GetSchema().GetIndex(c.col)
			newProp.props = append(newProp.props, &columnProp{col: child.GetSchema()[idx], desc: c.desc})
		}
		info, err = child.(LogicalPlan).convert2PhysicalPlan(newProp)
		count += info.count
		if err != nil {
			return nil, errors.Trace(err)
		}
		childInfos = append(childInfos, info)
	}
	info = p.matchProperty(prop, childInfos...)
	info.count = count
	p.storePlanInfo(prop, info)
	return info, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Selection) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	var err error
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	info, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, ok := p.GetChildByIndex(0).(*DataSource); ok {
		return info, nil
	}
	info = p.matchProperty(prop, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Projection) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	newProp := &requiredProperty{
		props:      make([]*columnProp, 0, len(prop.props)),
		sortKeyLen: prop.sortKeyLen}
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
				newProp.props = append(newProp.props, &columnProp{col: v, desc: c.desc})
			}
		case *expression.ScalarFunction:
			newProp = nil
			canPassSort = false
			break loop
		default:
			newProp.sortKeyLen--
		}
	}
	if !canPassSort {
		return &physicalPlanInfo{cost: math.MaxFloat64}, nil
	}
	info, err = p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(newProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = addPlanToResponse(p, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

func matchProp(target, new *requiredProperty) bool {
	if target.sortKeyLen > len(new.props) {
		return false
	}
	for i := 0; i < target.sortKeyLen; i++ {
		if !target.props[i].equal(new.props[i]) {
			return false
		}
	}
	for i := target.sortKeyLen; i < len(target.props); i++ {
		isMatch := false
		for _, pro := range new.props {
			if pro.col.Equal(target.props[i].col) {
				isMatch = true
				break
			}
		}
		if !isMatch {
			return false
		}
	}
	return true
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Sort) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	selfProp := &requiredProperty{
		props: make([]*columnProp, 0, len(p.ByItems)),
	}
	for _, by := range p.ByItems {
		if col, ok := by.Expr.(*expression.Column); ok {
			selfProp.props = append(selfProp.props, &columnProp{col: col, desc: by.Desc})
		} else {
			selfProp.props = nil
			break
		}
	}
	selfProp.sortKeyLen = len(selfProp.props)
	sortedPlanInfo, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(selfProp)
	log.Warnf("sort plan %v cnt %v", sortedPlanInfo.cost, sortedPlanInfo.count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	unSortedPlanInfo, err := p.GetChildByIndex(0).(LogicalPlan).convert2PhysicalPlan(&requiredProperty{})
	log.Warnf("unsort plan %v cnt %v", unSortedPlanInfo.cost, unSortedPlanInfo.count)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cnt := float64(unSortedPlanInfo.count)
	sortCost := cnt*math.Log2(cnt)*cpuFactor + memoryFactor*cnt
	if len(selfProp.props) == 0 {
		sortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	} else if sortCost+unSortedPlanInfo.cost < sortedPlanInfo.cost {
		sortedPlanInfo.cost = sortCost + unSortedPlanInfo.cost
		sortedPlanInfo = addPlanToResponse(p, unSortedPlanInfo)
	}
	if !matchProp(prop, selfProp) {
		sortedPlanInfo.cost = math.MaxFloat64
	}
	p.storePlanInfo(prop, sortedPlanInfo)
	return sortedPlanInfo, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
func (p *Apply) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return info, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	innerInfo, err := p.InnerPlan.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	np := &PhysicalApply{
		OuterSchema: p.OuterSchema,
		Checker:     p.Checker,
		InnerPlan:   innerInfo.p,
	}
	np.SetSchema(p.GetSchema())
	info, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = addPlanToResponse(np, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

// convert2PhysicalPlan implements LogicalPlan convert2PhysicalPlan interface.
// TODO: support streaming distinct.
func (p *Distinct) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	child := p.GetChildByIndex(0).(LogicalPlan)
	info, err = child.convert2PhysicalPlan(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = addPlanToResponse(p, info)
	info.count = uint64(float64(info.count) * distinctFactor)
	p.storePlanInfo(prop, info)
	return info, nil
}
