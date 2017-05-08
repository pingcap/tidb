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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

const (
	netWorkFactor   = 1.5
	scanFactor      = 2.0
	descScanFactor  = 5 * scanFactor
	memoryFactor    = 5.0
	selectionFactor = 0.8
	cpuFactor       = 0.9
	aggFactor       = 0.1
	joinFactor      = 0.3
)

// JoinConcurrency means the number of goroutines that participate in joining.
var JoinConcurrency = 5

func (p *DataSource) convert2TableScan(prop *requiredProperty) (*physicalPlanInfo, error) {
	client := p.ctx.GetClient()
	ts := PhysicalTableScan{
		Table:               p.tableInfo,
		Columns:             p.Columns,
		TableAsName:         p.TableAsName,
		DBName:              p.DBName,
		physicalTableSource: physicalTableSource{client: client},
	}.init(p.allocator, p.ctx)
	ts.SetSchema(p.Schema())
	if p.ctx.Txn() != nil {
		ts.readOnly = p.ctx.Txn().IsReadOnly()
	} else {
		ts.readOnly = true
	}

	var resultPlan PhysicalPlan
	resultPlan = ts
	table := p.tableInfo
	sc := p.ctx.GetSessionVars().StmtCtx
	if sel, ok := p.parents[0].(*Selection); ok {
		newSel := sel.Copy().(*Selection)
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.Clone())
		}
		ts.AccessCondition, newSel.Conditions = DetachTableScanConditions(conds, table)
		ts.TableConditionPBExpr, ts.tableFilterConditions, newSel.Conditions =
			expression.ExpressionsToPB(sc, newSel.Conditions, client)
		ranges, err := BuildTableRange(ts.AccessCondition, sc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ts.Ranges = ranges
		if len(newSel.Conditions) > 0 {
			newSel.SetChildren(ts)
			newSel.onTable = true
			resultPlan = newSel
		}
	} else {
		ts.Ranges = []types.IntColumnRange{{math.MinInt64, math.MaxInt64}}
	}
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	if table.PKIsHandle {
		for i, colInfo := range ts.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				ts.pkCol = p.Schema().Columns[i]
				var err error
				rowCount, err = statsTbl.GetRowCountByIntColumnRanges(sc, ts.pkCol.ID, ts.Ranges)
				if err != nil {
					return nil, errors.Trace(err)
				}
				break
			}
		}
	}
	if ts.TableConditionPBExpr != nil {
		rowCount = rowCount * selectionFactor
	}
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: rowCount, reliable: !statsTbl.Pseudo}), nil
}

func (p *DataSource) convert2IndexScan(prop *requiredProperty, index *model.IndexInfo) (*physicalPlanInfo, error) {
	client := p.ctx.GetClient()
	is := PhysicalIndexScan{
		Index:               index,
		Table:               p.tableInfo,
		Columns:             p.Columns,
		TableAsName:         p.TableAsName,
		OutOfOrder:          true,
		DBName:              p.DBName,
		physicalTableSource: physicalTableSource{client: client},
	}.init(p.allocator, p.ctx)
	is.SetSchema(p.schema)
	if p.ctx.Txn() != nil {
		is.readOnly = p.ctx.Txn().IsReadOnly()
	} else {
		is.readOnly = true
	}

	var resultPlan PhysicalPlan
	resultPlan = is
	statsTbl := p.statisticTable
	rowCount := float64(statsTbl.Count)
	sc := p.ctx.GetSessionVars().StmtCtx
	if sel, ok := p.parents[0].(*Selection); ok {
		newSel := sel.Copy().(*Selection)
		conds := make([]expression.Expression, 0, len(sel.Conditions))
		for _, cond := range sel.Conditions {
			conds = append(conds, cond.Clone())
		}
		is.AccessCondition, newSel.Conditions, is.accessEqualCount, is.accessInAndEqCount = DetachIndexScanConditions(conds, is.Index)
		memDB := infoschema.IsMemoryDB(p.DBName.L)
		isDistReq := !memDB && client != nil && client.SupportRequestType(kv.ReqTypeIndex, 0)
		if isDistReq {
			idxConds, tblConds := DetachIndexFilterConditions(newSel.Conditions, is.Index.Columns, is.Table)
			is.IndexConditionPBExpr, is.indexFilterConditions, idxConds = expression.ExpressionsToPB(sc, idxConds, client)
			is.TableConditionPBExpr, is.tableFilterConditions, tblConds = expression.ExpressionsToPB(sc, tblConds, client)
			newSel.Conditions = append(idxConds, tblConds...)
		}
		var err error
		is.Ranges, err = BuildIndexRange(p.ctx.GetSessionVars().StmtCtx, is.Table, is.Index, is.accessInAndEqCount, is.AccessCondition)
		if err != nil {
			if !terror.ErrorEqual(err, types.ErrTruncated) {
				return nil, errors.Trace(err)
			}
			log.Warn("truncate error in buildIndexRange")
		}
		rowCount, err = statsTbl.GetRowCountByIndexRanges(sc, is.Index.ID, is.Ranges, is.accessInAndEqCount)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(newSel.Conditions) > 0 {
			newSel.SetChildren(is)
			newSel.onTable = true
			resultPlan = newSel
		}
	} else {
		rb := rangeBuilder{sc: p.ctx.GetSessionVars().StmtCtx}
		is.Ranges = rb.buildIndexRanges(fullRange, types.NewFieldType(mysql.TypeNull))
	}
	is.DoubleRead = !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle)
	return resultPlan.matchProperty(prop, &physicalPlanInfo{count: rowCount, reliable: !statsTbl.Pseudo}), nil
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

func (p *DataSource) need2ConsiderIndex(prop *requiredProperty) bool {
	if _, ok := p.parents[0].(*Selection); ok || len(prop.props) > 0 {
		return true
	}
	return false
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
// If there is no index that matches the required property, the returned physicalPlanInfo
// will be table scan and has the cost of MaxInt64. But this can be ignored because the parent will call
// convert2PhysicalPlan again with an empty *requiredProperty, so the plan with the lowest
// cost will be chosen.
func (p *DataSource) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	info, err = p.tryToConvert2DummyScan(prop)
	if info != nil || err != nil {
		return info, errors.Trace(err)
	}
	client := p.ctx.GetClient()
	memDB := infoschema.IsMemoryDB(p.DBName.L)
	isDistReq := !memDB && client != nil && client.SupportRequestType(kv.ReqTypeSelect, 0)
	if !isDistReq {
		memTable := PhysicalMemTable{
			DBName:      p.DBName,
			Table:       p.tableInfo,
			Columns:     p.Columns,
			TableAsName: p.TableAsName,
		}.init(p.allocator, p.ctx)
		memTable.SetSchema(p.schema)
		rb := &rangeBuilder{sc: p.ctx.GetSessionVars().StmtCtx}
		memTable.Ranges = rb.buildTableRanges(fullRange)
		info = &physicalPlanInfo{p: memTable}
		info = enforceProperty(prop, info)
		p.storePlanInfo(prop, info)
		return info, nil
	}
	indices, includeTableScan := availableIndices(p.indexHints, p.tableInfo)
	if includeTableScan {
		info, err = p.convert2TableScan(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if !includeTableScan || p.need2ConsiderIndex(prop) {
		for _, index := range indices {
			indexInfo, err := p.convert2IndexScan(prop, index)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if info == nil || indexInfo.cost < info.cost {
				info = indexInfo
			}
		}
	}
	return info, errors.Trace(p.storePlanInfo(prop, info))
}

// tryToConvert2DummyScan is an optimization which checks if its parent is a selection with a constant condition
// that evaluates to false. If it is, there is no need for a real physical scan, a dummy scan will do.
func (p *DataSource) tryToConvert2DummyScan(prop *requiredProperty) (*physicalPlanInfo, error) {
	sel, isSel := p.parents[0].(*Selection)
	if !isSel {
		return nil, nil
	}

	for _, cond := range sel.Conditions {
		if con, ok := cond.(*expression.Constant); ok {
			result, err := expression.EvalBool([]expression.Expression{con}, nil, p.ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !result {
				dual := TableDual{}.init(p.allocator, p.ctx)
				dual.SetSchema(p.schema)
				info := &physicalPlanInfo{p: dual}
				p.storePlanInfo(prop, info)
				return info, nil
			}
		}
	}
	return nil, nil
}

// addPlanToResponse creates a *physicalPlanInfo that adds p as the parent of info.
func addPlanToResponse(parent PhysicalPlan, info *physicalPlanInfo) *physicalPlanInfo {
	np := parent.Copy()
	np.SetChildren(info.p)
	ret := &physicalPlanInfo{p: np, cost: info.cost, count: info.count, reliable: info.reliable}
	if _, ok := parent.(*MaxOneRow); ok {
		ret.count = 1
		ret.reliable = true
	}
	return ret
}

// enforceProperty creates a *physicalPlanInfo that satisfies the required property by adding
// sort or limit as the parent of the given physical plan.
func enforceProperty(prop *requiredProperty, info *physicalPlanInfo) *physicalPlanInfo {
	if info.p == nil {
		return info
	}
	if len(prop.props) != 0 {
		items := make([]*ByItems, 0, len(prop.props))
		for _, col := range prop.props {
			items = append(items, &ByItems{Expr: col.col, Desc: col.desc})
		}
		sort := Sort{
			ByItems:   items,
			ExecLimit: prop.limit,
		}.init(info.p.Allocator(), info.p.context())
		sort.SetSchema(info.p.Schema())
		info = addPlanToResponse(sort, info)

		count := info.count
		if prop.limit != nil {
			count = float64(prop.limit.Offset + prop.limit.Count)
			info.reliable = true
		}
		info.cost += sortCost(count)
	} else if prop.limit != nil {
		limit := Limit{Offset: prop.limit.Offset, Count: prop.limit.Count}.init(info.p.Allocator(), info.p.context())
		limit.SetSchema(info.p.Schema())
		info = addPlanToResponse(limit, info)
		info.reliable = true
	}
	if prop.limit != nil && float64(prop.limit.Count) < info.count {
		info.count = float64(prop.limit.Count)
	}
	return info
}

func sortCost(cnt float64) float64 {
	if cnt == 0 {
		// If cnt is 0, the log(cnt) will be NAN.
		return 0.0
	}
	return cnt*math.Log2(float64(cnt))*cpuFactor + memoryFactor*float64(cnt)
}

// removeLimit removes the limit from prop.
func removeLimit(prop *requiredProperty) *requiredProperty {
	ret := &requiredProperty{
		props:      prop.props,
		sortKeyLen: prop.sortKeyLen,
	}
	return ret
}

func removeSortOrder(prop *requiredProperty) *requiredProperty {
	ret := &requiredProperty{
		limit: prop.limit,
	}
	return ret
}

// convertLimitOffsetToCount changes the limit(offset, count) in prop to limit(0, offset + count).
func convertLimitOffsetToCount(prop *requiredProperty) *requiredProperty {
	ret := &requiredProperty{
		props:      prop.props,
		sortKeyLen: prop.sortKeyLen,
	}
	if prop.limit != nil {
		ret.limit = &Limit{
			Count: prop.limit.Offset + prop.limit.Count,
		}
	}
	return ret
}

func limitProperty(limit *Limit) *requiredProperty {
	return &requiredProperty{limit: limit}
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *Limit) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	info, err = p.children[0].(LogicalPlan).convert2PhysicalPlan(limitProperty(&Limit{Offset: p.Offset, Count: p.Count}))
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = enforceProperty(prop, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

// convert2PhysicalPlanSemi converts the semi join to *physicalPlanInfo.
func (p *LogicalJoin) convert2PhysicalPlanSemi(prop *requiredProperty) (*physicalPlanInfo, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	allLeft := true
	for _, col := range prop.props {
		if !lChild.Schema().Contains(col.col) {
			allLeft = false
		}
	}
	join := PhysicalHashSemiJoin{
		WithAux:         LeftOuterSemiJoin == p.JoinType,
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		Anti:            p.anti,
	}.init(p.allocator, p.ctx)
	join.SetSchema(p.schema)
	lProp := prop
	if !allLeft {
		lProp = &requiredProperty{}
	}
	if p.JoinType == SemiJoin {
		lProp = removeLimit(lProp)
	}
	lInfo, err := lChild.convert2PhysicalPlan(lProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	resultInfo := join.matchProperty(prop, lInfo, rInfo)
	if p.JoinType == SemiJoin {
		resultInfo.count = lInfo.count * selectionFactor
	} else {
		resultInfo.count = lInfo.count
	}
	if !allLeft {
		resultInfo = enforceProperty(prop, resultInfo)
	} else if p.JoinType == SemiJoin {
		resultInfo = enforceProperty(limitProperty(prop.limit), resultInfo)
	}
	return resultInfo, nil
}

// convert2PhysicalPlanLeft converts the left join to *physicalPlanInfo.
func (p *LogicalJoin) convert2PhysicalPlanLeft(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	allLeft := true
	for _, col := range prop.props {
		if !lChild.Schema().Contains(col.col) {
			allLeft = false
		}
	}
	join := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		SmallTable:      1,
		// TODO: decide concurrency by data size.
		Concurrency:   JoinConcurrency,
		DefaultValues: p.DefaultValues,
	}.init(p.allocator, p.ctx)
	join.SetSchema(p.schema)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = LeftOuterJoin
	}
	lProp := prop
	if !allLeft {
		lProp = &requiredProperty{}
	}
	var lInfo *physicalPlanInfo
	var err error
	if innerJoin {
		lInfo, err = lChild.convert2PhysicalPlan(removeLimit(lProp))
	} else {
		lInfo, err = lChild.convert2PhysicalPlan(convertLimitOffsetToCount(lProp))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	rInfo, err := rChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	resultInfo := join.matchProperty(prop, lInfo, rInfo)
	if !allLeft {
		resultInfo = enforceProperty(prop, resultInfo)
	} else {
		resultInfo = enforceProperty(limitProperty(prop.limit), resultInfo)
	}
	return resultInfo, nil
}

// replaceColsInPropBySchema replaces the columns in original prop with the columns in schema.
func replaceColsInPropBySchema(prop *requiredProperty, schema *expression.Schema) *requiredProperty {
	newProps := make([]*columnProp, 0, len(prop.props))
	for _, p := range prop.props {
		idx := schema.ColumnIndex(p.col)
		if idx == -1 {
			log.Errorf("Can't find column %s in schema", p.col)
		}
		newProps = append(newProps, &columnProp{col: schema.Columns[idx], desc: p.desc})
	}
	return &requiredProperty{
		props:      newProps,
		sortKeyLen: prop.sortKeyLen,
		limit:      prop.limit,
	}
}

// convert2PhysicalPlanRight converts the right join to *physicalPlanInfo.
func (p *LogicalJoin) convert2PhysicalPlanRight(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)
	allRight := true
	for _, col := range prop.props {
		if !rChild.Schema().Contains(col.col) {
			allRight = false
		}
	}
	join := PhysicalHashJoin{
		EqualConditions: p.EqualConditions,
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: p.OtherConditions,
		// TODO: decide concurrency by data size.
		Concurrency:   JoinConcurrency,
		DefaultValues: p.DefaultValues,
	}.init(p.allocator, p.ctx)
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
	rProp := prop
	if !allRight {
		rProp = &requiredProperty{}
	} else {
		rProp = replaceColsInPropBySchema(rProp, rChild.Schema())
	}
	var rInfo *physicalPlanInfo
	if innerJoin {
		rInfo, err = rChild.convert2PhysicalPlan(removeLimit(rProp))
	} else {
		rInfo, err = rChild.convert2PhysicalPlan(convertLimitOffsetToCount(rProp))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	resultInfo := join.matchProperty(prop, lInfo, rInfo)
	if !allRight {
		resultInfo = enforceProperty(prop, resultInfo)
	} else {
		resultInfo = enforceProperty(limitProperty(prop.limit), resultInfo)
	}
	return resultInfo, nil
}

// buildSelectionWithConds will build a selection use the conditions of join and convert one side's column to correlated column.
// If the inner side is one selection then one data source, the inner child should be the data source other than the selection.
// This is called when build nested loop join.
func (p *LogicalJoin) buildSelectionWithConds(leftAsOuter bool) (*Selection, []*expression.CorrelatedColumn) {
	var (
		outerSchema     *expression.Schema
		innerChild      Plan
		innerConditions []expression.Expression
	)
	if leftAsOuter {
		outerSchema = p.children[0].Schema()
		innerConditions = p.RightConditions
		innerChild = p.children[1]
	} else {
		outerSchema = p.children[1].Schema()
		innerConditions = p.LeftConditions
		innerChild = p.children[0]
	}
	if sel, ok := innerChild.(*Selection); ok {
		innerConditions = append(innerConditions, sel.Conditions...)
		innerChild = sel.children[0]
	}
	corCols := make([]*expression.CorrelatedColumn, 0, outerSchema.Len())
	for _, col := range outerSchema.Columns {
		corCol := &expression.CorrelatedColumn{Column: *col, Data: new(types.Datum)}
		corCol.Column.ResolveIndices(outerSchema)
		corCols = append(corCols, corCol)
	}
	selection := Selection{}.init(p.allocator, p.ctx)
	selection.SetSchema(innerChild.Schema().Clone())
	selection.SetChildren(innerChild)
	conds := make([]expression.Expression, 0, len(p.EqualConditions)+len(innerConditions)+len(p.OtherConditions))
	for _, cond := range p.EqualConditions {
		newCond := expression.ConvertCol2CorCol(cond, corCols, outerSchema)
		conds = append(conds, newCond)
	}
	selection.Conditions = conds
	// Currently only eq conds will be considered when we call checkScanController, and innerConds from the below sel may contain correlated column,
	// which will have side effect when we do check. So we do check before append other conditions into selection.
	selection.controllerStatus = selection.checkScanController()
	if selection.controllerStatus == notController {
		return nil, nil
	}
	for _, cond := range innerConditions {
		conds = append(conds, cond)
	}
	for _, cond := range p.OtherConditions {
		newCond := expression.ConvertCol2CorCol(cond, corCols, outerSchema)
		newCond.ResolveIndices(innerChild.Schema())
		conds = append(conds, newCond)
	}
	selection.Conditions = conds
	return selection, corCols
}

// outerTableCouldINLJ will check the whether is forced to build index nested loop join or outer info is reliable
// and the count satisfies the condition.
func (p *LogicalJoin) outerTableCouldINLJ(outerInfo *physicalPlanInfo, leftAsOuter bool) bool {
	var forced bool
	if leftAsOuter {
		forced = (p.preferINLJ&preferLeftAsOuter) > 0 && p.hasEqualConds()
	} else {
		forced = (p.preferINLJ&preferRightAsOuter) > 0 && p.hasEqualConds()
	}
	return forced || (outerInfo.reliable && outerInfo.count <= float64(p.ctx.GetSessionVars().MaxRowCountForINLJ))
}

func (p *LogicalJoin) convert2IndexNestedLoopJoinLeft(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	lChild := p.children[0].(LogicalPlan)
	switch x := p.children[1].(type) {
	case *DataSource:
	case *Selection:
		if _, ok := x.children[0].(*DataSource); !ok {
			return nil, nil
		}
	default:
		return nil, nil
	}
	allLeft := true
	for _, col := range prop.props {
		if !lChild.Schema().Contains(col.col) {
			allLeft = false
		}
	}
	lProp := prop
	if !allLeft {
		lProp = &requiredProperty{}
	} else {
		lProp = replaceColsInPropBySchema(prop, lChild.Schema())
	}
	var lInfo *physicalPlanInfo
	var err error
	if innerJoin {
		lInfo, err = lChild.convert2PhysicalPlan(removeLimit(lProp))
	} else {
		lInfo, err = lChild.convert2PhysicalPlan(convertLimitOffsetToCount(lProp))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if lInfo.p == nil {
		return nil, nil
	}
	// If the outer table's row count is reliable and don't exceed the MaxRowCountForINLJ or we use hint to force
	// choosing index nested loop join, we will continue building. Otherwise we just break and return nil.
	if !p.outerTableCouldINLJ(lInfo, true) {
		return nil, nil
	}
	selection, corCols := p.buildSelectionWithConds(true)
	if selection == nil {
		return nil, nil
	}
	rInfo := selection.makeScanController()
	join := PhysicalHashJoin{
		LeftConditions: p.LeftConditions,
		// TODO: decide concurrency by data size.
		Concurrency:   JoinConcurrency,
		DefaultValues: p.DefaultValues,
		SmallTable:    1,
	}.init(p.allocator, p.ctx)
	join.SetChildren(lInfo.p, rInfo.p)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = LeftOuterJoin
	}
	join.SetSchema(p.Schema())
	resultInfo := join.matchProperty(prop, lInfo, rInfo)
	ap := PhysicalApply{
		PhysicalJoin: resultInfo.p,
		OuterSchema:  corCols,
	}.init(p.allocator, p.ctx)
	ap.SetChildren(resultInfo.p.Children()...)
	ap.SetSchema(resultInfo.p.Schema())
	resultInfo.p = ap
	if !allLeft {
		resultInfo = enforceProperty(prop, resultInfo)
	} else {
		resultInfo = enforceProperty(limitProperty(prop.limit), resultInfo)
	}
	return resultInfo, nil
}

func (p *LogicalJoin) convert2IndexNestedLoopJoinRight(prop *requiredProperty, innerJoin bool) (*physicalPlanInfo, error) {
	rChild := p.children[1].(LogicalPlan)
	switch x := p.children[0].(type) {
	case *DataSource:
	case *Selection:
		if _, ok := x.children[0].(*DataSource); !ok {
			return nil, nil
		}
	default:
		return nil, nil
	}
	allRight := true
	for _, col := range prop.props {
		if !rChild.Schema().Contains(col.col) {
			allRight = false
		}
	}
	rProp := prop
	if !allRight {
		rProp = &requiredProperty{}
	} else {
		rProp = replaceColsInPropBySchema(prop, rChild.Schema())
	}
	var rInfo *physicalPlanInfo
	var err error
	if innerJoin {
		rInfo, err = rChild.convert2PhysicalPlan(removeLimit(rProp))
	} else {
		rInfo, err = rChild.convert2PhysicalPlan(convertLimitOffsetToCount(rProp))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if rInfo.p == nil {
		return nil, nil
	}
	// If the outer table's row count is reliable and don't exceed the MaxRowCountForINLJ or we use hint to force
	// choosing index nested loop join, we will continue building. Otherwise we just break and return nil.
	if !p.outerTableCouldINLJ(rInfo, false) {
		return nil, nil
	}
	selection, corCols := p.buildSelectionWithConds(false)
	if selection == nil {
		return nil, nil
	}
	lInfo := selection.makeScanController()
	join := PhysicalHashJoin{
		RightConditions: p.RightConditions,
		// TODO: decide concurrency by data size.
		Concurrency:   JoinConcurrency,
		DefaultValues: p.DefaultValues,
	}.init(p.allocator, p.ctx)
	join.SetChildren(lInfo.p, rInfo.p)
	if innerJoin {
		join.JoinType = InnerJoin
	} else {
		join.JoinType = RightOuterJoin
	}
	join.SetSchema(p.Schema())
	resultInfo := join.matchProperty(prop, lInfo, rInfo)
	ap := PhysicalApply{
		PhysicalJoin: resultInfo.p,
		OuterSchema:  corCols,
	}.init(p.allocator, p.ctx)
	ap.SetChildren(resultInfo.p.Children()...)
	ap.SetSchema(resultInfo.p.Schema())
	resultInfo.p = ap
	if !allRight {
		resultInfo = enforceProperty(prop, resultInfo)
	} else {
		resultInfo = enforceProperty(limitProperty(prop.limit), resultInfo)
	}
	return resultInfo, nil
}

func generateJoinProp(column *expression.Column) *requiredProperty {
	return &requiredProperty{
		props:      []*columnProp{{column, false}},
		sortKeyLen: 1,
	}
}

func compareTypeForOrder(lhs *types.FieldType, rhs *types.FieldType) bool {
	if lhs.Tp != rhs.Tp {
		return false
	}
	if lhs.ToClass() == types.ClassString &&
		(lhs.Charset != rhs.Charset || lhs.Collate != rhs.Collate) {
		return false
	}
	return true
}

// Generate all possible combinations from join conditions for cost evaluation
// It will try all keys in join conditions
func constructPropertyByJoin(join *LogicalJoin) ([][]*requiredProperty, []int, error) {
	var result [][]*requiredProperty
	var condIndex []int

	if join.EqualConditions == nil {
		return nil, nil, nil
	}
	for i, cond := range join.EqualConditions {
		if len(cond.GetArgs()) != 2 {
			return nil, nil, errors.New("unexpected argument count for equal expression")
		}
		lExpr, rExpr := cond.GetArgs()[0], cond.GetArgs()[1]
		// Only consider raw column reference and cowardly ignore calculations
		// since we don't know if the function call preserve order
		lColumn, lOK := lExpr.(*expression.Column)
		rColumn, rOK := rExpr.(*expression.Column)
		if lOK && rOK && compareTypeForOrder(lColumn.RetType, rColumn.RetType) {
			result = append(result, []*requiredProperty{generateJoinProp(lColumn), generateJoinProp(rColumn)})
			condIndex = append(condIndex, i)
		} else {
			continue
		}
	}
	if len(result) == 0 {
		return nil, nil, nil
	}
	return result, condIndex, nil
}

// convert2PhysicalMergeJoin converts the merge join to *physicalPlanInfo.
// TODO: Refactor and merge with hash join
func (p *LogicalJoin) convert2PhysicalMergeJoin(parentProp *requiredProperty, lProp *requiredProperty, rProp *requiredProperty, condIndex int, joinType JoinType) (*physicalPlanInfo, error) {
	lChild := p.children[0].(LogicalPlan)
	rChild := p.children[1].(LogicalPlan)

	newEQConds := make([]*expression.ScalarFunction, 0, len(p.EqualConditions)-1)
	for i, cond := range p.EqualConditions {
		if i == condIndex {
			continue
		}
		// prevent further index contamination
		newCond := cond.Clone()
		newCond.ResolveIndices(p.schema)
		newEQConds = append(newEQConds, newCond.(*expression.ScalarFunction))
	}
	eqCond := p.EqualConditions[condIndex]

	otherFilter := append(expression.ScalarFuncs2Exprs(newEQConds), p.OtherConditions...)

	join := PhysicalMergeJoin{
		EqualConditions: []*expression.ScalarFunction{eqCond},
		LeftConditions:  p.LeftConditions,
		RightConditions: p.RightConditions,
		OtherConditions: otherFilter,
		DefaultValues:   p.DefaultValues,
		// Assume order for both side are the same
		Desc: lProp.props[0].desc,
	}.init(p.allocator, p.ctx)
	join.SetSchema(p.schema)
	join.JoinType = joinType

	var lInfo *physicalPlanInfo
	var rInfo *physicalPlanInfo

	// Try no sort first
	lInfoEnforceSort, err := lChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	lInfoEnforceSort = enforceProperty(lProp, lInfoEnforceSort)

	lInfoNoSorted, err := lChild.convert2PhysicalPlan(lProp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if lInfoNoSorted.cost < lInfoEnforceSort.cost {
		lInfo = lInfoNoSorted
	} else {
		lInfo = lInfoEnforceSort
	}

	rInfoEnforceSort, err := rChild.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	rInfoEnforceSort = enforceProperty(rProp, rInfoEnforceSort)

	rInfoNoSorted, err := rChild.convert2PhysicalPlan(rProp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rInfoEnforceSort.cost < rInfoNoSorted.cost {
		rInfo = rInfoEnforceSort
	} else {
		rInfo = rInfoNoSorted
	}
	parentProp = join.tryConsumeOrder(parentProp, eqCond)

	resultInfo := join.matchProperty(parentProp, lInfo, rInfo)
	// TODO: Considering keeping order in join to remove at least
	// one ordering property
	resultInfo = enforceProperty(parentProp, resultInfo)
	return resultInfo, nil
}

func (p *LogicalJoin) convert2PhysicalMergeJoinOnCost(prop *requiredProperty) (*physicalPlanInfo, error) {
	var info *physicalPlanInfo
	reqPropPairs, condIndex, err := constructPropertyByJoin(p)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if reqPropPairs == nil {
		return nil, nil
	}
	minCost := math.MaxFloat64
	var minInfo *physicalPlanInfo
	for i, reqPropPair := range reqPropPairs {
		info, err = p.convert2PhysicalMergeJoin(prop, reqPropPair[0], reqPropPair[1], condIndex[i], p.JoinType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Force to choose first instead of nil if all Inf cost
		// TODO: Consider cost instead of force merge
		if minInfo == nil {
			minInfo = info
			minCost = info.cost
		} else {
			if info.cost <= minCost {
				minInfo = info
				minCost = info.cost
			}
		}
	}
	return minInfo, nil
}

func (p *LogicalJoin) hasEqualConds() bool {
	// rule out non-equal join
	if len(p.EqualConditions) == 0 {
		return false
	}

	return true
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *LogicalJoin) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin:
		info, err = p.convert2PhysicalPlanSemi(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case LeftOuterJoin:
		if p.preferMergeJoin && p.hasEqualConds() {
			info, err = p.convert2PhysicalMergeJoinOnCost(prop)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if info != nil {
				break
			}
		}
		nljInfo, err := p.convert2IndexNestedLoopJoinLeft(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if nljInfo != nil {
			info = nljInfo
			break
		}
		// Otherwise fall into hash join
		info, err = p.convert2PhysicalPlanLeft(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case RightOuterJoin:
		if p.preferMergeJoin && p.hasEqualConds() {
			info, err = p.convert2PhysicalMergeJoinOnCost(prop)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if info != nil {
				break
			}
		}
		nljInfo, err := p.convert2IndexNestedLoopJoinRight(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if nljInfo != nil {
			info = nljInfo
			break
		}
		info, err = p.convert2PhysicalPlanRight(prop, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		// Inner Join
		if p.preferMergeJoin && p.hasEqualConds() {
			info, err = p.convert2PhysicalMergeJoinOnCost(prop)
			if err != nil {
				return nil, errors.Trace(err)
			}
			break
		}
		lNLJInfo, err := p.convert2IndexNestedLoopJoinLeft(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rNLJInfo, err := p.convert2IndexNestedLoopJoinRight(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if lNLJInfo != nil {
			info = lNLJInfo
		}
		if info == nil || (rNLJInfo != nil && info.cost > rNLJInfo.cost) {
			info = rNLJInfo
		}
		if info != nil {
			break
		}
		// fall back to hash join
		lInfo, err := p.convert2PhysicalPlanLeft(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		rInfo, err := p.convert2PhysicalPlanRight(prop, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rInfo.cost < lInfo.cost {
			info = rInfo
		} else {
			info = lInfo
		}
	}
	p.storePlanInfo(prop, info)
	return info, nil
}

// convert2PhysicalPlanStream converts the logical aggregation to the stream aggregation *physicalPlanInfo.
func (p *LogicalAggregation) convert2PhysicalPlanStream(prop *requiredProperty) (*physicalPlanInfo, error) {
	for _, aggFunc := range p.AggFuncs {
		if aggFunc.GetMode() == expression.FinalMode {
			return &physicalPlanInfo{cost: math.MaxFloat64}, nil
		}
	}
	agg := PhysicalAggregation{
		AggType:      StreamedAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}.init(p.allocator, p.ctx)
	agg.HasGby = len(p.GroupByItems) > 0
	agg.SetSchema(p.schema)
	// TODO: Consider distinct key.
	info := &physicalPlanInfo{cost: math.MaxFloat64}
	gbyCols := p.groupByCols
	if len(gbyCols) != len(p.GroupByItems) {
		// group by a + b is not interested in any order.
		return info, nil
	}
	isSortKey := make([]bool, len(gbyCols))
	newProp := &requiredProperty{
		props: make([]*columnProp, 0, len(gbyCols)),
	}
	for _, pro := range prop.props {
		idx := p.getGbyColIndex(pro.col)
		if idx == -1 {
			return info, nil
		}
		isSortKey[idx] = true
		// We should add columns in aggregation in order to keep index right.
		newProp.props = append(newProp.props, &columnProp{col: gbyCols[idx], desc: pro.desc})
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
	info.cost += info.count * cpuFactor
	info.count = info.count * aggFactor
	return info, nil
}

// convert2PhysicalPlanFinalHash converts the logical aggregation to the final hash aggregation *physicalPlanInfo.
func (p *LogicalAggregation) convert2PhysicalPlanFinalHash(x physicalDistSQLPlan, childInfo *physicalPlanInfo) *physicalPlanInfo {
	agg := PhysicalAggregation{
		AggType:      FinalAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}.init(p.allocator, p.ctx)
	agg.SetSchema(p.schema)
	agg.HasGby = len(p.GroupByItems) > 0
	schema := x.addAggregation(p.ctx, agg)
	if schema.Len() == 0 {
		return nil
	}
	x.(PhysicalPlan).SetSchema(schema)
	info := addPlanToResponse(agg, childInfo)
	info.count = info.count * aggFactor
	// if we build the final aggregation, it must be the best plan.
	info.cost = 0
	return info
}

// convert2PhysicalPlanCompleteHash converts the logical aggregation to the complete hash aggregation *physicalPlanInfo.
func (p *LogicalAggregation) convert2PhysicalPlanCompleteHash(childInfo *physicalPlanInfo) *physicalPlanInfo {
	agg := PhysicalAggregation{
		AggType:      CompleteAgg,
		AggFuncs:     p.AggFuncs,
		GroupByItems: p.GroupByItems,
	}.init(p.allocator, p.ctx)
	agg.HasGby = len(p.GroupByItems) > 0
	agg.SetSchema(p.schema)
	info := addPlanToResponse(agg, childInfo)
	info.cost += info.count * memoryFactor
	info.count = info.count * aggFactor
	return info
}

// convert2PhysicalPlanHash converts the logical aggregation to the physical hash aggregation.
func (p *LogicalAggregation) convert2PhysicalPlanHash() (*physicalPlanInfo, error) {
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
		if x, ok := childInfo.p.(physicalDistSQLPlan); ok {
			info := p.convert2PhysicalPlanFinalHash(x, childInfo)
			if info != nil {
				return info, nil
			}
		}
	}
	return p.convert2PhysicalPlanCompleteHash(childInfo), nil
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *LogicalAggregation) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	planInfo, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if planInfo != nil {
		return planInfo, nil
	}
	limit := prop.limit
	if len(prop.props) == 0 {
		planInfo, err = p.convert2PhysicalPlanHash()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	streamInfo, err := p.convert2PhysicalPlanStream(removeLimit(prop))
	if planInfo == nil || streamInfo.cost < planInfo.cost {
		planInfo = streamInfo
	}
	planInfo = enforceProperty(limitProperty(limit), planInfo)
	err = p.storePlanInfo(prop, planInfo)
	return planInfo, errors.Trace(err)
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *Union) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	limit := prop.limit
	childInfos := make([]*physicalPlanInfo, 0, len(p.children))
	for _, child := range p.Children() {
		newProp := &requiredProperty{}
		if limit != nil {
			newProp = convertLimitOffsetToCount(prop)
			newProp.props = make([]*columnProp, 0, len(prop.props))
			for _, c := range prop.props {
				idx := p.Schema().ColumnIndex(c.col)
				newProp.props = append(newProp.props, &columnProp{col: child.Schema().Columns[idx], desc: c.desc})
			}
		}
		childInfo, err := child.(LogicalPlan).convert2PhysicalPlan(newProp)
		if err != nil {
			return nil, errors.Trace(err)
		}
		childInfos = append(childInfos, childInfo)
	}
	info = p.matchProperty(prop, childInfos...)
	info = enforceProperty(prop, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

// makeScanController will try to build a selection that controls the below scan's filter condition,
// and return a physicalPlanInfo. If the onlyCheck is true, it will only check whether this selection
// can become a scan controller without building the physical plan.
func (p *Selection) makeScanController() *physicalPlanInfo {
	var (
		child       PhysicalPlan
		corColConds []expression.Expression
	)
	ds := p.children[0].(*DataSource)
	indices, _ := availableIndices(ds.indexHints, ds.tableInfo)
	for _, expr := range p.Conditions {
		if !expr.IsCorrelated() {
			continue
		}
		cond := pushDownNot(expr, false, nil)
		corCols := extractCorColumns(cond)
		for _, col := range corCols {
			*col.Data = expression.One.Value
		}
		newCond, _ := expression.SubstituteCorCol2Constant(cond)
		corColConds = append(corColConds, newCond)
	}
	if p.controllerStatus == controlTableScan {
		ts := PhysicalTableScan{
			Table:               ds.tableInfo,
			Columns:             ds.Columns,
			TableAsName:         ds.TableAsName,
			DBName:              ds.DBName,
			physicalTableSource: physicalTableSource{client: ds.ctx.GetClient()},
		}.init(p.allocator, p.ctx)
		ts.SetSchema(ds.schema)
		if ds.ctx.Txn() != nil {
			ts.readOnly = p.ctx.Txn().IsReadOnly()
		} else {
			ts.readOnly = true
		}
		child = ts
	} else if p.controllerStatus == controlIndexScan {
		var (
			chosenPlan     *PhysicalIndexScan
			bestEqualCount int
		)
		for _, idx := range indices {
			condsBackUp := make([]expression.Expression, 0, len(corColConds))
			for _, cond := range corColConds {
				condsBackUp = append(condsBackUp, cond.Clone())
			}
			_, _, accessEqualCount, _ := DetachIndexScanConditions(condsBackUp, idx)
			if chosenPlan == nil || bestEqualCount < accessEqualCount {
				is := PhysicalIndexScan{
					Table:               ds.tableInfo,
					Index:               idx,
					Columns:             ds.Columns,
					TableAsName:         ds.TableAsName,
					OutOfOrder:          true,
					DBName:              ds.DBName,
					physicalTableSource: physicalTableSource{client: ds.ctx.GetClient()},
				}.init(p.allocator, p.ctx)
				is.SetSchema(ds.schema)
				if is.ctx.Txn() != nil {
					is.readOnly = p.ctx.Txn().IsReadOnly()
				} else {
					is.readOnly = true
				}
				is.DoubleRead = !isCoveringIndex(is.Columns, is.Index.Columns, is.Table.PKIsHandle)
				chosenPlan, bestEqualCount = is, accessEqualCount
			}
		}
		child = chosenPlan
	}
	newSel := p.Copy().(*Selection)
	newSel.ScanController = true
	newSel.SetChildren(child)
	info := &physicalPlanInfo{
		p:     newSel,
		count: float64(ds.statisticTable.Count),
	}
	info.cost = info.count * selectionFactor
	return info
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *Selection) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	if p.controllerStatus != notController {
		info = p.makeScanController()
		info = enforceProperty(prop, info)
		p.storePlanInfo(prop, info)
		return info, nil
	}
	// Firstly, we try to push order.
	info, err = p.convert2PhysicalPlanPushOrder(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(prop.props) > 0 {
		// Secondly, we push nothing and enforce this property.
		infoEnforce, err := p.convert2PhysicalPlanEnforce(prop)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if infoEnforce.cost < info.cost {
			info = infoEnforce
		}
	}
	info = p.matchProperty(prop, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

func (p *Selection) appendSelToInfo(info *physicalPlanInfo) *physicalPlanInfo {
	np := p.Copy().(*Selection)
	np.SetChildren(info.p)
	return &physicalPlanInfo{
		p:        np,
		cost:     info.cost,
		count:    info.count * selectionFactor,
		reliable: info.reliable,
	}
}

func (p *Selection) convert2PhysicalPlanPushOrder(prop *requiredProperty) (*physicalPlanInfo, error) {
	child := p.children[0].(LogicalPlan)
	limit := prop.limit
	info, err := child.convert2PhysicalPlan(removeLimit(prop))
	if err != nil {
		return nil, errors.Trace(err)
	}
	if limit != nil && info.p != nil {
		if np, ok := info.p.(physicalDistSQLPlan); ok {
			np.addLimit(limit)
			scanCount := info.count
			info.count = float64(limit.Count)
			info.cost = np.calculateCost(info.count, scanCount)
			if limit.Offset > 0 {
				info = enforceProperty(&requiredProperty{limit: limit}, info)
			}
		} else {
			info = enforceProperty(&requiredProperty{limit: limit}, p.appendSelToInfo(info))
		}
	} else if ds, ok := p.children[0].(*DataSource); !ok {
		// TODO: It's tooooooo tricky here, we will remove all these logic after implementing DAG push down.
		info = p.appendSelToInfo(info)
	} else {
		client := p.ctx.GetClient()
		memDB := infoschema.IsMemoryDB(ds.DBName.L)
		isDistReq := !memDB && client != nil && client.SupportRequestType(kv.ReqTypeSelect, 0)
		if !isDistReq {
			info = p.appendSelToInfo(info)
		}
	}
	return info, nil
}

// convert2PhysicalPlanEnforce converts a selection to *physicalPlanInfo which does not push the
// required property to the children, but enforce the property instead.
func (p *Selection) convert2PhysicalPlanEnforce(prop *requiredProperty) (*physicalPlanInfo, error) {
	child := p.children[0].(LogicalPlan)
	info, err := child.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if prop.limit != nil && len(prop.props) > 0 {
		if t, ok := info.p.(physicalDistSQLPlan); ok {
			t.addTopN(p.ctx, prop)
		} else if _, ok := info.p.(*Selection); !ok {
			info = p.appendSelToInfo(info)
		}
		info = enforceProperty(prop, info)
	} else if len(prop.props) != 0 {
		info = &physicalPlanInfo{cost: math.MaxFloat64}
	}
	return info, nil
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
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
		sortKeyLen: prop.sortKeyLen,
		limit:      prop.limit}
	childSchema := p.children[0].Schema()
	usedCols := make([]bool, childSchema.Len())
	canPassSort := true
loop:
	for _, c := range prop.props {
		idx := p.schema.ColumnIndex(c.col)
		switch v := p.Exprs[idx].(type) {
		case *expression.Column:
			childIdx := childSchema.ColumnIndex(v)
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
	info, err = p.children[0].(LogicalPlan).convert2PhysicalPlan(newProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info = addPlanToResponse(p, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

func matchProp(ctx context.Context, target, new *requiredProperty) bool {
	if target.sortKeyLen > len(new.props) {
		return false
	}
	for i := 0; i < target.sortKeyLen; i++ {
		if !target.props[i].equal(new.props[i], ctx) {
			return false
		}
	}
	for i := target.sortKeyLen; i < len(target.props); i++ {
		isMatch := false
		for _, pro := range new.props {
			if pro.col.Equal(target.props[i].col, ctx) {
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

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *Sort) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	if len(p.ByItems) == 0 {
		return p.children[0].(LogicalPlan).convert2PhysicalPlan(prop)
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
	if len(selfProp.props) != 0 && len(prop.props) == 0 && prop.limit != nil {
		selfProp.limit = prop.limit
	}
	sortedPlanInfo, err := p.children[0].(LogicalPlan).convert2PhysicalPlan(selfProp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	unSortedPlanInfo, err := p.children[0].(LogicalPlan).convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	sortCost := sortCost(unSortedPlanInfo.count)
	if len(selfProp.props) == 0 {
		np := p.Copy().(*Sort)
		np.ExecLimit = prop.limit
		sortedPlanInfo = addPlanToResponse(np, sortedPlanInfo)
	} else if sortCost+unSortedPlanInfo.cost < sortedPlanInfo.cost {
		sortedPlanInfo.cost = sortCost + unSortedPlanInfo.cost
		np := p.Copy().(*Sort)
		np.ExecLimit = selfProp.limit
		sortedPlanInfo = addPlanToResponse(np, unSortedPlanInfo)
	}
	if !matchProp(p.ctx, prop, selfProp) {
		sortedPlanInfo.cost = math.MaxFloat64
	}
	p.storePlanInfo(prop, sortedPlanInfo)
	return sortedPlanInfo, nil
}

// convert2PhysicalPlan implements the LogicalPlan convert2PhysicalPlan interface.
func (p *LogicalApply) convert2PhysicalPlan(prop *requiredProperty) (*physicalPlanInfo, error) {
	info, err := p.getPlanInfo(prop)
	if err != nil {
		return info, errors.Trace(err)
	}
	if info != nil {
		return info, nil
	}
	if p.JoinType == InnerJoin || p.JoinType == LeftOuterJoin {
		info, err = p.LogicalJoin.convert2PhysicalPlanLeft(&requiredProperty{}, p.JoinType == InnerJoin)
	} else {
		info, err = p.LogicalJoin.convert2PhysicalPlanSemi(&requiredProperty{})
	}
	if err != nil {
		return info, errors.Trace(err)
	}
	switch info.p.(type) {
	case *PhysicalHashJoin, *PhysicalHashSemiJoin:
		ap := PhysicalApply{
			PhysicalJoin: info.p,
			OuterSchema:  p.corCols,
		}.init(p.allocator, p.ctx)
		ap.SetChildren(info.p.Children()...)
		ap.SetSchema(info.p.Schema())
		info.p = ap
	default:
		info.cost = math.MaxFloat64
		info.p = nil
	}
	info = enforceProperty(prop, info)
	p.storePlanInfo(prop, info)
	return info, nil
}

// addCachePlan will add a Cache plan above the plan whose father's IsCorrelated() is true but its own IsCorrelated() is false.
func addCachePlan(p PhysicalPlan, allocator *idAllocator) []*expression.CorrelatedColumn {
	if len(p.Children()) == 0 {
		return nil
	}
	selfCorCols := p.extractCorrelatedCols()
	newChildren := make([]Plan, 0, len(p.Children()))
	for _, child := range p.Children() {
		childCorCols := addCachePlan(child.(PhysicalPlan), allocator)
		// If p is a Selection and controls the access condition of below scan plan, there shouldn't have a cache plan.
		if sel, ok := p.(*Selection); len(selfCorCols) > 0 && len(childCorCols) == 0 && (!ok || !sel.ScanController) {
			newChild := Cache{}.init(p.Allocator(), p.context())
			newChild.SetSchema(child.Schema())

			addChild(newChild, child)
			newChild.SetParents(p)

			newChildren = append(newChildren, newChild)
		} else {
			newChildren = append(newChildren, child)
		}
	}
	p.SetChildren(newChildren...)
	return selfCorCols
}
