// Copyright 2018 PingCAP, Inc.
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

package cascades

import (
	"math"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	impl "github.com/pingcap/tidb/planner/implementation"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
)

// ImplementationRule defines the interface for implementation rules.
type ImplementationRule interface {
	// Match checks if current GroupExpr matches this rule under required physical property.
	Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool)
	// OnImplement generates physical plan using this rule for current GroupExpr. Note that
	// childrenReqProps of generated physical plan should be set correspondingly in this function.
	OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error)
}

var defaultImplementationMap = map[memo.Operand][]ImplementationRule{
	memo.OperandTableDual: {
		&ImplTableDual{},
	},
	memo.OperandMemTableScan: {
		&ImplMemTableScan{},
	},
	memo.OperandProjection: {
		&ImplProjection{},
	},
	memo.OperandTableScan: {
		&ImplTableScan{},
	},
	memo.OperandIndexScan: {
		&ImplIndexScan{},
	},
	memo.OperandTiKVSingleGather: {
		&ImplTiKVSingleReadGather{},
	},
	memo.OperandShow: {
		&ImplShow{},
	},
	memo.OperandSelection: {
		&ImplSelection{},
	},
	memo.OperandSort: {
		&ImplSort{},
	},
	memo.OperandAggregation: {
		&ImplHashAgg{},
	},
	memo.OperandLimit: {
		&ImplLimit{},
	},
	memo.OperandTopN: {
		&ImplTopN{},
		&ImplTopNAsLimit{},
	},
	memo.OperandJoin: {
		&ImplHashJoinBuildLeft{},
		&ImplHashJoinBuildRight{},
		&ImplMergeJoin{},
	},
	memo.OperandUnionAll: {
		&ImplUnionAll{},
	},
	memo.OperandApply: {
		&ImplApply{},
	},
	memo.OperandMaxOneRow: {
		&ImplMaxOneRow{},
	},
	memo.OperandWindow: {
		&ImplWindow{},
	},
}

// ImplTableDual implements LogicalTableDual as PhysicalTableDual.
type ImplTableDual struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTableDual) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	if !prop.IsEmpty() {
		return false
	}
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTableDual) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicDual := expr.ExprNode.(*plannercore.LogicalTableDual)
	dual := plannercore.PhysicalTableDual{RowCount: logicDual.RowCount}.Init(logicDual.SCtx(), logicProp.Stats, logicDual.SelectBlockOffset())
	dual.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewTableDualImpl(dual)}, nil
}

// ImplMemTableScan implements LogicalMemTable as PhysicalMemTable.
type ImplMemTableScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplMemTableScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	if !prop.IsEmpty() {
		return false
	}
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplMemTableScan) OnImplement(
	expr *memo.GroupExpr,
	reqProp *property.PhysicalProperty,
) ([]memo.Implementation, error) {
	logic := expr.ExprNode.(*plannercore.LogicalMemTable)
	logicProp := expr.Group.Prop
	physical := plannercore.PhysicalMemTable{
		DBName:    logic.DBName,
		Table:     logic.TableInfo,
		Columns:   logic.TableInfo.Columns,
		Extractor: logic.Extractor,
	}.Init(logic.SCtx(), logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logic.SelectBlockOffset())
	physical.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewMemTableScanImpl(physical)}, nil
}

// ImplProjection implements LogicalProjection as PhysicalProjection.
type ImplProjection struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplProjection) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplProjection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicProj := expr.ExprNode.(*plannercore.LogicalProjection)
	childProp, ok := logicProj.TryToGetChildProp(reqProp)
	if !ok {
		return nil, nil
	}
	proj := plannercore.PhysicalProjection{
		Exprs:                logicProj.Exprs,
		CalculateNoDelay:     logicProj.CalculateNoDelay,
		AvoidColumnEvaluator: logicProj.AvoidColumnEvaluator,
	}.Init(logicProj.SCtx(), logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicProj.SelectBlockOffset(), childProp)
	proj.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewProjectionImpl(proj)}, nil
}

// ImplTiKVSingleReadGather implements TiKVSingleGather
// as PhysicalTableReader or PhysicalIndexReader.
type ImplTiKVSingleReadGather struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTiKVSingleReadGather) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTiKVSingleReadGather) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	sg := expr.ExprNode.(*plannercore.TiKVSingleGather)
	if sg.IsIndexGather {
		reader := sg.GetPhysicalIndexReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
		return []memo.Implementation{impl.NewIndexReaderImpl(reader, sg.Source.TblColHists)}, nil
	}
	reader := sg.GetPhysicalTableReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
	return []memo.Implementation{impl.NewTableReaderImpl(reader, sg.Source.TblColHists)}, nil
}

// ImplTableScan implements TableScan as PhysicalTableScan.
type ImplTableScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTableScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ts := expr.ExprNode.(*plannercore.LogicalTableScan)
	return prop.IsEmpty() || (len(prop.Items) == 1 && ts.Handle != nil && prop.Items[0].Col.Equal(nil, ts.Handle))
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTableScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicalScan := expr.ExprNode.(*plannercore.LogicalTableScan)
	ts := logicalScan.GetPhysicalScan(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		ts.KeepOrder = true
		ts.Desc = reqProp.Items[0].Desc
	}
	tblCols, tblColHists := logicalScan.Source.TblCols, logicalScan.Source.TblColHists
	return []memo.Implementation{impl.NewTableScanImpl(ts, tblCols, tblColHists)}, nil
}

// ImplIndexScan implements IndexScan as PhysicalIndexScan.
type ImplIndexScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplIndexScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	is := expr.ExprNode.(*plannercore.LogicalIndexScan)
	return is.MatchIndexProp(prop)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplIndexScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalScan := expr.ExprNode.(*plannercore.LogicalIndexScan)
	is := logicalScan.GetPhysicalIndexScan(expr.Group.Prop.Schema, expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		is.KeepOrder = true
		if reqProp.Items[0].Desc {
			is.Desc = true
		}
	}
	return []memo.Implementation{impl.NewIndexScanImpl(is, logicalScan.Source.TblColHists)}, nil
}

// ImplShow is the implementation rule which implements LogicalShow to
// PhysicalShow.
type ImplShow struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplShow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplShow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	show := expr.ExprNode.(*plannercore.LogicalShow)

	// TODO(zz-jason): unifying LogicalShow and PhysicalShow to a single
	// struct. So that we don't need to create a new PhysicalShow object, which
	// can help us to reduce the gc pressure of golang runtime and improve the
	// overall performance.
	showPhys := plannercore.PhysicalShow{ShowContents: show.ShowContents}.Init(show.SCtx())
	showPhys.SetSchema(logicProp.Schema)
	return []memo.Implementation{impl.NewShowImpl(showPhys)}, nil
}

// ImplSelection is the implementation rule which implements LogicalSelection
// to PhysicalSelection.
type ImplSelection struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplSelection) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplSelection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalSel := expr.ExprNode.(*plannercore.LogicalSelection)
	physicalSel := plannercore.PhysicalSelection{
		Conditions: logicalSel.Conditions,
	}.Init(logicalSel.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicalSel.SelectBlockOffset(), reqProp.Clone())
	switch expr.Group.EngineType {
	case memo.EngineTiDB:
		return []memo.Implementation{impl.NewTiDBSelectionImpl(physicalSel)}, nil
	case memo.EngineTiKV:
		return []memo.Implementation{impl.NewTiKVSelectionImpl(physicalSel)}, nil
	default:
		return nil, plannercore.ErrInternal.GenWithStack("Unsupported EngineType '%s' for Selection.", expr.Group.EngineType.String())
	}
}

// ImplSort is the implementation rule which implements LogicalSort
// to PhysicalSort or NominalSort.
type ImplSort struct {
}

// Match implements ImplementationRule match interface.
func (r *ImplSort) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ls := expr.ExprNode.(*plannercore.LogicalSort)
	return plannercore.MatchItems(prop, ls.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
// If all of the sort items are columns, generate a NominalSort, otherwise
// generate a PhysicalSort.
func (r *ImplSort) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	ls := expr.ExprNode.(*plannercore.LogicalSort)
	if newProp, canUseNominal := plannercore.GetPropByOrderByItems(ls.ByItems); canUseNominal {
		newProp.ExpectedCnt = reqProp.ExpectedCnt
		ns := plannercore.NominalSort{}.Init(ls.SCtx(), ls.SelectBlockOffset(), newProp)
		return []memo.Implementation{impl.NewNominalSortImpl(ns)}, nil
	}
	ps := plannercore.PhysicalSort{ByItems: ls.ByItems}.Init(
		ls.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		ls.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	return []memo.Implementation{impl.NewSortImpl(ps)}, nil
}

// ImplHashAgg is the implementation rule which implements LogicalAggregation
// to PhysicalHashAgg.
type ImplHashAgg struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplHashAgg) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	// TODO: deal with the hints when we have implemented StreamAgg.
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplHashAgg) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	la := expr.ExprNode.(*plannercore.LogicalAggregation)
	hashAgg := plannercore.NewPhysicalHashAgg(
		la,
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	hashAgg.SetSchema(expr.Group.Prop.Schema.Clone())
	switch expr.Group.EngineType {
	case memo.EngineTiDB:
		return []memo.Implementation{impl.NewTiDBHashAggImpl(hashAgg)}, nil
	case memo.EngineTiKV:
		return []memo.Implementation{impl.NewTiKVHashAggImpl(hashAgg)}, nil
	default:
		return nil, plannercore.ErrInternal.GenWithStack("Unsupported EngineType '%s' for HashAggregation.", expr.Group.EngineType.String())
	}
}

// ImplLimit is the implementation rule which implements LogicalLimit
// to PhysicalLimit.
type ImplLimit struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplLimit) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplLimit) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalLimit := expr.ExprNode.(*plannercore.LogicalLimit)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(logicalLimit.Count + logicalLimit.Offset)}
	physicalLimit := plannercore.PhysicalLimit{
		Offset: logicalLimit.Offset,
		Count:  logicalLimit.Count,
	}.Init(logicalLimit.SCtx(), expr.Group.Prop.Stats, logicalLimit.SelectBlockOffset(), newProp)
	return []memo.Implementation{impl.NewLimitImpl(physicalLimit)}, nil
}

// ImplTopN is the implementation rule which implements LogicalTopN
// to PhysicalTopN.
type ImplTopN struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTopN) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*plannercore.LogicalTopN)
	if expr.Group.EngineType != memo.EngineTiDB {
		return prop.IsEmpty()
	}
	return plannercore.MatchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTopN) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lt := expr.ExprNode.(*plannercore.LogicalTopN)
	resultProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	topN := plannercore.PhysicalTopN{
		ByItems: lt.ByItems,
		Count:   lt.Count,
		Offset:  lt.Offset,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), resultProp)
	switch expr.Group.EngineType {
	case memo.EngineTiDB:
		return []memo.Implementation{impl.NewTiDBTopNImpl(topN)}, nil
	case memo.EngineTiKV:
		return []memo.Implementation{impl.NewTiKVTopNImpl(topN)}, nil
	default:
		return nil, plannercore.ErrInternal.GenWithStack("Unsupported EngineType '%s' for TopN.", expr.Group.EngineType.String())
	}
}

// ImplTopNAsLimit is the implementation rule which implements LogicalTopN
// as PhysicalLimit with required order property.
type ImplTopNAsLimit struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTopNAsLimit) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	topN := expr.ExprNode.(*plannercore.LogicalTopN)
	_, canUseLimit := plannercore.GetPropByOrderByItems(topN.ByItems)
	return canUseLimit && plannercore.MatchItems(prop, topN.ByItems)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTopNAsLimit) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lt := expr.ExprNode.(*plannercore.LogicalTopN)
	newProp := &property.PhysicalProperty{ExpectedCnt: float64(lt.Count + lt.Offset)}
	newProp.Items = make([]property.Item, len(lt.ByItems))
	for i, item := range lt.ByItems {
		newProp.Items[i].Col = item.Expr.(*expression.Column)
		newProp.Items[i].Desc = item.Desc
	}
	physicalLimit := plannercore.PhysicalLimit{
		Offset: lt.Offset,
		Count:  lt.Count,
	}.Init(lt.SCtx(), expr.Group.Prop.Stats, lt.SelectBlockOffset(), newProp)
	return []memo.Implementation{impl.NewLimitImpl(physicalLimit)}, nil
}

func getImplForHashJoin(expr *memo.GroupExpr, prop *property.PhysicalProperty, innerIdx int, useOuterToBuild bool) memo.Implementation {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[0] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	chReqProps[1] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	stats := expr.Group.Prop.Stats
	if prop.ExpectedCnt < stats.RowCount {
		expCntScale := prop.ExpectedCnt / stats.RowCount
		chReqProps[1-innerIdx].ExpectedCnt = expr.Children[1-innerIdx].Prop.Stats.RowCount * expCntScale
	}
	hashJoin := plannercore.NewPhysicalHashJoin(join, innerIdx, useOuterToBuild, stats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps...)
	hashJoin.SetSchema(expr.Group.Prop.Schema)
	return impl.NewHashJoinImpl(hashJoin)
}

// ImplHashJoinBuildLeft implements LogicalJoin to PhysicalHashJoin which uses the left child to build hash table.
type ImplHashJoinBuildLeft struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplHashJoinBuildLeft) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	switch expr.ExprNode.(*plannercore.LogicalJoin).JoinType {
	case plannercore.InnerJoin, plannercore.LeftOuterJoin, plannercore.RightOuterJoin:
		return prop.IsEmpty()
	default:
		return false
	}
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplHashJoinBuildLeft) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	switch join.JoinType {
	case plannercore.InnerJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, false)}, nil
	case plannercore.LeftOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, true)}, nil
	case plannercore.RightOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, false)}, nil
	default:
		return nil, nil
	}
}

// ImplHashJoinBuildRight implements LogicalJoin to PhysicalHashJoin which uses the right child to build hash table.
type ImplHashJoinBuildRight struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplHashJoinBuildRight) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplHashJoinBuildRight) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	switch join.JoinType {
	case plannercore.SemiJoin, plannercore.AntiSemiJoin,
		plannercore.LeftOuterSemiJoin, plannercore.AntiLeftOuterSemiJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case plannercore.InnerJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case plannercore.LeftOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 1, false)}, nil
	case plannercore.RightOuterJoin:
		return []memo.Implementation{getImplForHashJoin(expr, reqProp, 0, true)}, nil
	}
	return nil, nil
}

// ImplMergeJoin implements LogicalMergeJoin to PhysicalMergeJoin.
type ImplMergeJoin struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplMergeJoin) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplMergeJoin) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	physicalMergeJoins := join.GetMergeJoin(reqProp, expr.Schema(), expr.Group.Prop.Stats, expr.Children[0].Prop.Stats, expr.Children[1].Prop.Stats)
	mergeJoinImpls := make([]memo.Implementation, 0, len(physicalMergeJoins))
	for _, physicalPlan := range physicalMergeJoins {
		physicalMergeJoin := physicalPlan.(*plannercore.PhysicalMergeJoin)
		mergeJoinImpls = append(mergeJoinImpls, impl.NewMergeJoinImpl(physicalMergeJoin))
	}
	return mergeJoinImpls, nil
}

// ImplUnionAll implements LogicalUnionAll to PhysicalUnionAll.
type ImplUnionAll struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplUnionAll) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplUnionAll) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicalUnion := expr.ExprNode.(*plannercore.LogicalUnionAll)
	chReqProps := make([]*property.PhysicalProperty, len(expr.Children))
	for i := range expr.Children {
		chReqProps[i] = &property.PhysicalProperty{ExpectedCnt: reqProp.ExpectedCnt}
	}
	physicalUnion := plannercore.PhysicalUnionAll{}.Init(
		logicalUnion.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		logicalUnion.SelectBlockOffset(),
		chReqProps...,
	)
	physicalUnion.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewUnionAllImpl(physicalUnion)}, nil
}

// ImplApply implements LogicalApply to PhysicalApply
type ImplApply struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplApply) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.AllColsFromSchema(expr.Children[0].Prop.Schema)
}

// OnImplement implements ImplementationRule OnImplement interface
func (r *ImplApply) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	la := expr.ExprNode.(*plannercore.LogicalApply)
	join := la.GetHashJoin(reqProp)
	physicalApply := plannercore.PhysicalApply{
		PhysicalHashJoin: *join,
		OuterSchema:      la.CorCols,
	}.Init(
		la.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		la.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: reqProp.Items},
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64})
	physicalApply.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewApplyImpl(physicalApply)}, nil
}

// ImplMaxOneRow implements LogicalMaxOneRow to PhysicalMaxOneRow.
type ImplMaxOneRow struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplMaxOneRow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return prop.IsEmpty()
}

// OnImplement implements ImplementationRule OnImplement interface
func (r *ImplMaxOneRow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	mor := expr.ExprNode.(*plannercore.LogicalMaxOneRow)
	physicalMaxOneRow := plannercore.PhysicalMaxOneRow{}.Init(
		mor.SCtx(),
		expr.Group.Prop.Stats,
		mor.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: 2})
	return []memo.Implementation{impl.NewMaxOneRowImpl(physicalMaxOneRow)}, nil
}

// ImplWindow implements LogicalWindow to PhysicalWindow.
type ImplWindow struct {
}

// Match implements ImplementationRule Match interface.
func (w *ImplWindow) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	lw := expr.ExprNode.(*plannercore.LogicalWindow)
	var byItems []property.Item
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	childProperty := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems}
	return prop.IsPrefix(childProperty)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (w *ImplWindow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	lw := expr.ExprNode.(*plannercore.LogicalWindow)
	var byItems []property.Item
	byItems = append(byItems, lw.PartitionBy...)
	byItems = append(byItems, lw.OrderBy...)
	physicalWindow := plannercore.PhysicalWindow{
		WindowFuncDescs: lw.WindowFuncDescs,
		PartitionBy:     lw.PartitionBy,
		OrderBy:         lw.OrderBy,
		Frame:           lw.Frame,
	}.Init(
		lw.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		lw.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: byItems},
	)
	physicalWindow.SetSchema(expr.Group.Prop.Schema)
	return []memo.Implementation{impl.NewWindowImpl(physicalWindow)}, nil
}
