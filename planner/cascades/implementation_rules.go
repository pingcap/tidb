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

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	plannercore "github.com/pingcap/tidb/planner/core"
	impl "github.com/pingcap/tidb/planner/implementation"
	"github.com/pingcap/tidb/planner/memo"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/util/ranger"
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
	memo.OperandTiKVDoubleGather: {
		&ImplTiKVDoubleReadGather{},
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
		&ImplStreamAgg{},
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
		&ImplIndexJoin{},
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

// ImplTiKVDoubleReadGather implements TiKVDoubleGather as PhysicalIndexLookUpReader.
type ImplTiKVDoubleReadGather struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTiKVDoubleReadGather) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTiKVDoubleReadGather) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	logicProp := expr.Group.Prop
	dg := expr.ExprNode.(*plannercore.TiKVDoubleGather)
	var reader plannercore.PhysicalPlan
	var proj *plannercore.PhysicalProjection
	indexScanProp := reqProp.Clone()
	tableScanProp := reqProp.Clone()
	tableScanProp.Items = nil
	reader = dg.GetPhysicalIndexLookUpReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), indexScanProp, tableScanProp)
	// Since the handle column is not primary key and double read need to keep order. Then we need to inject a projection
	// to filter the tidb_rowid.
	if reader.Schema().ColumnIndex(dg.HandleCol) == -1 && !reqProp.IsEmpty() {
		reader.Schema().Append(dg.HandleCol)
		reader.(*plannercore.PhysicalIndexLookUpReader).ExtraHandleCol = dg.HandleCol
		proj = plannercore.PhysicalProjection{Exprs: expression.Column2Exprs(logicProp.Schema.Columns)}.Init(dg.SCtx(), logicProp.Stats, dg.SelectBlockOffset())
		proj.SetSchema(logicProp.Schema)
		proj.SetChildren(reader)
	}
	indexLookUp := impl.NewIndexLookUpReaderImpl(reader, dg.Source.TblColHists, proj)
	indexLookUp.KeepOrder = !reqProp.IsEmpty()
	return []memo.Implementation{indexLookUp}, nil
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
	ts, sel := logicalScan.GetPhysicalScan(logicProp.Schema, logicProp.Stats, reqProp)
	if !reqProp.IsEmpty() {
		ts.KeepOrder = true
		ts.Desc = reqProp.Items[0].Desc
	}
	tblCols, tblColHists := logicalScan.Source.TblCols, logicalScan.Source.TblColHists
	if sel != nil {
		return []memo.Implementation{impl.NewTableScanImpl(sel, tblCols, tblColHists)}, nil
	}
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
	is, sel := logicalScan.GetPhysicalIndexScan(expr.Group.Prop.Schema, expr.Group.Prop.Stats, reqProp)
	if !reqProp.IsEmpty() {
		is.KeepOrder = true
		if reqProp.Items[0].Desc {
			is.Desc = true
		}
	}
	if sel != nil {
		return []memo.Implementation{impl.NewIndexScanImpl(sel, logicalScan.Source.TblColHists)}, nil
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
		ns := plannercore.NominalSort{}.Init(
			ls.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), ls.SelectBlockOffset(), newProp)
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
	la := expr.ExprNode.(*plannercore.LogicalAggregation)
	preferHash, preferStream := la.ResetHintIfConflicted()
	return prop.IsEmpty() && (preferHash || !preferStream)
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

// ImplStreamAgg is the implementation rule which implements LogicalAggregation
// to PhysicalStreamAgg.
type ImplStreamAgg struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplStreamAgg) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	la := expr.ExprNode.(*plannercore.LogicalAggregation)
	all, _ := prop.AllSameOrder()
	if !all {
		return false
	}

	for _, aggFunc := range la.AggFuncs {
		if aggFunc.Mode == aggregation.FinalMode {
			return false
		}
	}

	// group by a + b is not interested in any order.
	if len(la.GetGroupByCols()) != len(la.GroupByItems) {
		return false
	}

	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplStreamAgg) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	switch expr.Group.EngineType {
	case memo.EngineTiDB:
		var newStreamAggImpl = func(agg *plannercore.PhysicalStreamAgg) memo.Implementation { return impl.NewTiDBStreamAggImpl(agg) }
		return r.getStreamAggImpls(expr, reqProp, newStreamAggImpl), nil
	case memo.EngineTiKV:
		var newStreamAggImpl = func(agg *plannercore.PhysicalStreamAgg) memo.Implementation { return impl.NewTiKVStreamAggImpl(agg) }
		return r.getStreamAggImpls(expr, reqProp, newStreamAggImpl), nil
	default:
		return nil, plannercore.ErrInternal.GenWithStack("Unsupported EngineType '%s' for StreamAggregation.", expr.Group.EngineType.String())
	}
}

func (r *ImplStreamAgg) getStreamAggImpls(expr *memo.GroupExpr, reqProp *property.PhysicalProperty, newStreamAggImpl func(*plannercore.PhysicalStreamAgg) memo.Implementation) []memo.Implementation {
	la := expr.ExprNode.(*plannercore.LogicalAggregation)
	physicalStreamAggs := r.getImplForStreamAgg(la, reqProp, expr.Schema(), expr.Group.Prop.Stats, expr.Children[0].Prop.Stats)

	streamAggImpls := make([]memo.Implementation, 0, len(physicalStreamAggs))
	for _, physicalPlan := range physicalStreamAggs {
		physicalStreamAgg := physicalPlan.(*plannercore.PhysicalStreamAgg)
		streamAggImpls = append(streamAggImpls, newStreamAggImpl(physicalStreamAgg))
	}

	return streamAggImpls
}

func (r *ImplStreamAgg) getImplForStreamAgg(la *plannercore.LogicalAggregation, prop *property.PhysicalProperty, schema *expression.Schema, statsInfo *property.StatsInfo, childStatsInfo *property.StatsInfo) []plannercore.PhysicalPlan {
	childProp := &property.PhysicalProperty{
		ExpectedCnt: math.Max(prop.ExpectedCnt*childStatsInfo.RowCount/statsInfo.RowCount, prop.ExpectedCnt),
	}
	_, desc := prop.AllSameOrder()

	streamAggs := make([]plannercore.PhysicalPlan, 0, len(la.GetPossibleProperties()))
	for _, possibleChildProperty := range la.GetPossibleProperties() {
		childProp.Items = property.ItemsFromCols(possibleChildProperty[:len(la.GetGroupByCols())], desc)
		if !prop.IsPrefix(childProp) {
			continue
		}

		agg := plannercore.NewPhysicalStreamAgg(la, statsInfo, prop, childProp.Clone())
		agg.SetSchema(schema.Clone())
		streamAggs = append(streamAggs, agg)
	}

	// If STREAM_AGG hint is existed, it should consider enforce stream aggregation,
	// because we can't trust possibleChildProperty completely.
	if la.IsPreferStream() {
		childProp.Items = property.ItemsFromCols(la.GetGroupByCols(), desc)
		childProp.Enforced = true
		// It's ok to not clone.
		streamAggs = append(streamAggs, plannercore.NewPhysicalStreamAgg(la, statsInfo, prop, childProp))
	}
	return streamAggs
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

// ImplIndexJoin implements LogicalJoin to PhysicalIndexJoin, PhysicalIndexHashJoin and
// PhysicalIndexMergeJoin.
type ImplIndexJoin struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplIndexJoin) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	all, _ := prop.AllSameOrder()
	return all
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplIndexJoin) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) ([]memo.Implementation, error) {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	var supportLeftOuter, supportRightOuter bool
	switch join.JoinType {
	case plannercore.SemiJoin, plannercore.AntiSemiJoin, plannercore.LeftOuterSemiJoin,
		plannercore.AntiLeftOuterSemiJoin, plannercore.LeftOuterJoin:
		supportLeftOuter = true
	case plannercore.RightOuterJoin:
		supportRightOuter = true
	case plannercore.InnerJoin:
		supportLeftOuter, supportRightOuter = true, true
	}

	impls := make([]memo.Implementation, 0)
	if supportLeftOuter {
		impls = append(impls, r.getIndexJoinByOuterIdx(expr, reqProp, 0)...)
	}

	if supportRightOuter {
		impls = append(impls, r.getIndexJoinByOuterIdx(expr, reqProp, 1)...)
	}
	return impls, nil
}

func (r *ImplIndexJoin) getIndexJoinByOuterIdx(expr *memo.GroupExpr, prop *property.PhysicalProperty, outerIdx int) []memo.Implementation {
	join := expr.ExprNode.(*plannercore.LogicalJoin)
	outerChildGroup, innerChildGroup := expr.Children[outerIdx], expr.Children[1-outerIdx]
	if !prop.AllColsFromSchema(outerChildGroup.Prop.Schema) {
		return nil
	}
	var outerJoinKeys, innerJoinKeys []*expression.Column
	if outerIdx == 0 {
		outerJoinKeys, innerJoinKeys = join.GetJoinKeys()
	} else {
		innerJoinKeys, outerJoinKeys = join.GetJoinKeys()
	}
	// TODO: Handle UnionScan when we support it in cascades planner.
	var avgInnerRowCnt float64
	if outerChildGroup.Prop.Stats.RowCount > 0 {
		avgInnerRowCnt = join.EqualCondOutCnt / outerChildGroup.Prop.Stats.RowCount
	}

	indexJoins := r.buildIndexJoinInner2TableScan(expr, prop, innerChildGroup, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
	// TODO: Support use IndexPath to build IndexJoin.
	return indexJoins
}

func (r *ImplIndexJoin) buildIndexJoinInner2TableScan(
	joinExpr *memo.GroupExpr, prop *property.PhysicalProperty, innerGroup *memo.Group,
	innerJoinKeys []*expression.Column, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) []memo.Implementation {
	// Step 1: Find the InnerSide TableReader(TiKVSingleGather without index).
	var tableGatherExpr *memo.GroupExpr
	for iter := innerGroup.Equivalents.Front(); iter != nil; iter = iter.Next() {
		expr := iter.Value.(*memo.GroupExpr)
		gather, ok := expr.ExprNode.(*plannercore.TiKVSingleGather)
		if !ok {
			continue
		}
		if !gather.IsIndexGather {
			// Notice: we assume there is only ONE TiKVSingleGather for LogicalTableScan.
			tableGatherExpr = expr
			break
		}
	}
	if tableGatherExpr == nil {
		return nil
	}
	// Step 2: Check whether the HandleColumn of LogicalTableScan is a JoinKey.
	tableScanExpr := r.findLeafGroupExpr(tableGatherExpr)
	tableScan := tableScanExpr.ExprNode.(*plannercore.LogicalTableScan)
	pkCol := tableScan.Handle
	if pkCol == nil {
		return nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	pkMatched := false
	for i, key := range innerJoinKeys {
		if !key.Equal(nil, pkCol) {
			keyOff2IdxOff[i] = -1
			continue
		}
		pkMatched = true
		keyOff2IdxOff[i] = 0
	}
	if !pkMatched {
		return nil
	}
	// Step 3: Build InnerSide Implementation.
	innerImpl := r.constructInnerTableScan(tableGatherExpr, tableScanExpr, outerJoinKeys, false, false, avgInnerRowCnt)

	// Step 4: Build IndexJoin Implementation.
	indexJoinImpl := r.constructIndexJoin(
		joinExpr, prop, outerIdx, innerImpl,
		innerJoinKeys, outerJoinKeys, nil,
		keyOff2IdxOff, nil, nil)
	// TODO: Support IndexHashJoin & IndexMergeJoin.

	return []memo.Implementation{indexJoinImpl}
}

func (r *ImplIndexJoin) findLeafGroupExpr(expr *memo.GroupExpr) *memo.GroupExpr {
	if len(expr.Children) == 0 {
		return expr
	}
	return r.findLeafGroupExpr(expr.Children[0].Equivalents.Front().Value.(*memo.GroupExpr))
}

func (r *ImplIndexJoin) constructIndexJoin(
	joinExpr *memo.GroupExpr, prop *property.PhysicalProperty, outerIdx int, innerImpl memo.Implementation,
	innerKeys []*expression.Column, outerKeys []*expression.Column, ranges []*ranger.Range,
	keyOff2IdxOff []int, idxColLens []int, cmpFilter *plannercore.ColWithCmpFuncManager) memo.Implementation {
	join := joinExpr.ExprNode.(*plannercore.LogicalJoin)
	chReqProps := make([]*property.PhysicalProperty, 2)
	chReqProps[outerIdx] = &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64, Items: prop.Items}
	joinStats := joinExpr.Group.Prop.Stats
	if prop.ExpectedCnt < joinStats.RowCount {
		expCntScale := prop.ExpectedCnt / joinStats.RowCount
		chReqProps[outerIdx].ExpectedCnt = joinExpr.Children[outerIdx].Prop.Stats.RowCount * expCntScale
	}
	newInnerKeys := make([]*expression.Column, 0, len(innerKeys))
	newOuterKeys := make([]*expression.Column, 0, len(outerKeys))
	newKeyOff := make([]int, 0, len(keyOff2IdxOff))
	newOtherConds := make([]expression.Expression, len(join.OtherConditions), len(join.OtherConditions)+len(join.EqualConditions))
	copy(newOtherConds, join.OtherConditions)
	for keyOff, idxOff := range keyOff2IdxOff {
		if keyOff2IdxOff[keyOff] < 0 {
			newOtherConds = append(newOtherConds, join.EqualConditions[keyOff])
			continue
		}
		newInnerKeys = append(newInnerKeys, innerKeys[keyOff])
		newOuterKeys = append(newOuterKeys, outerKeys[keyOff])
		newKeyOff = append(newKeyOff, idxOff)
	}
	indexJoin := plannercore.NewPhysicalIndexJoin(
		join, outerIdx, joinStats.ScaleByExpectCnt(prop.ExpectedCnt), chReqProps,
		newInnerKeys, newOuterKeys, newKeyOff, nil, newOtherConds,
		ranges, nil)
	indexJoin.IdxColLens = idxColLens
	indexJoin.SetSchema(joinExpr.Schema())
	return impl.NewIndexLookUpJoinImpl(indexJoin, innerImpl, outerIdx)
}

func (r *ImplIndexJoin) constructInnerTableScan(
	tableGatherExpr *memo.GroupExpr, tableScanExpr *memo.GroupExpr,
	outerJoinKeys []*expression.Column, keepOrder bool,
	desc bool, rowCount float64) memo.Implementation {
	gather := tableGatherExpr.ExprNode.(*plannercore.TiKVSingleGather)
	logicalTS := tableScanExpr.ExprNode.(*plannercore.LogicalTableScan)
	pk := logicalTS.Handle
	ranges := ranger.FullIntRange(mysql.HasUnsignedFlag(pk.RetType.Flag))
	ds := logicalTS.Source
	physicalTS := plannercore.PhysicalTableScan{
		Table:          ds.TableInfo(),
		Columns:        ds.Columns,
		TableAsName:    ds.TableAsName,
		DBName:         ds.DBName,
		Ranges:         ranges,
		RangeDecidedBy: outerJoinKeys,
		KeepOrder:      keepOrder,
		Desc:           desc,
	}.Init(logicalTS.SCtx(), logicalTS.SelectBlockOffset())
	physicalTS.SetSchema(tableScanExpr.Schema().Clone())

	filterConditions := r.getAllFilterConditions(tableGatherExpr)
	if rowCount <= 0 {
		rowCount = float64(1)
	}
	selectivity := float64(1)
	countAfterAccess := rowCount
	if len(filterConditions) > 0 {
		var err error
		selectivity, _, err = ds.TableStats.HistColl.Selectivity(ds.SCtx(), filterConditions, ds.GetAccessPaths())
		if err != nil || selectivity < 0 {
			selectivity = plannercore.SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterAccess * selectivity`.
		countAfterAccess = rowCount / selectivity
	}
	stats := &property.StatsInfo{
		// TableScan as inner child of IndexJoin can return at most 1 tuple for each outer row.
		RowCount:     math.Min(1.0, countAfterAccess),
		StatsVersion: tableScanExpr.Group.Prop.Stats.StatsVersion,
		// Cardinality would not be used in cost computation of IndexJoin, set leave it as default nil.
	}
	physicalTS.SetStats(stats)

	sessVars := ds.SCtx().GetSessionVars()
	rowSize := ds.TblColHists.GetTableAvgRowSize(physicalTS.SCtx(), ds.TblCols, physicalTS.StoreType, true)
	cost := sessVars.ScanFactor * rowSize * stats.RowCount

	var copImpl memo.Implementation
	if len(filterConditions) > 0 {
		sel := plannercore.PhysicalSelection{
			Conditions: filterConditions,
		}.Init(logicalTS.SCtx(), stats.Scale(selectivity), logicalTS.SelectBlockOffset())
		cost += stats.RowCount * sessVars.CopCPUFactor
		sel.SetChildren(physicalTS)
		selImpl := impl.NewTiKVSelectionImpl(sel)
		selImpl.SetCost(cost)
		copImpl = selImpl
	} else {
		tsImpl := impl.NewTableScanImpl(physicalTS, nil, nil)
		tsImpl.SetCost(cost)
		copImpl = tsImpl
	}
	reader := gather.GetPhysicalTableReader(tableGatherExpr.Schema(), copImpl.GetPlan().Stats(), nil)
	readerImpl := impl.NewTableReaderImpl(reader, ds.TblColHists)
	readerImpl.CalcCost(countAfterAccess, copImpl)
	readerImpl.AttachChildren(copImpl)
	return readerImpl
}

func (r *ImplIndexJoin) getAllFilterConditions(expr *memo.GroupExpr) []expression.Expression {
	switch plan := expr.ExprNode.(type) {
	case *plannercore.TiKVSingleGather:
		return r.getAllFilterConditions(expr.Children[0].GetFirstGroupExpr())
	case *plannercore.LogicalSelection:
		return append(plan.Conditions, r.getAllFilterConditions(expr.Children[0].GetFirstGroupExpr())...)
	case *plannercore.LogicalTableScan:
		return plan.AccessConds
	default:
		return nil
	}
}
