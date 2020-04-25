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
	"bytes"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"math"
	"sort"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
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
	if !reqProp.IsEmpty() && reader.Schema().ColumnIndex(dg.HandleCol) == -1 {
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

	//for _, aggFunc := range la.AggFuncs {
	//	if aggFunc.Mode == aggregation.FinalMode {
	//		return false
	//	}
	//}

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
	join := la.GetHashJoin(reqProp, expr.Group.Prop.Stats)
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
	if len(indexJoins) > 0 {
		return indexJoins
	}
    return r.buildIndexJoinInner2IndexScan(expr, prop, innerChildGroup, innerJoinKeys, outerJoinKeys, outerIdx, avgInnerRowCnt)
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

	joins := make([]memo.Implementation, 0, 3)
	// Step 4: Build IndexJoin Implementation.
	indexJoinImpl := r.constructIndexJoin(
		joinExpr, prop, outerIdx, innerImpl,
		innerJoinKeys, outerJoinKeys, nil,
		keyOff2IdxOff, nil, nil)
	joins = append(joins, indexJoinImpl)
	innerImplForIMJ := r.constructInnerTableScan(tableGatherExpr, tableScanExpr, outerJoinKeys, true, !prop.IsEmpty()&&prop.Items[0].Desc, avgInnerRowCnt)
	indexMergeJoinImpl := r.constructIndexMergeJoin(
		joinExpr, prop, outerIdx, innerImplForIMJ,
		innerJoinKeys, outerJoinKeys, nil,
		keyOff2IdxOff, nil, nil)
	if indexMergeJoinImpl != nil {
		joins = append(joins, indexMergeJoinImpl)
	}
	joins = append(joins,
		r.constructIndexHashJoin(joinExpr, prop, outerIdx, innerImpl,
		innerJoinKeys, outerJoinKeys, nil,
		keyOff2IdxOff, nil, nil))
	return joins
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

func (r *ImplIndexJoin) constructIndexMergeJoin(
	joinExpr *memo.GroupExpr, prop *property.PhysicalProperty, outerIdx int, innerImpl memo.Implementation,
	innerKeys []*expression.Column, outerKeys []*expression.Column, ranges []*ranger.Range,
	keyOff2IdxOff []int, idxColLens []int, cmpFilter *plannercore.ColWithCmpFuncManager)  memo.Implementation {
	indexJoinImpl := r.constructIndexJoin(joinExpr, prop, outerIdx, innerImpl, innerKeys, outerKeys, ranges, keyOff2IdxOff, idxColLens, cmpFilter)
	join := indexJoinImpl.GetPlan().(*plannercore.PhysicalIndexJoin)
	hasPrefixCol := false
	for _, l := range join.IdxColLens {
		if l != types.UnspecifiedLength {
			hasPrefixCol = true
			break
		}
	}
	if hasPrefixCol {
		return nil
	}
	keyOff2KeyOffOrderByIdx := make([]int, len(join.OuterJoinKeys))
	keyOffMapList := make([]int, len(join.KeyOff2IdxOff))
	copy(keyOffMapList, join.KeyOff2IdxOff)
	keyOffMap := make(map[int]int, len(keyOffMapList))
	for i, idxOff := range keyOffMapList {
		keyOffMap[idxOff] = i
	}
	sort.Slice(keyOffMapList, func(i, j int) bool { return keyOffMapList[i] < keyOffMapList[j] })
	for keyOff, idxOff := range keyOffMapList {
		keyOff2KeyOffOrderByIdx[keyOffMap[idxOff]] = keyOff
	}
	// isOuterKeysPrefix means whether the outer join keys are the prefix of the prop items.
	isOuterKeysPrefix := len(join.OuterJoinKeys) <= len(prop.Items)
	compareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))
	outerCompareFuncs := make([]expression.CompareFunc, 0, len(join.OuterJoinKeys))

	for i := range join.KeyOff2IdxOff {
		if isOuterKeysPrefix && !prop.Items[i].Col.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
			isOuterKeysPrefix = false
		}
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(join.SCtx(), join.OuterJoinKeys[i], join.InnerJoinKeys[i]))
		outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(join.SCtx(), join.OuterJoinKeys[i], join.OuterJoinKeys[i]))
	}
	// canKeepOuterOrder means whether the prop items are the prefix of the outer join keys.
	canKeepOuterOrder := len(prop.Items) <= len(join.OuterJoinKeys)
	for i := 0; canKeepOuterOrder && i < len(prop.Items); i++ {
		if !prop.Items[i].Col.Equal(nil, join.OuterJoinKeys[keyOff2KeyOffOrderByIdx[i]]) {
			canKeepOuterOrder = false
		}
	}
	// Since index merge join requires prop items the prefix of outer join keys
	// or outer join keys the prefix of the prop items. So we need `canKeepOuterOrder` or
	// `isOuterKeysPrefix` to be true.
	if canKeepOuterOrder || isOuterKeysPrefix {
		indexMergeJoin := plannercore.PhysicalIndexMergeJoin{
			PhysicalIndexJoin:       *join,
			KeyOff2KeyOffOrderByIdx: keyOff2KeyOffOrderByIdx,
			NeedOuterSort:           !isOuterKeysPrefix,
			CompareFuncs:            compareFuncs,
			OuterCompareFuncs:       outerCompareFuncs,
			Desc:                    !prop.IsEmpty() && prop.Items[0].Desc,
		}.Init(join.SCtx())
		return impl.NewIndexMergeJoinImpl(indexMergeJoin, innerImpl, outerIdx)
	}
	return nil
}

func (r *ImplIndexJoin) constructIndexHashJoin(
	joinExpr *memo.GroupExpr, prop *property.PhysicalProperty, outerIdx int, innerImpl memo.Implementation,
	innerKeys []*expression.Column, outerKeys []*expression.Column, ranges []*ranger.Range,
	keyOff2IdxOff []int, idxColLens []int, cmpFilter *plannercore.ColWithCmpFuncManager)  memo.Implementation {
	indexJoinImpl := r.constructIndexJoin(joinExpr, prop, outerIdx, innerImpl, innerKeys, outerKeys, ranges, keyOff2IdxOff, idxColLens, cmpFilter)
	join := indexJoinImpl.GetPlan().(*plannercore.PhysicalIndexJoin)
	indexHashJoin := plannercore.PhysicalIndexHashJoin{
		PhysicalIndexJoin: *join,
		// Prop is empty means that the parent operator does not need the
		// join operator to provide any promise of the output order.
		KeepOuterOrder: !prop.IsEmpty(),
	}.Init(join.SCtx())
	return impl.NewIndexHashJoinImpl(indexHashJoin, innerImpl, outerIdx)
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

// constructInnerIndexScanTask is specially used to construct the inner plan for PhysicalIndexJoin.
func (r *ImplIndexJoin) constructInnerIndexScan(
	path *indexJoinAccessPath, filterConds []expression.Expression,
	outerJoinKeys []*expression.Column, rangeInfo string,
	keepOrder bool, desc bool, rowCount float64, maxOneRow bool) memo.Implementation {
	ds := path.indexScan.Source
	is := plannercore.PhysicalIndexScan{
		Table:   ds.GetTableInfo(),
		TableAsName: ds.TableAsName,
		DBName:      ds.DBName,
		Columns:     path.indexScan.Columns,
		Index:       path.indexScan.Index,
		IdxCols:     path.indexScan.IdxCols,
		IdxColLens:  path.indexScan.IdxColLens,
		DataSourceSchema: ds.Schema(),
		KeepOrder:   keepOrder,
		Ranges:      ranger.FullRange(),
		RangeInfo:   rangeInfo,
		Desc:        desc,
		PhysicalTableID: ds.GetPhysicalTableID(),
	}.Init(ds.SCtx(), ds.SelectBlockOffset())
	is.InitSchema(is.Index, path.indexScan.FullIdxCols, path.tableScan != nil)
	indexConds, tblConds := plannercore.SplitIndexFilterConditions(filterConds, path.indexScan.FullIdxCols, path.indexScan.FullIdxColLens, is.Table)
	// Specially handle cases when input rowCount is 0, which can only happen in 2 scenarios:
	// - estimated row count of outer plan is 0;
	// - estimated row count of inner "DataSource + filters" is 0;
	// if it is the first case, it does not matter what row count we set for inner task, since the cost of index join would
	// always be 0 then;
	// if it is the second case, HashJoin should always be cheaper than IndexJoin then, so we set row count of inner task
	// to table size, to simply make it more expensive.
	if rowCount <= 0 {
		rowCount = ds.TableStats.RowCount
	}
	if maxOneRow {
		// Theoretically, this line is unnecessary because row count estimation of join should guarantee rowCount is not larger
		// than 1.0; however, there may be rowCount larger than 1.0 in reality, e.g, pseudo statistics cases, which does not reflect
		// unique constraint in NDV.
		rowCount = math.Min(rowCount, 1.0)
	}
	finalStats := ds.TableStats.ScaleByExpectCnt(rowCount)
	tmpPath := &util.AccessPath{
		IndexFilters:     indexConds,
		TableFilters:     tblConds,
		CountAfterIndex:  rowCount,
		CountAfterAccess: rowCount,
	}
	// Assume equal conditions used by index join and other conditions are independent.
	if len(tblConds) > 0 {
		selectivity, _, err := ds.TableStats.HistColl.Selectivity(ds.SCtx(), tblConds, nil)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = plannercore.SelectionFactor
		}
		// rowCount is computed from result row count of join, which has already accounted the filters on DataSource,
		// i.e, rowCount equals to `countAfterIndex * selectivity`.
		cnt := rowCount / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterIndex = cnt
		tmpPath.CountAfterAccess = cnt
	}
	if len(indexConds) > 0 {
		selectivity, _, err := ds.TableStats.HistColl.Selectivity(ds.SCtx(), indexConds, nil)
		if err != nil || selectivity <= 0 {
			logutil.BgLogger().Debug("unexpected selectivity, use selection factor", zap.Float64("selectivity", selectivity), zap.String("table", ds.TableAsName.L))
			selectivity = plannercore.SelectionFactor
		}
		cnt := tmpPath.CountAfterIndex / selectivity
		if maxOneRow {
			cnt = math.Min(cnt, 1.0)
		}
		tmpPath.CountAfterAccess = cnt
	}
	is.SetStats(ds.TableStats.ScaleByExpectCnt(tmpPath.CountAfterAccess))
	var indexPathImpl memo.Implementation
	indexPathImpl = impl.NewIndexScanImpl(is, ds.TblColHists)
	indexPathImpl.CalcCost(tmpPath.CountAfterAccess, nil)
	if len(indexConds) > 0 {
		indexPathSel := plannercore.PhysicalSelection{
			Conditions: indexConds,
		}.Init(ds.SCtx(), ds.TableStats.ScaleByExpectCnt(tmpPath.CountAfterIndex), ds.SelectBlockOffset())
		selImpl := impl.NewTiKVSelectionImpl(indexPathSel)
		selImpl.CalcCost(tmpPath.CountAfterIndex, indexPathImpl)
		indexPathImpl = selImpl.AttachChildren(indexPathImpl)
	}

	if path.tableScan == nil {
		gather := path.gatherExpr.ExprNode.(*plannercore.TiKVSingleGather)
		reader := gather.GetPhysicalIndexReader(path.gatherExpr.Schema(), ds.TableStats.ScaleByExpectCnt(tmpPath.CountAfterIndex))
		indexReaderImpl := impl.NewIndexReaderImpl(reader, ds.TblColHists)
		indexReaderImpl.CalcCost(rowCount, indexPathImpl)
		indexReaderImpl.AttachChildren(indexPathImpl)
		return indexReaderImpl
	}
	// index lookup reader
	gather := path.gatherExpr.ExprNode.(*plannercore.TiKVDoubleGather)
	ts := plannercore.PhysicalTableScan{
		Columns:	     ds.Columns,
		Table:   		 is.Table,
		TableAsName: 	 ds.TableAsName,
		HandleCol:       path.tableScan.Handle,
		PhysicalTableID: ds.GetPhysicalTableID(),
	}.Init(ds.SCtx(), ds.SelectBlockOffset())
	ts.SetStats(ds.TableStats.ScaleByExpectCnt(tmpPath.CountAfterIndex))
	ts.SetSchema(is.DataSourceSchema.Clone())
	var tsImpl memo.Implementation = impl.NewTableScanImpl(ts, ds.TblCols, ds.TblColHists)
	tsImpl.CalcCost(rowCount)
	if len(tblConds) > 0 {
		sel := plannercore.PhysicalSelection{
			Conditions:tblConds,
		}.Init(ds.SCtx(), finalStats, ds.SelectBlockOffset())
		selImpl := impl.NewTiKVSelectionImpl(sel)
		selImpl.CalcCost(finalStats.RowCount, tsImpl)
		selImpl.AttachChildren(tsImpl)
		tsImpl = selImpl
	}
	reader := gather.GetPhysicalIndexLookUpReader(path.gatherExpr.Schema().Clone(), finalStats)
	var proj *plannercore.PhysicalProjection
	if keepOrder && gather.HandleCol == nil {
		return nil
	}
	if keepOrder && !reader.Schema().Contains(gather.HandleCol) {
		reader.Schema().Append(gather.HandleCol)
		reader.ExtraHandleCol = gather.HandleCol
		proj = plannercore.PhysicalProjection{Exprs: expression.Column2Exprs(path.gatherExpr.Schema().Columns)}.Init(gather.SCtx(), reader.Stats(), reader.SelectBlockOffset())
		proj.SetSchema(path.gatherExpr.Schema())
		proj.SetChildren(reader)
	}
	readerImpl := impl.NewIndexLookUpReaderImpl(reader, ds.TblColHists, proj)
	readerImpl.CalcCost(finalStats.RowCount, indexPathImpl, tsImpl)
	readerImpl.AttachChildren(indexPathImpl, tsImpl)
	return readerImpl
}

func (r *ImplIndexJoin) getAllFilterConditions(expr *memo.GroupExpr) []expression.Expression {
	switch plan := expr.ExprNode.(type) {
	case *plannercore.TiKVSingleGather:
		return r.getAllFilterConditions(expr.Children[0].GetFirstGroupExpr())
	case *plannercore.LogicalSelection:
		return append(plan.Conditions, r.getAllFilterConditions(expr.Children[0].GetFirstGroupExpr())...)
	case *plannercore.LogicalTableScan:
		return plan.AllConds
	default:
		return nil
	}
}

type indexJoinBuildHelper struct {
	joinExpr *memo.GroupExpr

	chosenIndexInfo *model.IndexInfo
	maxUsedCols     int
	chosenAccess    []expression.Expression
	chosenRemained  []expression.Expression
	idxOff2KeyOff   []int
	lastColManager  *plannercore.ColWithCmpFuncManager
	chosenRanges    []*ranger.Range
	chosenPath		*indexJoinAccessPath
	chosenIndex     *model.IndexInfo

	curPossibleUsedKeys []*expression.Column
	curNotUsedIndexCols []*expression.Column
	curNotUsedColLens   []int
	curIdxOff2KeyOff    []int
}

type indexJoinAccessPath struct {
	gatherExpr *memo.GroupExpr
	indexScan *plannercore.LogicalIndexScan
	tableScan *plannercore.LogicalTableScan
	conditions    []expression.Expression
}

func (r *ImplIndexJoin) collectIndexPath(group *memo.Group) []*indexJoinAccessPath {
	res := make([]*indexJoinAccessPath, 0, group.Equivalents.Len())
	for iter := group.Equivalents.Front(); iter != nil; iter = iter.Next() {
		expr := iter.Value.(*memo.GroupExpr)
		switch gather := expr.ExprNode.(type) {
		case *plannercore.TiKVSingleGather:
			if !gather.IsIndexGather {
				continue
			}
			if idxScan, ok := expr.Children[0].GetFirstGroupExpr().ExprNode.(*plannercore.LogicalIndexScan); ok {
				path := indexJoinAccessPath{gatherExpr:expr, indexScan:idxScan}
				path.conditions = idxScan.AllConds
				res = append(res, &path)
			}
		case *plannercore.TiKVDoubleGather:
			idxScan, ok := expr.Children[0].GetFirstGroupExpr().ExprNode.(*plannercore.LogicalIndexScan)
			if !ok {
				continue
			}
			tblScan, ok := expr.Children[1].GetFirstGroupExpr().ExprNode.(*plannercore.LogicalTableScan)
			if !ok {
				continue
			}
			path := indexJoinAccessPath{gatherExpr:expr, indexScan:idxScan, tableScan:tblScan}
			path.conditions = make([]expression.Expression, 0, len(idxScan.AllConds)+len(tblScan.AllConds))
			path.conditions = append(path.conditions, idxScan.AllConds...)
			path.conditions = append(path.conditions, tblScan.AllConds...)
			res = append(res, &path)
		default:
			continue
		}
	}
	return res
}

func(r *ImplIndexJoin) buildIndexJoinInner2IndexScan(
	joinExpr *memo.GroupExpr, prop *property.PhysicalProperty, innerGroup *memo.Group,
	innerJoinKeys []*expression.Column, outerJoinKeys []*expression.Column,
	outerIdx int, avgInnerRowCnt float64) []memo.Implementation {
	helper := &indexJoinBuildHelper{joinExpr:joinExpr}
	indexPaths := r.collectIndexPath(innerGroup)
	for _, path := range indexPaths {
		emptyRange, _ := helper.analyzeLookUpFilters(path, innerGroup, innerJoinKeys)
		if emptyRange {
			return nil
		}
	}
	if helper.chosenPath == nil {
		return nil
	}
	keyOff2IdxOff := make([]int, len(innerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = -1
	}
	for idxOff, keyOff := range helper.idxOff2KeyOff {
		if keyOff != -1 {
			keyOff2IdxOff[keyOff] = idxOff
		}
	}
	impls := make([]memo.Implementation, 0, 3)
	rangeInfo := helper.buildRangeDecidedByInformation(helper.chosenPath.indexScan.IdxCols, outerJoinKeys)
	maxOneRow := false
	if helper.chosenPath.indexScan.Index.Unique && helper.maxUsedCols == len(helper.chosenPath.indexScan.FullIdxCols) {
		l := len(helper.chosenAccess)
		if l == 0 {
			maxOneRow = true
		} else {
			sf, ok := helper.chosenAccess[l-1].(*expression.ScalarFunction)
			maxOneRow = ok && (sf.FuncName.L == ast.EQ)
		}
	}
	innerImpl := r.constructInnerIndexScan(helper.chosenPath, helper.chosenRemained, outerJoinKeys, rangeInfo, false, false, avgInnerRowCnt, maxOneRow)
	impls = append(impls, r.constructIndexJoin(joinExpr, prop, outerIdx, innerImpl, innerJoinKeys, outerJoinKeys, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath.indexScan.IdxColLens, helper.lastColManager))
	impls = append(impls, r.constructIndexHashJoin(joinExpr, prop, outerIdx, innerImpl, innerJoinKeys, outerJoinKeys, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath.indexScan.IdxColLens, helper.lastColManager))
	innerImplForILMJ := r.constructInnerIndexScan(helper.chosenPath, helper.chosenRemained, outerJoinKeys, rangeInfo, true, !prop.IsEmpty() && prop.Items[0].Desc, avgInnerRowCnt, maxOneRow)
	impls = append(impls, r.constructIndexMergeJoin(joinExpr, prop, outerIdx, innerImplForILMJ, innerJoinKeys, outerJoinKeys, helper.chosenRanges, keyOff2IdxOff, helper.chosenPath.indexScan.IdxColLens, helper.lastColManager))
	return impls
}

func (ijHelper *indexJoinBuildHelper) buildRangeDecidedByInformation(idxCols []*expression.Column, outerJoinKeys []*expression.Column) string {
	buffer := bytes.NewBufferString("[")
	isFirst := true
	for idxOff, keyOff := range ijHelper.idxOff2KeyOff {
		if keyOff == -1 {
			continue
		}
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("eq(%v, %v)", idxCols[idxOff], outerJoinKeys[keyOff]))
	}
	for _, access := range ijHelper.chosenAccess {
		if !isFirst {
			buffer.WriteString(" ")
		} else {
			isFirst = false
		}
		buffer.WriteString(fmt.Sprintf("%v", access))
	}
	buffer.WriteString("]")
	return buffer.String()
}

func (ijHelper *indexJoinBuildHelper) resetContextForIndex(innerKeys []*expression.Column, idxCols []*expression.Column, colLens []int) {
	tmpSchema := expression.NewSchema(innerKeys...)
	ijHelper.curIdxOff2KeyOff = make([]int, len(idxCols))
	ijHelper.curNotUsedIndexCols = make([]*expression.Column, 0, len(idxCols))
	ijHelper.curNotUsedColLens = make([]int, 0, len(idxCols))
	for i, idxCol := range idxCols {
		ijHelper.curIdxOff2KeyOff[i] = tmpSchema.ColumnIndex(idxCol)
		if ijHelper.curIdxOff2KeyOff[i] >= 0 {
			continue
		}
		ijHelper.curNotUsedIndexCols = append(ijHelper.curNotUsedIndexCols, idxCol)
		ijHelper.curNotUsedColLens = append(ijHelper.curNotUsedColLens, colLens[i])
	}
}

// findUsefulEqAndInFilters analyzes the pushedDownConds held by inner child and split them to three parts.
// usefulEqOrInFilters is the continuous eq/in conditions on current unused index columns.
// uselessFilters is the conditions which cannot be used for building ranges.
// remainingRangeCandidates is the other conditions for future use.
func (ijHelper *indexJoinBuildHelper) findUsefulEqAndInFilters(conds []expression.Expression) (usefulEqOrInFilters, uselessFilters, remainingRangeCandidates []expression.Expression) {
	uselessFilters = make([]expression.Expression, 0, len(conds))
	var remainedEqOrIn []expression.Expression
	// Extract the eq/in functions of possible join key.
	// you can see the comment of ExtractEqAndInCondition to get the meaning of the second return value.
	usefulEqOrInFilters, remainedEqOrIn, remainingRangeCandidates, _ = ranger.ExtractEqAndInCondition(
		ijHelper.joinExpr.ExprNode.SCtx(), conds,
		ijHelper.curNotUsedIndexCols,
		ijHelper.curNotUsedColLens,
	)
	uselessFilters = append(uselessFilters, remainedEqOrIn...)
	return usefulEqOrInFilters, uselessFilters, remainingRangeCandidates
}

// removeUselessEqAndInFunc removes the useless eq/in conditions. It's designed for the following case:
//   t1 join t2 on t1.a=t2.a and t1.c=t2.c where t1.b > t2.b-10 and t1.b < t2.b+10 there's index(a, b, c) on t1.
//   In this case the curIdxOff2KeyOff is [0 -1 1] and the notKeyEqAndIn is [].
//   It's clearly that the column c cannot be used to access data. So we need to remove it and reset the IdxOff2KeyOff to
//   [0 -1 -1].
//   So that we can use t1.a=t2.a and t1.b > t2.b-10 and t1.b < t2.b+10 to build ranges then access data.
func (ijHelper *indexJoinBuildHelper) removeUselessEqAndInFunc(
	idxCols []*expression.Column,
	notKeyEqAndIn []expression.Expression) (
	usefulEqAndIn, uselessOnes []expression.Expression,
) {
	ijHelper.curPossibleUsedKeys = make([]*expression.Column, 0, len(idxCols))
	for idxColPos, notKeyColPos := 0, 0; idxColPos < len(idxCols); idxColPos++ {
		if ijHelper.curIdxOff2KeyOff[idxColPos] != -1 {
			ijHelper.curPossibleUsedKeys = append(ijHelper.curPossibleUsedKeys, idxCols[idxColPos])
			continue
		}
		if notKeyColPos < len(notKeyEqAndIn) && ijHelper.curNotUsedIndexCols[notKeyColPos].Equal(nil, idxCols[idxColPos]) {
			notKeyColPos++
			continue
		}
		for i := idxColPos + 1; i < len(idxCols); i++ {
			ijHelper.curIdxOff2KeyOff[i] = -1
		}
		remained := make([]expression.Expression, 0, len(notKeyEqAndIn)-notKeyColPos)
		remained = append(remained, notKeyEqAndIn[notKeyColPos:]...)
		notKeyEqAndIn = notKeyEqAndIn[:notKeyColPos]
		return notKeyEqAndIn, remained
	}
	return notKeyEqAndIn, nil
}

func (ijHelper *indexJoinBuildHelper) buildTemplateRange(matchedKeyCnt int, eqAndInFuncs []expression.Expression, nextColRange []*ranger.Range, haveExtraCol bool) (ranges []*ranger.Range, emptyRange bool, err error) {
	pointLength := matchedKeyCnt + len(eqAndInFuncs)
	if nextColRange != nil {
		for _, colRan := range nextColRange {
			// The range's exclude status is the same with last col's.
			ran := &ranger.Range{
				LowVal:      make([]types.Datum, pointLength, pointLength+1),
				HighVal:     make([]types.Datum, pointLength, pointLength+1),
				LowExclude:  colRan.LowExclude,
				HighExclude: colRan.HighExclude,
			}
			ran.LowVal = append(ran.LowVal, colRan.LowVal[0])
			ran.HighVal = append(ran.HighVal, colRan.HighVal[0])
			ranges = append(ranges, ran)
		}
	} else if haveExtraCol {
		// Reserve a position for the last col.
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength+1),
			HighVal: make([]types.Datum, pointLength+1),
		})
	} else {
		ranges = append(ranges, &ranger.Range{
			LowVal:  make([]types.Datum, pointLength),
			HighVal: make([]types.Datum, pointLength),
		})
	}
	sc := ijHelper.joinExpr.ExprNode.SCtx().GetSessionVars().StmtCtx
	for i, j := 0, 0; j < len(eqAndInFuncs); i++ {
		// This position is occupied by join key.
		if ijHelper.curIdxOff2KeyOff[i] != -1 {
			continue
		}
		oneColumnRan, err := ranger.BuildColumnRange([]expression.Expression{eqAndInFuncs[j]}, sc, ijHelper.curNotUsedIndexCols[j].RetType, ijHelper.curNotUsedColLens[j])
		if err != nil {
			return nil, false, err
		}
		if len(oneColumnRan) == 0 {
			return nil, true, nil
		}
		for _, ran := range ranges {
			ran.LowVal[i] = oneColumnRan[0].LowVal[0]
			ran.HighVal[i] = oneColumnRan[0].HighVal[0]
		}
		curRangeLen := len(ranges)
		for ranIdx := 1; ranIdx < len(oneColumnRan); ranIdx++ {
			newRanges := make([]*ranger.Range, 0, curRangeLen)
			for oldRangeIdx := 0; oldRangeIdx < curRangeLen; oldRangeIdx++ {
				newRange := ranges[oldRangeIdx].Clone()
				newRange.LowVal[i] = oneColumnRan[ranIdx].LowVal[0]
				newRange.HighVal[i] = oneColumnRan[ranIdx].HighVal[0]
				newRanges = append(newRanges, newRange)
			}
			ranges = append(ranges, newRanges...)
		}
		j++
	}
	return ranges, false, nil
}

func (ijHelper *indexJoinBuildHelper) updateBestChoice(ranges []*ranger.Range, path *indexJoinAccessPath, accesses,
	remained []expression.Expression, lastColManager *plannercore.ColWithCmpFuncManager) {
	// We choose the index by the number of used columns of the range, the much the better.
	// Notice that there may be the cases like `t1.a=t2.a and b > 2 and b < 1`. So ranges can be nil though the conditions are valid.
	// But obviously when the range is nil, we don't need index join.
	if len(ranges) > 0 && len(ranges[0].LowVal) > ijHelper.maxUsedCols {
		ijHelper.chosenPath = path
		ijHelper.maxUsedCols = len(ranges[0].LowVal)
		ijHelper.chosenRanges = ranges
		ijHelper.chosenAccess = accesses
		ijHelper.chosenRemained = remained
		ijHelper.idxOff2KeyOff = ijHelper.curIdxOff2KeyOff
		ijHelper.lastColManager = lastColManager
	}
}

var symmetricOp = map[string]string{
	ast.LT: ast.GT,
	ast.GE: ast.LE,
	ast.GT: ast.LT,
	ast.LE: ast.GE,
}

// buildLastColManager analyze the `OtherConditions` of join to see whether there're some filters can be used in manager.
// The returned value is just for outputting explain information
func (ijHelper *indexJoinBuildHelper) buildLastColManager(nextCol *expression.Column,
	innerSchema *expression.Schema, cwc *plannercore.ColWithCmpFuncManager) []expression.Expression {
	var lastColAccesses []expression.Expression
	join := ijHelper.joinExpr.ExprNode.(*plannercore.LogicalJoin)
loopOtherConds:
	for _, filter := range join.OtherConditions {
		sf, ok := filter.(*expression.ScalarFunction)
		if !ok || !(sf.FuncName.L == ast.LE || sf.FuncName.L == ast.LT || sf.FuncName.L == ast.GE || sf.FuncName.L == ast.GT) {
			continue
		}
		var funcName string
		var anotherArg expression.Expression
		if lCol, ok := sf.GetArgs()[0].(*expression.Column); ok && lCol.Equal(nil, nextCol) {
			anotherArg = sf.GetArgs()[1]
			funcName = sf.FuncName.L
		} else if rCol, ok := sf.GetArgs()[1].(*expression.Column); ok && rCol.Equal(nil, nextCol) {
			anotherArg = sf.GetArgs()[0]
			// The column manager always build expression in the form of col op arg1.
			// So we need use the symmetric one of the current function.
			funcName = symmetricOp[sf.FuncName.L]
		} else {
			continue
		}
		affectedCols := expression.ExtractColumns(anotherArg)
		if len(affectedCols) == 0 {
			continue
		}
		for _, col := range affectedCols {
			if innerSchema.Contains(col) {
				continue loopOtherConds
			}
		}
		lastColAccesses = append(lastColAccesses, sf)
		cwc.AppendNewExpr(funcName, anotherArg, affectedCols)
	}
	return lastColAccesses
}

func (ijHelper *indexJoinBuildHelper) analyzeLookUpFilters(path *indexJoinAccessPath, innerGroup *memo.Group, innerJoinKeys []*expression.Column) (emptyRange bool, err error) {
	if len(path.indexScan.IdxCols) == 0 {
		return false, nil
	}
	ctx := ijHelper.joinExpr.ExprNode.SCtx()
	accesses := make([]expression.Expression, 0, len(path.indexScan.IdxCols))
	ijHelper.resetContextForIndex(innerJoinKeys, path.indexScan.IdxCols, path.indexScan.IdxColLens)
	notKeyEqAndIn, remained, rangeFilterCandidates := ijHelper.findUsefulEqAndInFilters(path.conditions)
	var remainedEqAndIn []expression.Expression
	notKeyEqAndIn, remainedEqAndIn = ijHelper.removeUselessEqAndInFunc(path.indexScan.IdxCols, notKeyEqAndIn)
	matchedKeyCnt := len(ijHelper.curPossibleUsedKeys)
	// If no join key is matched while join keys actually are not empty. We don't choose index join for now.
	if matchedKeyCnt <= 0 && len(innerJoinKeys) > 0 {
		return false, nil
	}
	accesses = append(accesses, notKeyEqAndIn...)
	remained = append(remained, remainedEqAndIn...)
	lastColPos := matchedKeyCnt + len(notKeyEqAndIn)
	// There should be some equal conditions. But we don't need that there must be some join key in accesses here.
	// A more strict check is applied later.
	if lastColPos <= 0 {
		return false, nil
	}
	// If all the index columns are covered by eq/in conditions, we don't need to consider other conditions anymore.
	if lastColPos == len(path.indexScan.IdxCols) {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1. And t2 has index(a, b).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		remained = append(remained, rangeFilterCandidates...)
		ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		ijHelper.updateBestChoice(ranges, path, accesses, remained, nil)
		return false, nil
	}
	lastPossibleCol := path.indexScan.IdxCols[lastColPos]
	lastColManager := &plannercore.ColWithCmpFuncManager{
		TargetCol:         lastPossibleCol,
		ColLength:         path.indexScan.IdxColLens[lastColPos],
		AffectedColSchema: expression.NewSchema(),
	}
	lastColAccess := ijHelper.buildLastColManager(lastPossibleCol, innerGroup.Prop.Schema, lastColManager)
	// If the column manager holds no expression, then we fallback to find whether there're useful normal filters
	if len(lastColAccess) == 0 {
		// If there's join key matched index column. Then choose hash join is always a better idea.
		// e.g. select * from t1, t2 where t2.a=1 and t2.b=1 and t2.c > 10 and t2.c < 20. And t2 has index(a, b, c).
		//      If we don't have the following check, TiDB will build index join for this case.
		if matchedKeyCnt <= 0 {
			return false, nil
		}
		colAccesses, colRemained := ranger.DetachCondsForColumn(ctx, rangeFilterCandidates, lastPossibleCol)
		var ranges, nextColRange []*ranger.Range
		var err error
		if len(colAccesses) > 0 {
			nextColRange, err = ranger.BuildColumnRange(colAccesses, ctx.GetSessionVars().StmtCtx, lastPossibleCol.RetType, path.indexScan.IdxColLens[lastColPos])
			if err != nil {
				return false, err
			}
		}
		ranges, emptyRange, err = ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nextColRange, false)
		if err != nil {
			return false, err
		}
		if emptyRange {
			return true, nil
		}
		remained = append(remained, colRemained...)
		if path.indexScan.IdxColLens[lastColPos] != types.UnspecifiedLength {
			remained = append(remained, colAccesses...)
		}
		accesses = append(accesses, colAccesses...)
		ijHelper.updateBestChoice(ranges, path, accesses, remained, nil)
		return false, nil
	}
	accesses = append(accesses, lastColAccess...)
	remained = append(remained, rangeFilterCandidates...)
	ranges, emptyRange, err := ijHelper.buildTemplateRange(matchedKeyCnt, notKeyEqAndIn, nil, true)
	if err != nil {
		return false, err
	}
	if emptyRange {
		return true, nil
	}
	ijHelper.updateBestChoice(ranges, path, accesses, remained, lastColManager)
	return false, nil
}
