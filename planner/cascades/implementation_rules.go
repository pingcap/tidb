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
	OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error)
}

var defaultImplementationMap = map[memo.Operand][]ImplementationRule{
	memo.OperandTableDual: {
		&ImplTableDual{},
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
	memo.OperandTableGather: {
		&ImplTableGather{},
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
func (r *ImplTableDual) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicDual := expr.ExprNode.(*plannercore.LogicalTableDual)
	dual := plannercore.PhysicalTableDual{RowCount: logicDual.RowCount}.Init(logicDual.SCtx(), logicProp.Stats, logicDual.SelectBlockOffset())
	dual.SetSchema(logicProp.Schema)
	return impl.NewTableDualImpl(dual), nil
}

// ImplProjection implements LogicalProjection as PhysicalProjection.
type ImplProjection struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplProjection) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplProjection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
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
	return impl.NewProjectionImpl(proj), nil
}

// ImplTableGather implements TableGather as PhysicalTableReader.
type ImplTableGather struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTableGather) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	return true
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTableGather) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicProp := expr.Group.Prop
	tg := expr.ExprNode.(*plannercore.TableGather)
	if tg.IsIndexGather {
		reader := tg.GetPhysicalIndexReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
		return impl.NewIndexReaderImpl(reader, tg.Source.TblColHists), nil
	} else {
		reader := tg.GetPhysicalTableReader(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), reqProp)
		return impl.NewTableReaderImpl(reader, tg.Source.TblColHists), nil
	}
}

// ImplTableScan implements TableScan as PhysicalTableScan.
type ImplTableScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplTableScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	ts := expr.ExprNode.(*plannercore.TableScan)
	return prop.IsEmpty() || (len(prop.Items) == 1 && ts.Handle != nil && prop.Items[0].Col.Equal(nil, ts.Handle))
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplTableScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicProp := expr.Group.Prop
	logicalScan := expr.ExprNode.(*plannercore.TableScan)
	ts := logicalScan.GetPhysicalScan(logicProp.Schema, logicProp.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		ts.KeepOrder = true
		ts.Desc = reqProp.Items[0].Desc
	}
	tblCols, tblColHists := logicalScan.Source.TblCols, logicalScan.Source.TblColHists
	return impl.NewTableScanImpl(ts, tblCols, tblColHists), nil
}

// ImplIndexScan implements IndexScan as PhysicalIndexScan.
type ImplIndexScan struct {
}

// Match implements ImplementationRule Match interface.
func (r *ImplIndexScan) Match(expr *memo.GroupExpr, prop *property.PhysicalProperty) (matched bool) {
	is := expr.ExprNode.(*plannercore.IndexScan)
	return prop.IsEmpty() || is.MatchIndexProp(prop)
}

// OnImplement implements ImplementationRule OnImplement interface.
func (r *ImplIndexScan) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicalScan := expr.ExprNode.(*plannercore.IndexScan)
	is := logicalScan.GetPhysicalIndexScan(expr.Group.Prop.Schema, expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt))
	if !reqProp.IsEmpty() {
		is.KeepOrder = true
		if reqProp.Items[0].Desc {
			is.Desc = true
		}
	}
	return impl.NewIndexScanImpl(is, logicalScan.Source.TblColHists), nil
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
func (r *ImplShow) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicProp := expr.Group.Prop
	show := expr.ExprNode.(*plannercore.LogicalShow)

	// TODO(zz-jason): unifying LogicalShow and PhysicalShow to a single
	// struct. So that we don't need to create a new PhysicalShow object, which
	// can help us to reduce the gc pressure of golang runtime and improve the
	// overall performance.
	showPhys := plannercore.PhysicalShow{ShowContents: show.ShowContents}.Init(show.SCtx())
	showPhys.SetSchema(logicProp.Schema)
	return impl.NewShowImpl(showPhys), nil
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
func (r *ImplSelection) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	logicalSel := expr.ExprNode.(*plannercore.LogicalSelection)
	physicalSel := plannercore.PhysicalSelection{
		Conditions: logicalSel.Conditions,
	}.Init(logicalSel.SCtx(), expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt), logicalSel.SelectBlockOffset(), reqProp.Clone())
	switch expr.Group.EngineType {
	case memo.EngineTiDB:
		return impl.NewTiDBSelectionImpl(physicalSel), nil
	case memo.EngineTiKV:
		return impl.NewTiKVSelectionImpl(physicalSel), nil
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
func (r *ImplSort) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	ls := expr.ExprNode.(*plannercore.LogicalSort)
	if newProp, canUseNominal := plannercore.GetPropByOrderByItems(ls.ByItems); canUseNominal {
		newProp.ExpectedCnt = reqProp.ExpectedCnt
		ns := plannercore.NominalSort{}.Init(ls.SCtx(), ls.SelectBlockOffset(), newProp)
		return impl.NewNominalSortImpl(ns), nil
	}
	ps := plannercore.PhysicalSort{ByItems: ls.ByItems}.Init(
		ls.SCtx(),
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		ls.SelectBlockOffset(),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	return impl.NewSortImpl(ps), nil
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
func (r *ImplHashAgg) OnImplement(expr *memo.GroupExpr, reqProp *property.PhysicalProperty) (memo.Implementation, error) {
	la := expr.ExprNode.(*plannercore.LogicalAggregation)
	hashAgg := plannercore.NewPhysicalHashAgg(
		la,
		expr.Group.Prop.Stats.ScaleByExpectCnt(reqProp.ExpectedCnt),
		&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64},
	)
	hashAgg.SetSchema(expr.Group.Prop.Schema.Clone())
	// TODO: Implement TiKVHashAgg
	return impl.NewTiDBHashAggImpl(hashAgg), nil
}
