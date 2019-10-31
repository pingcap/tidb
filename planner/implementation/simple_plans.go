// Copyright 2019 PingCAP, Inc.
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

package implementation

import (
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/memo"
)

// ProjectionImpl is the implementation of PhysicalProjection.
type ProjectionImpl struct {
	baseImpl
}

// NewProjectionImpl creates a new projection Implementation.
func NewProjectionImpl(proj *plannercore.PhysicalProjection) *ProjectionImpl {
	return &ProjectionImpl{baseImpl{plan: proj}}
}

// ShowImpl is the Implementation of PhysicalShow.
type ShowImpl struct {
	baseImpl
}

// NewShowImpl creates a new ShowImpl.
func NewShowImpl(show *plannercore.PhysicalShow) *ShowImpl {
	return &ShowImpl{baseImpl: baseImpl{plan: show}}
}

// TiDBSelectionImpl is the implementation of PhysicalSelection in TiDB layer.
type TiDBSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiDBSelectionImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	sel.cost = children[0].GetPlan().Stats().RowCount*sel.plan.SCtx().GetSessionVars().CPUFactor + children[0].GetCost()
	return sel.cost
}

// NewTiDBSelectionImpl creates a new TiDBSelectionImpl.
func NewTiDBSelectionImpl(sel *plannercore.PhysicalSelection) *TiDBSelectionImpl {
	return &TiDBSelectionImpl{baseImpl{plan: sel}}
}

// TiKVSelectionImpl is the implementation of PhysicalSelection in TiKV layer.
type TiKVSelectionImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (sel *TiKVSelectionImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	sel.cost = children[0].GetPlan().Stats().RowCount*sel.plan.SCtx().GetSessionVars().CopCPUFactor + children[0].GetCost()
	return sel.cost
}

// NewTiKVSelectionImpl creates a new TiKVSelectionImpl.
func NewTiKVSelectionImpl(sel *plannercore.PhysicalSelection) *TiKVSelectionImpl {
	return &TiKVSelectionImpl{baseImpl{plan: sel}}
}

// TiDBHashAggImpl is the implementation of PhysicalHashAgg in TiDB layer.
type TiDBHashAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiDBHashAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, true)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (agg *TiDBHashAggImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	hashAgg.SetChildren(children[0].GetPlan())
	// Inject extraProjection if the AggFuncs or GroupByItems contain ScalarFunction.
	plannercore.InjectProjBelowAgg(hashAgg, hashAgg.AggFuncs, hashAgg.GroupByItems)
	return agg
}

// NewTiDBHashAggImpl creates a new TiDBHashAggImpl.
func NewTiDBHashAggImpl(agg *plannercore.PhysicalHashAgg) *TiDBHashAggImpl {
	return &TiDBHashAggImpl{baseImpl{plan: agg}}
}

// LimitImpl is the implementation of PhysicalLimit. Since PhysicalLimit on different
// engines have the same behavior, and we don't calculate the cost of `Limit`, we only
// have one Implementation for it.
type LimitImpl struct {
	baseImpl
}

// NewLimitImpl creates a new LimitImpl.
func NewLimitImpl(limit *plannercore.PhysicalLimit) *LimitImpl {
	return &LimitImpl{baseImpl{plan: limit}}
}
