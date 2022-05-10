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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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

// CalcCost implements Implementation CalcCost interface.
func (impl *ProjectionImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	proj := impl.plan.(*plannercore.PhysicalProjection)
	impl.cost = proj.GetCost(children[0].GetPlan().Stats().RowCount) + children[0].GetCost()
	return impl.cost
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
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, true, false, 0)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (agg *TiDBHashAggImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	hashAgg.SetChildren(children[0].GetPlan())
	return agg
}

// NewTiDBHashAggImpl creates a new TiDBHashAggImpl.
func NewTiDBHashAggImpl(agg *plannercore.PhysicalHashAgg) *TiDBHashAggImpl {
	return &TiDBHashAggImpl{baseImpl{plan: agg}}
}

// TiKVHashAggImpl is the implementation of PhysicalHashAgg in TiKV layer.
type TiKVHashAggImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (agg *TiKVHashAggImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashAgg := agg.plan.(*plannercore.PhysicalHashAgg)
	selfCost := hashAgg.GetCost(children[0].GetPlan().Stats().RowCount, false, false, 0)
	agg.cost = selfCost + children[0].GetCost()
	return agg.cost
}

// NewTiKVHashAggImpl creates a new TiKVHashAggImpl.
func NewTiKVHashAggImpl(agg *plannercore.PhysicalHashAgg) *TiKVHashAggImpl {
	return &TiKVHashAggImpl{baseImpl{plan: agg}}
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

// TiDBTopNImpl is the implementation of PhysicalTopN in TiDB layer.
type TiDBTopNImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *TiDBTopNImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	topN := impl.plan.(*plannercore.PhysicalTopN)
	childCount := children[0].GetPlan().Stats().RowCount
	impl.cost = topN.GetCost(childCount, true) + children[0].GetCost()
	return impl.cost
}

// NewTiDBTopNImpl creates a new TiDBTopNImpl.
func NewTiDBTopNImpl(topN *plannercore.PhysicalTopN) *TiDBTopNImpl {
	return &TiDBTopNImpl{baseImpl{plan: topN}}
}

// TiKVTopNImpl is the implementation of PhysicalTopN in TiKV layer.
type TiKVTopNImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *TiKVTopNImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	topN := impl.plan.(*plannercore.PhysicalTopN)
	childCount := children[0].GetPlan().Stats().RowCount
	impl.cost = topN.GetCost(childCount, false) + children[0].GetCost()
	return impl.cost
}

// NewTiKVTopNImpl creates a new TiKVTopNImpl.
func NewTiKVTopNImpl(topN *plannercore.PhysicalTopN) *TiKVTopNImpl {
	return &TiKVTopNImpl{baseImpl{plan: topN}}
}

// UnionAllImpl is the implementation of PhysicalUnionAll.
type UnionAllImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *UnionAllImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	var childMaxCost float64
	for _, child := range children {
		childCost := child.GetCost()
		if childCost > childMaxCost {
			childMaxCost = childCost
		}
	}
	selfCost := float64(1+len(children)) * impl.plan.SCtx().GetSessionVars().ConcurrencyFactor
	// Children of UnionAll are executed in parallel.
	impl.cost = selfCost + childMaxCost
	return impl.cost
}

// GetCostLimit implements Implementation interface.
func (impl *UnionAllImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	return costLimit
}

// NewUnionAllImpl creates a new UnionAllImpl.
func NewUnionAllImpl(union *plannercore.PhysicalUnionAll) *UnionAllImpl {
	return &UnionAllImpl{baseImpl{plan: union}}
}

// ApplyImpl is the implementation of PhysicalApply.
type ApplyImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *ApplyImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	apply := impl.plan.(*plannercore.PhysicalApply)
	impl.cost = apply.GetCost(
		children[0].GetPlan().Stats().RowCount,
		children[1].GetPlan().Stats().RowCount,
		children[0].GetCost(),
		children[1].GetCost())
	return impl.cost
}

// GetCostLimit implements Implementation GetCostLimit interface.
func (impl *ApplyImpl) GetCostLimit(costLimit float64, children ...memo.Implementation) float64 {
	if len(children) == 0 {
		return costLimit
	}
	// The Cost of Apply is: selfCost + leftCost + leftCount * rightCost.
	// If we have implemented the leftChild, the costLimit for the right
	// side should be (costLimit - selfCost - leftCost)/leftCount. Since
	// we haven't implement the rightChild, we cannot calculate the `selfCost`.
	// So we just use (costLimit - leftCost)/leftCount here.
	leftCount, leftCost := children[0].GetPlan().Stats().RowCount, children[0].GetCost()
	apply := impl.plan.(*plannercore.PhysicalApply)
	if len(apply.LeftConditions) > 0 {
		leftCount *= plannercore.SelectionFactor
	}
	return (costLimit - leftCost) / leftCount
}

// NewApplyImpl creates a new ApplyImpl.
func NewApplyImpl(apply *plannercore.PhysicalApply) *ApplyImpl {
	return &ApplyImpl{baseImpl{plan: apply}}
}

// MaxOneRowImpl is the implementation of PhysicalApply.
type MaxOneRowImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *MaxOneRowImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	impl.cost = children[0].GetCost()
	return impl.cost
}

// NewMaxOneRowImpl creates a new MaxOneRowImpl.
func NewMaxOneRowImpl(maxOneRow *plannercore.PhysicalMaxOneRow) *MaxOneRowImpl {
	return &MaxOneRowImpl{baseImpl{plan: maxOneRow}}
}

// WindowImpl is the implementation of PhysicalWindow.
type WindowImpl struct {
	baseImpl
}

// NewWindowImpl creates a new WindowImpl.
func NewWindowImpl(window *plannercore.PhysicalWindow) *WindowImpl {
	return &WindowImpl{baseImpl{plan: window}}
}

// CalcCost implements Implementation CalcCost interface.
func (impl *WindowImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	impl.cost = children[0].GetCost()
	return impl.cost
}
