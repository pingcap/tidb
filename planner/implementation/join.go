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

// HashJoinImpl is the implementation for PhysicalHashJoin.
type HashJoinImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *HashJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	hashJoin := impl.plan.(*plannercore.PhysicalHashJoin)
	// The children here are only used to calculate the cost.
	hashJoin.SetChildren(children[0].GetPlan(), children[1].GetPlan())
	selfCost := hashJoin.GetCost(children[0].GetPlan().StatsCount(), children[1].GetPlan().StatsCount())
	impl.cost = selfCost + children[0].GetCost() + children[1].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *HashJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	hashJoin := impl.plan.(*plannercore.PhysicalHashJoin)
	hashJoin.SetChildren(children[0].GetPlan(), children[1].GetPlan())
	return impl
}

// NewHashJoinImpl creates a new HashJoinImpl.
func NewHashJoinImpl(hashJoin *plannercore.PhysicalHashJoin) *HashJoinImpl {
	return &HashJoinImpl{baseImpl{plan: hashJoin}}
}

// MergeJoinImpl is the implementation for PhysicalMergeJoin.
type MergeJoinImpl struct {
	baseImpl
}

// CalcCost implements Implementation CalcCost interface.
func (impl *MergeJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	mergeJoin := impl.plan.(*plannercore.PhysicalMergeJoin)
	// The children here are only used to calculate the cost.
	mergeJoin.SetChildren(children[0].GetPlan(), children[1].GetPlan())
	selfCost := mergeJoin.GetCost(children[0].GetPlan().StatsCount(), children[1].GetPlan().StatsCount())
	impl.cost = selfCost + children[0].GetCost() + children[1].GetCost()
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *MergeJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	mergeJoin := impl.plan.(*plannercore.PhysicalMergeJoin)
	mergeJoin.SetChildren(children[0].GetPlan(), children[1].GetPlan())
	mergeJoin.SetSchema(plannercore.BuildPhysicalJoinSchema(mergeJoin.JoinType, mergeJoin))
	return impl
}

// NewMergeJoinImpl creates a new MergeJoinImpl.
func NewMergeJoinImpl(mergeJoin *plannercore.PhysicalMergeJoin) *MergeJoinImpl {
	return &MergeJoinImpl{baseImpl{plan: mergeJoin}}
}

// IndexLookUpJoinImpl is the implementation of IndexLookUpJoin(or IndexJoin).
type IndexLookUpJoinImpl struct {
	baseImpl

	InnerImpl memo.Implementation
	OuterIdx  int
}

// CalcCost implements Implementation CalcCost interface.
func (impl *IndexLookUpJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	outerChild := children[0]
	innerChild := impl.InnerImpl
	indexJoin := impl.plan.(*plannercore.PhysicalIndexJoin)
	impl.cost = indexJoin.GetCost(
		outerChild.GetPlan().Stats().RowCount, innerChild.GetPlan().Stats().RowCount,
		outerChild.GetCost(), innerChild.GetCost())
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *IndexLookUpJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	outerChild := children[0].GetPlan()
	innerChild := impl.InnerImpl.GetPlan()
	join := impl.plan.(*plannercore.PhysicalIndexJoin)
	if impl.OuterIdx == 0 {
		join.SetChildren(outerChild, innerChild)
	} else {
		join.SetChildren(innerChild, outerChild)
	}
	join.SetSchema(plannercore.BuildPhysicalJoinSchema(join.JoinType, join))
	return impl
}

// NewIndexLookUpJoinImpl creates a new IndexLookUpJoinImpl.
func NewIndexLookUpJoinImpl(join *plannercore.PhysicalIndexJoin, innerImpl memo.Implementation, outerIdx int) *IndexLookUpJoinImpl {
	return &IndexLookUpJoinImpl{baseImpl{plan: join}, innerImpl, outerIdx}
}

type IndexMergeJoinImpl struct {
	baseImpl

	InnerImpl memo.Implementation
	OuterIdx  int
}

// CalcCost implements Implementation CalcCost interface.
func (impl *IndexMergeJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	outerChild := children[0]
	innerChild := impl.InnerImpl
	indexJoin := impl.plan.(*plannercore.PhysicalIndexMergeJoin)
	impl.cost = indexJoin.GetCost(
		innerChild.GetPlan().Stats().RowCount, outerChild.GetPlan().Stats().RowCount,
		innerChild.GetCost(), outerChild.GetCost())
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *IndexMergeJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	outerChild := children[0].GetPlan()
	innerChild := impl.InnerImpl.GetPlan()
	join := impl.plan.(*plannercore.PhysicalIndexMergeJoin)
	if impl.OuterIdx == 0 {
		join.SetChildren(outerChild, innerChild)
	} else {
		join.SetChildren(innerChild, outerChild)
	}
	join.SetSchema(plannercore.BuildPhysicalJoinSchema(join.JoinType, join))
	return impl
}

// NewIndexLookUpJoinImpl creates a new IndexLookUpJoinImpl.
func NewIndexMergeJoinImpl(join *plannercore.PhysicalIndexMergeJoin, innerImpl memo.Implementation, outerIdx int) *IndexMergeJoinImpl {
	return &IndexMergeJoinImpl{baseImpl{plan: join}, innerImpl, outerIdx}
}

type IndexHashJoinImpl struct {
	baseImpl

	InnerImpl memo.Implementation
	OuterIdx  int
}

// CalcCost implements Implementation CalcCost interface.
func (impl *IndexHashJoinImpl) CalcCost(outCount float64, children ...memo.Implementation) float64 {
	outerChild := children[0]
	innerChild := impl.InnerImpl
	indexJoin := impl.plan.(*plannercore.PhysicalIndexHashJoin)
	impl.cost = indexJoin.GetCost(
		innerChild.GetPlan().Stats().RowCount, outerChild.GetPlan().Stats().RowCount,
		innerChild.GetCost(), outerChild.GetCost())
	return impl.cost
}

// AttachChildren implements Implementation AttachChildren interface.
func (impl *IndexHashJoinImpl) AttachChildren(children ...memo.Implementation) memo.Implementation {
	outerChild := children[0].GetPlan()
	innerChild := impl.InnerImpl.GetPlan()
	join := impl.plan.(*plannercore.PhysicalIndexHashJoin)
	if impl.OuterIdx == 0 {
		join.SetChildren(outerChild, innerChild)
	} else {
		join.SetChildren(innerChild, outerChild)
	}
	join.SetSchema(plannercore.BuildPhysicalJoinSchema(join.JoinType, join))
	return impl
}

// NewIndexHashJoinImpl creates a new IndexLookUpJoinImpl.
func NewIndexHashJoinImpl(join *plannercore.PhysicalIndexHashJoin, innerImpl memo.Implementation, outerIdx int) *IndexHashJoinImpl {
	return &IndexHashJoinImpl{baseImpl{plan: join}, innerImpl, outerIdx}
}
