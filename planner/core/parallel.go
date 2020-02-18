// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

type parallelLogicalPlanHelper struct {
	possibleChildrenProperties [][]*property.PhysicalProperty
}

func (p *parallelLogicalPlanHelper) preparePossiblePartitionProperties(
	lp LogicalPlan, globalGrouping []*expression.Column,
	childrenPartitionProperties ...[]*property.PhysicalProperty,
) []*property.PhysicalProperty {
	p.possibleChildrenProperties = make([][]*property.PhysicalProperty, len(lp.Children()))
	for i := range lp.Children() {
		p.possibleChildrenProperties[i] = p.preparePossiblePartitionProperties4OneChild(lp, globalGrouping, childrenPartitionProperties[i])
	}
	return p.possibleChildrenProperties[0] // Future: support multiple children.
}

func (p *parallelLogicalPlanHelper) preparePossiblePartitionProperties4OneChild(
	lp LogicalPlan, globalGrouping []*expression.Column,
	childPartitionProperties []*property.PhysicalProperty,
) []*property.PhysicalProperty {
	matched := make([]int, 0, len(childPartitionProperties))
	isEqual := false
	for i, childProp := range childPartitionProperties {
		if len(globalGrouping) == 0 {
			// no required grouping column, so any grouping column of child will do.
			matched = append(matched, i)
			if len(childProp.PartitionGroupingCols) == 0 {
				isEqual = true
			}
		} else {
			// any NONEMPTY SUBSET grouping columns of child will do.
			if len(childProp.PartitionGroupingCols) > 0 {
				if including, equal := isColumnsIncluding(globalGrouping, childProp.PartitionGroupingCols); including {
					matched = append(matched, i)
					if equal {
						isEqual = true
					}
				}
			}
		}
	}

	possibleProperties := make([]*property.PhysicalProperty, 0, len(matched)+1)
	for _, i := range matched {
		childProp := childPartitionProperties[i]
		prop := &property.PhysicalProperty{
			IsPartitioning:        true,
			PartitionGroupingCols: childProp.PartitionGroupingCols,
		}
		possibleProperties = append(possibleProperties, prop)
	}
	if !isEqual { // enforced parallel property for init-partitioning.
		prop := &property.PhysicalProperty{
			IsPartitioning:        true,
			PartitionGroupingCols: globalGrouping,
		}
		possibleProperties = append(possibleProperties, prop)
	}

	return possibleProperties
}

func (p *parallelLogicalPlanHelper) exhaustParallelPhysicalPlans4SingleChild(
	ctx sessionctx.Context, lp LogicalPlan, prop *property.PhysicalProperty, exhaustor func(*property.PhysicalProperty) []PhysicalPlan,
	deliveringLocalItems []property.Item, deliveringLocalItemExprs []property.ItemExpression,
) []PhysicalPlan {
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	if concurrency <= 1 {
		return nil
	}
	if len(p.possibleChildrenProperties) == 0 {
		return nil
	}

	plans := make([]PhysicalPlan, 0, len(p.possibleChildrenProperties[0]))
	for _, possibleProp := range p.possibleChildrenProperties[0] {
		var physicals []PhysicalPlan
		if exhaustor != nil {
			physicals = exhaustor(prop)
		} else {
			physicals = lp.exhaustPhysicalPlans(prop)
		}
		if physicals == nil {
			return nil
		}

		if len(possibleProp.PartitionGroupingCols) > 0 {
			NDV := int(getCardinality(possibleProp.PartitionGroupingCols, lp.Schema(), lp.statsInfo()))
			if NDV <= 1 {
				continue
			}
		} else {
			// Should not be parallel when less or equal to ONE chunk.
			numberOfChunks := (float64)(lp.statsInfo().RowCount) / (float64)(ctx.GetSessionVars().MaxChunkSize)
			if numberOfChunks <= 1.0 {
				continue
			}
		}

		for _, physical := range physicals {
			childProp := physical.GetChildReqProps(0)
			childProp.IsPartitioning = true
			childProp.PartitionGroupingCols = possibleProp.PartitionGroupingCols
			physical.SetConcurrency(concurrency)
			physical.SetPartitionDeliveringProperty(&property.PhysicalProperty{
				IsPartitioning:        true,
				Items:                 deliveringLocalItems,
				ItemExprs:             deliveringLocalItemExprs,
				PartitionGroupingCols: possibleProp.PartitionGroupingCols,
			})
			plans = append(plans, physical)
		}
	}
	return plans
}

// matchPhysicalProperty match parent required property and delivering property, and enforce Shuffle if necessary.
func matchPhysicalProperty(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, tsk task, ctx sessionctx.Context) task {
	if tsk.plan() == nil {
		return tsk
	}
	tsk = finishCopTask(ctx, tsk)
	deliveringProperty := pp.GetPartitionDeliveringProperty()

	if !requiredProperty.IsPartitioning {
		///// serial -> serial /////
		if !pp.IsParallel() {
			return tsk
		}
		///// serial -> parallel /////
		shuffle := enforceFullMerge(pp, requiredProperty, deliveringProperty, ctx)
		return shuffle.attach2Task(tsk)
	}

	///// parallel -> serial /////
	if !pp.IsParallel() {
		shuffle := enforceInitialPartition(pp, requiredProperty, ctx)
		return shuffle.attach2Task(tsk)
	}
	///// parallel -> parallel /////
	if matchGlobalPhysicalProperty(requiredProperty, deliveringProperty) {
		return tsk
	}
	shuffle := enforceRepartition(pp, requiredProperty, deliveringProperty, ctx)
	return shuffle.attach2Task(tsk)
}

func newPhysicalShuffle(child PhysicalPlan, requiredProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	reqProp := &property.PhysicalProperty{ExpectedCnt: requiredProperty.ExpectedCnt}
	shuffle := PhysicalShuffle{
		Concurrency: 1,
		FanOut:      1,
	}.Init(ctx, child.statsInfo(), child.SelectBlockOffset(), reqProp)
	return shuffle
}

func setShuffleNoneSplit(shuffle *PhysicalShuffle) {
	shuffle.FanOut = 1
	shuffle.SplitterType = ShuffleNoneSplitterType
}

func setShuffleSplitByHash(shuffle *PhysicalShuffle, concurrency int, groupingCols []*expression.Column) {
	shuffle.FanOut = concurrency
	shuffle.SplitterType = ShuffleHashSplitterType
	shuffle.SplitByItems = groupingCols
}

func setShuffleSplitByRandom(shuffle *PhysicalShuffle, concurrency int) *PhysicalShuffle {
	shuffle.FanOut = concurrency
	shuffle.SplitterType = ShuffleRandomSplitterType
	return shuffle
}

func setShuffleNoneMerge(shuffle *PhysicalShuffle) {
	shuffle.Concurrency = 1
	shuffle.MergerType = ShuffleNoneMergerType
}

func setShuffleMergeByMergeSort(shuffle *PhysicalShuffle, concurrency int, byItems []property.ItemExpression) {
	shuffle.Concurrency = concurrency
	shuffle.MergerType = ShuffleMergeSortMergerType
	shuffle.MergeByItems = byItems
}

func setShuffleMergeByRandom(shuffle *PhysicalShuffle, concurrency int) {
	shuffle.Concurrency = concurrency
	shuffle.MergerType = ShuffleRandomMergerType
}

func locateChildShuffle(pp PhysicalPlan) (tail PhysicalPlan, child *PhysicalShuffle) {
	var ok bool
	child, ok = pp.(*PhysicalShuffle)
	for !ok {
		if len(pp.Children()) > 0 {
			tail = pp
			pp = pp.Children()[0] //Future: support multi-children.
			child, ok = pp.(*PhysicalShuffle)
		} else {
			return nil, nil
		}
	}
	return tail, child
}

func enforceInitialPartition(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	shuffle := newPhysicalShuffle(pp, requiredProperty, ctx)
	if len(requiredProperty.PartitionGroupingCols) > 0 {
		setShuffleSplitByHash(shuffle, concurrency, requiredProperty.PartitionGroupingCols)
	} else {
		setShuffleSplitByRandom(shuffle, concurrency)
	}
	setShuffleNoneMerge(shuffle)
	return shuffle
}

func enforceFullMerge(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	_, isPhysicalSort := pp.(*PhysicalSort)
	shuffle := newPhysicalShuffle(pp, requiredProperty, ctx)
	setShuffleNoneSplit(shuffle)
	if len(requiredProperty.Items) > 0 || isPhysicalSort {
		// local property(i.e. requiredProperty.IsPrefix(deliveringProperty)) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.ItemExprs)
	} else {
		setShuffleMergeByRandom(shuffle, concurrency)
	}
	shuffle.Tail, shuffle.ChildShuffle = locateChildShuffle(pp)
	return shuffle
}

func matchGlobalPhysicalProperty(requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty) bool {
	if requiredProperty.IsNonePartitionGrouping() {
		return true
	}
	including, _ := isColumnsIncluding(requiredProperty.PartitionGroupingCols, deliveringProperty.PartitionGroupingCols)
	return including
}

func enforceRepartition(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	_, isPhysicalSort := pp.(*PhysicalSort)
	shuffle := newPhysicalShuffle(pp, requiredProperty, ctx)
	setShuffleSplitByHash(shuffle, concurrency, requiredProperty.PartitionGroupingCols)
	if len(requiredProperty.Items) > 0 || isPhysicalSort {
		// local property(i.e. requiredProperty.IsPrefix(deliveringProperty)) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.ItemExprs)
	} else {
		setShuffleMergeByRandom(shuffle, concurrency)
	}
	shuffle.Tail, shuffle.ChildShuffle = locateChildShuffle(pp)
	return shuffle
}
