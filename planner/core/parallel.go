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
	"math"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

// matchPhysicalProperty match parent required property and delivering property, and enforce Shuffle if necessary.
func matchPhysicalProperty(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, tsk task, ctx sessionctx.Context) task {
	if tsk.plan() == nil {
		return tsk
	}
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

func newPhysicalShuffle(child PhysicalPlan, ctx sessionctx.Context) *PhysicalShuffle {
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	return PhysicalShuffle{}.Init(ctx, child.statsInfo(), child.SelectBlockOffset(), reqProp)
}

func setShuffleSplitByHash(shuffle *PhysicalShuffle, concurrency int, groupingCols []*expression.Column) {
	byItems := make([]expression.Expression, 0, len(groupingCols))
	for _, col := range groupingCols {
		byItems = append(byItems, col)
	}
	shuffle.FanOut = concurrency
	shuffle.SplitterType = ShuffleHashSplitterType
	shuffle.HashByItems = byItems
}

func setShuffleSplitByRandom(shuffle *PhysicalShuffle, concurrency int) *PhysicalShuffle {
	shuffle.FanOut = concurrency
	shuffle.SplitterType = ShuffleRandomSplitterType
	return shuffle
}

func setShuffleMergeByMergeSort(shuffle *PhysicalShuffle, concurrency int, byItems []property.Item) {
	shuffle.Concurrency = concurrency
	shuffle.MergerType = ShuffleMergeSortMergerType
	shuffle.MergeSortByItems = byItems
}

func setShuffleMergeByRandom(shuffle *PhysicalShuffle, concurrency int) {
	shuffle.Concurrency = concurrency
	shuffle.MergerType = ShuffleRandomMergerType
}

func enforceInitialPartition(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ParallelExecutorConcurrency
	shuffle := newPhysicalShuffle(pp, ctx)
	if len(requiredProperty.PartitionGroupingCols) > 0 {
		setShuffleSplitByHash(shuffle, concurrency, requiredProperty.PartitionGroupingCols)
		return shuffle
	}
	setShuffleSplitByRandom(shuffle, concurrency)
	return shuffle
}

func enforceFullMerge(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ParallelExecutorConcurrency
	shuffle := newPhysicalShuffle(pp, ctx)
	if len(requiredProperty.Items) > 0 {
		// requiredProperty.IsPrefix(deliveringProperty) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.Items)
		return shuffle
	}
	setShuffleMergeByRandom(shuffle, concurrency)
	return shuffle
}

func matchGlobalPhysicalProperty(requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty) bool {
	if len(requiredProperty.PartitionGroupingCols) > 0 {
		including, _ := isColumnsIncluding(requiredProperty.PartitionGroupingCols, deliveringProperty.PartitionGroupingCols)
		return including
	}
	return true
}

func enforceRepartition(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, deliveringProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ParallelExecutorConcurrency
	shuffle := newPhysicalShuffle(pp, ctx)
	setShuffleSplitByHash(shuffle, concurrency, requiredProperty.PartitionGroupingCols)
	if len(requiredProperty.Items) > 0 {
		// requiredProperty.IsPrefix(deliveringProperty) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.Items)
		return shuffle
	}
	setShuffleMergeByRandom(shuffle, concurrency)
	return shuffle
}
