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

func newPhysicalShuffle(child PhysicalPlan, ctx sessionctx.Context) *PhysicalShuffle {
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	return PhysicalShuffle{
		Concurrency: 1,
		FanOut:      1,
	}.Init(ctx, child.statsInfo(), child.SelectBlockOffset(), reqProp)
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

func setShuffleMergeByMergeSort(shuffle *PhysicalShuffle, concurrency int, byItems []property.Item) {
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
			pp = pp.Children()[0] // TODOO: multi-children
			child, ok = pp.(*PhysicalShuffle)
		} else {
			return nil, nil
		}
	}
	return tail, child
}

func enforceInitialPartition(pp PhysicalPlan, requiredProperty *property.PhysicalProperty, ctx sessionctx.Context) *PhysicalShuffle {
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	shuffle := newPhysicalShuffle(pp, ctx)
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
	shuffle := newPhysicalShuffle(pp, ctx)
	setShuffleNoneSplit(shuffle)
	if len(requiredProperty.Items) > 0 {
		// local property(i.e. requiredProperty.IsPrefix(deliveringProperty)) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.Items)
	} else {
		setShuffleMergeByRandom(shuffle, concurrency)
	}
	shuffle.Tail, shuffle.ChildShuffle = locateChildShuffle(pp)
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
	concurrency := ctx.GetSessionVars().ExecutorsConcurrency
	shuffle := newPhysicalShuffle(pp, ctx)
	setShuffleSplitByHash(shuffle, concurrency, requiredProperty.PartitionGroupingCols)
	if len(requiredProperty.Items) > 0 {
		// local property(i.e. requiredProperty.IsPrefix(deliveringProperty)) is ensured in `exhaustPhysicalPlans`.
		setShuffleMergeByMergeSort(shuffle, concurrency, deliveringProperty.Items)
	} else {
		setShuffleMergeByRandom(shuffle, concurrency)
	}
	shuffle.Tail, shuffle.ChildShuffle = locateChildShuffle(pp)
	return shuffle
}
