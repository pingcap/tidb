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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// AsSctx gets sessionctx.Context from PlanContext.
//
// Use this helper when the caller requires a session-backed PlanContext.
// If conversion fails, it returns an error (and triggers intest.Assert in test builds).
func AsSctx(pctx base.PlanContext) (sessionctx.Context, error) {
	if sctx, ok := pctx.(sessionctx.Context); ok {
		return sctx, nil
	}

	// Some PlanContext implementations are wrappers (e.g. planctx.WithExprCtx). Unwrap them to recover
	// the underlying session-backed context when needed.
	u, ok := pctx.(planctx.InternalSctxUnwrapper)
	if !ok {
		intest.Assert(false, "PlanContext %T is not session-backed and does not implement planctx.InternalSctxUnwrapper", pctx)
		return nil, errors.Errorf("the current PlanContext (%T) cannot be converted to sessionctx.Context: missing planctx.InternalSctxUnwrapper", pctx)
	}

	unwrapped := u.UnwrapAsInternalSctx()
	if sctx, ok := unwrapped.(sessionctx.Context); ok {
		return sctx, nil
	}

	intest.Assert(false, "planctx.InternalSctxUnwrapper returned non-session value %T from %T", unwrapped, pctx)
	return nil, errors.Errorf("the current PlanContext (%T) cannot be converted to sessionctx.Context: unwrapped value type is %T", pctx, unwrapped)
}

// optimizeByShuffle insert `PhysicalShuffle` to optimize performance by running in a parallel manner.
func optimizeByShuffle(tsk base.Task, ctx base.PlanContext) base.Task {
	if tsk.Plan() == nil {
		return tsk
	}

	switch p := tsk.Plan().(type) {
	case *physicalop.PhysicalWindow:
		if shuffle := optimizeByShuffle4Window(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	case *physicalop.PhysicalMergeJoin:
		if shuffle := optimizeByShuffle4MergeJoin(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	case *physicalop.PhysicalStreamAgg:
		if shuffle := optimizeByShuffle4StreamAgg(p, ctx); shuffle != nil {
			return shuffle.Attach2Task(tsk)
		}
	}
	return tsk
}

func optimizeByShuffle4Window(pp *physicalop.PhysicalWindow, ctx base.PlanContext) *physicalop.PhysicalShuffle {
	concurrency := ctx.GetSessionVars().WindowConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*physicalop.PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		partitionBy = append(partitionBy, item.Col)
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(ctx, partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

	byItems := make([]expression.Expression, 0, len(pp.PartitionBy))
	for _, item := range pp.PartitionBy {
		byItems = append(byItems, item.Col)
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := physicalop.PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []base.PhysicalPlan{tail},
		DataSources:  []base.PhysicalPlan{dataSource},
		SplitterType: physicalop.PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{byItems},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4StreamAgg(pp *physicalop.PhysicalStreamAgg, ctx base.PlanContext) *physicalop.PhysicalShuffle {
	concurrency := ctx.GetSessionVars().StreamAggConcurrency()
	if concurrency <= 1 {
		return nil
	}

	sort, ok := pp.Children()[0].(*physicalop.PhysicalSort)
	if !ok {
		// Multi-thread executing on SORTED data source is not effective enough by current implementation.
		// TODO: Implement a better one.
		return nil
	}
	tail, dataSource := sort, sort.Children()[0]

	partitionBy := make([]*expression.Column, 0, len(pp.GroupByItems))
	for _, item := range pp.GroupByItems {
		if col, ok := item.(*expression.Column); ok {
			partitionBy = append(partitionBy, col)
		}
	}
	ndv, _ := cardinality.EstimateColsNDVWithMatchedLen(ctx, partitionBy, dataSource.Schema(), dataSource.StatsInfo())
	if ndv <= 1 {
		return nil
	}
	concurrency = min(concurrency, int(ndv))

	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := physicalop.PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        []base.PhysicalPlan{tail},
		DataSources:  []base.PhysicalPlan{dataSource},
		SplitterType: physicalop.PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{util.CloneExprs(pp.GroupByItems)},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func optimizeByShuffle4MergeJoin(pp *physicalop.PhysicalMergeJoin, ctx base.PlanContext) *physicalop.PhysicalShuffle {
	concurrency := ctx.GetSessionVars().MergeJoinConcurrency()
	if concurrency <= 1 {
		return nil
	}

	children := pp.Children()
	dataSources := make([]base.PhysicalPlan, len(children))
	tails := make([]base.PhysicalPlan, len(children))

	for i := range children {
		sort, ok := children[i].(*physicalop.PhysicalSort)
		if !ok {
			// Multi-thread executing on SORTED data source is not effective enough by current implementation.
			// TODO: Implement a better one.
			return nil
		}
		tails[i], dataSources[i] = sort, sort.Children()[0]
	}

	leftByItemArray := make([]expression.Expression, 0, len(pp.LeftJoinKeys))
	for _, col := range pp.LeftJoinKeys {
		leftByItemArray = append(leftByItemArray, col.Clone())
	}
	rightByItemArray := make([]expression.Expression, 0, len(pp.RightJoinKeys))
	for _, col := range pp.RightJoinKeys {
		rightByItemArray = append(rightByItemArray, col.Clone())
	}
	reqProp := &property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}
	shuffle := physicalop.PhysicalShuffle{
		Concurrency:  concurrency,
		Tails:        tails,
		DataSources:  dataSources,
		SplitterType: physicalop.PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{leftByItemArray, rightByItemArray},
	}.Init(ctx, pp.StatsInfo(), pp.QueryBlockOffset(), reqProp)
	return shuffle
}

func getEstimatedProbeCntFromProbeParents(probeParents []base.PhysicalPlan) float64 {
	res := float64(1)
	for _, pp := range probeParents {
		switch pp.(type) {
		case *physicalop.PhysicalApply, *physicalop.PhysicalIndexHashJoin,
			*physicalop.PhysicalIndexMergeJoin, *physicalop.PhysicalIndexJoin:
			if join, ok := pp.(interface{ GetInnerChildIdx() int }); ok {
				outer := pp.Children()[1-join.GetInnerChildIdx()]
				res *= outer.StatsInfo().RowCount
			}
		}
	}
	return res
}

func getActualProbeCntFromProbeParents(pps []base.PhysicalPlan, statsColl *execdetails.RuntimeStatsColl) int64 {
	res := int64(1)
	for _, pp := range pps {
		switch pp.(type) {
		case *physicalop.PhysicalApply, *physicalop.PhysicalIndexHashJoin,
			*physicalop.PhysicalIndexMergeJoin, *physicalop.PhysicalIndexJoin:
			if join, ok := pp.(interface{ GetInnerChildIdx() int }); ok {
				outerChildID := pp.Children()[1-join.GetInnerChildIdx()].ID()
				actRows := int64(1)
				if statsColl.ExistsRootStats(outerChildID) {
					actRows = statsColl.GetRootStats(outerChildID).GetActRows()
				}
				if statsColl.ExistsCopStats(outerChildID) {
					actRows = statsColl.GetCopStats(outerChildID).GetActRows()
				}
				// TODO: For PhysicalApply, we need to consider cache hit ratio in JoinRuntimeStats and use actRows/(1-ratio) here.
				res *= actRows
			}
		}
	}
	return res
}
