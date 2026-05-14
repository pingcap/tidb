// Copyright 2026 PingCAP, Inc.
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

package windows

import (
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/vecgroupchecker"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// Build constructs the concrete executor for a window physical plan.
func Build(sctx sessionctx.Context, v *physicalop.PhysicalWindow, childExec exec.Executor, forcePipelined bool) (exec.Executor, error) {
	base := exec.NewBaseExecutor(sctx, v.Schema(), v.ID(), childExec)
	groupByItems := make([]expression.Expression, 0, len(v.PartitionBy))
	for _, item := range v.PartitionBy {
		groupByItems = append(groupByItems, item.Col)
	}
	orderByCols := make([]*expression.Column, 0, len(v.OrderBy))
	for _, item := range v.OrderBy {
		orderByCols = append(orderByCols, item.Col)
	}
	windowFuncs := make([]aggfuncs.AggFunc, 0, len(v.WindowFuncDescs))
	partialResults := make([]aggfuncs.PartialResult, 0, len(v.WindowFuncDescs))
	resultColIdx := v.Schema().Len() - len(v.WindowFuncDescs)
	exprCtx := sctx.GetExprCtx()
	for _, desc := range v.WindowFuncDescs {
		aggDesc, err := aggregation.NewAggFuncDescForWindowFunc(exprCtx, desc, false)
		if err != nil {
			return nil, err
		}
		agg := aggfuncs.BuildWindowFunctions(exprCtx, aggDesc, resultColIdx, orderByCols)
		windowFuncs = append(windowFuncs, agg)
		partialResult, _ := agg.AllocPartialResult()
		partialResults = append(partialResults, partialResult)
		resultColIdx++
	}

	if forcePipelined || sctx.GetSessionVars().EnablePipelinedWindowExec {
		exec := &PipelinedWindowExec{
			BaseExecutor:   base,
			groupChecker:   vecgroupchecker.NewVecGroupChecker(sctx.GetExprCtx().GetEvalCtx(), sctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
			numWindowFuncs: len(v.WindowFuncDescs),
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
		exec.slidingWindowFuncs = make([]aggfuncs.SlidingWindowAggFunc, len(exec.windowFuncs))
		for i, windowFunc := range exec.windowFuncs {
			if slidingWindowAggFunc, ok := windowFunc.(aggfuncs.SlidingWindowAggFunc); ok {
				exec.slidingWindowFuncs[i] = slidingWindowAggFunc
			}
		}
		if v.Frame == nil {
			exec.start = &logicalop.FrameBound{Type: ast.Preceding, UnBounded: true}
			exec.end = &logicalop.FrameBound{Type: ast.Following, UnBounded: true}
		} else {
			exec.start = v.Frame.Start
			exec.end = v.Frame.End
			if v.Frame.Type == ast.Ranges {
				cmpResult := int64(-1)
				if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
					cmpResult = 1
				}
				exec.orderByCols = orderByCols
				exec.expectedCmpResult = cmpResult
				exec.isRangeFrame = true
				if err := exec.start.UpdateCompareCols(sctx, exec.orderByCols); err != nil {
					return nil, err
				}
				if err := exec.end.UpdateCompareCols(sctx, exec.orderByCols); err != nil {
					return nil, err
				}
			}
		}
		return exec, nil
	}

	var processor windowProcessor
	if v.Frame == nil {
		processor = &aggWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
		}
	} else if v.Frame.Type == ast.Rows {
		processor = &rowFrameWindowProcessor{
			windowFuncs:    windowFuncs,
			partialResults: partialResults,
			start:          v.Frame.Start,
			end:            v.Frame.End,
		}
	} else {
		cmpResult := int64(-1)
		if len(v.OrderBy) > 0 && v.OrderBy[0].Desc {
			cmpResult = 1
		}
		tmpProcessor := &rangeFrameWindowProcessor{
			windowFuncs:       windowFuncs,
			partialResults:    partialResults,
			start:             v.Frame.Start,
			end:               v.Frame.End,
			orderByCols:       orderByCols,
			expectedCmpResult: cmpResult,
		}
		if err := tmpProcessor.start.UpdateCompareCols(sctx, orderByCols); err != nil {
			return nil, err
		}
		if err := tmpProcessor.end.UpdateCompareCols(sctx, orderByCols); err != nil {
			return nil, err
		}
		processor = tmpProcessor
	}
	return &WindowExec{
		BaseExecutor:   base,
		processor:      processor,
		groupChecker:   vecgroupchecker.NewVecGroupChecker(sctx.GetExprCtx().GetEvalCtx(), sctx.GetSessionVars().EnableVectorizedExpression, groupByItems),
		numWindowFuncs: len(v.WindowFuncDescs),
	}, nil
}
