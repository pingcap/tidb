// Copyright 2024 PingCAP, Inc.
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

package logicaltrace

import (
	"bytes"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
)

func appendItemPruneTraceStep(p base.LogicalPlan, itemType string, prunedObjects []expression.StringerWithCtx,
	opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedObjects) < 1 {
		return
	}
	action := func() string {
		buffer := bytes.NewBufferString(fmt.Sprintf("%v_%v's %v[", p.TP(), p.ID(), itemType))
		for i, item := range prunedObjects {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(item.StringWithCtx(p.SCtx().GetExprCtx().GetEvalCtx(), errors.RedactLogDisable))
		}
		buffer.WriteString("] have been pruned")
		return buffer.String()
	}
	reason := func() string {
		return ""
	}
	opt.AppendStepToCurrent(p.ID(), p.TP(), reason, action)
}

// AppendColumnPruneTraceStep appends a trace step for column pruning.
func AppendColumnPruneTraceStep(p base.LogicalPlan, prunedColumns []*expression.Column,
	opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedColumns) < 1 {
		return
	}
	s := make([]expression.StringerWithCtx, 0, len(prunedColumns))
	for _, item := range prunedColumns {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "columns", s, opt)
}

// AppendFunctionPruneTraceStep appends a trace step for group by pruning.
func AppendFunctionPruneTraceStep(p base.LogicalPlan, prunedFunctions []*aggregation.AggFuncDesc,
	opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedFunctions) < 1 {
		return
	}
	s := make([]expression.StringerWithCtx, 0, len(prunedFunctions))
	for _, item := range prunedFunctions {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "aggregation functions", s, opt)
}

// AppendByItemsPruneTraceStep appends a trace step for group by pruning.
func AppendByItemsPruneTraceStep(p base.LogicalPlan, prunedByItems []*util.ByItems,
	opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedByItems) < 1 {
		return
	}
	s := make([]expression.StringerWithCtx, 0, len(prunedByItems))
	for _, item := range prunedByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "byItems", s, opt)
}

// AppendGroupByItemsPruneTraceStep appends a trace step for group by pruning.
func AppendGroupByItemsPruneTraceStep(p base.LogicalPlan, prunedGroupByItems []expression.Expression,
	opt *optimizetrace.LogicalOptimizeOp) {
	if len(prunedGroupByItems) < 1 {
		return
	}
	s := make([]expression.StringerWithCtx, 0, len(prunedGroupByItems))
	for _, item := range prunedGroupByItems {
		s = append(s, item)
	}
	appendItemPruneTraceStep(p, "groupByItems", s, opt)
}

// ApplyEliminateTraceStep appends a trace step for aggregation pruning.
func ApplyEliminateTraceStep(lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) {
	action := func() string {
		buffer := bytes.NewBufferString(
			fmt.Sprintf("%v_%v is eliminated.", lp.TP(), lp.ID()))
		return buffer.String()
	}
	reason := func() string {
		return fmt.Sprintf("%v_%v can be eliminated because it hasn't been used by it's parent.", lp.TP(), lp.ID())
	}
	opt.AppendStepToCurrent(lp.ID(), lp.TP(), reason, action)
}
