// Copyright 2025 PingCAP, Inc.
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

package eoj

import (
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
)

var _ rule.Rule = &XFEliminateOuterJoinBelowAgg{}

// XFEliminateOuterJoinBelowAgg eliminate an outer join below an Aggregation.
type XFEliminateOuterJoinBelowAgg struct {
	outerJoinEliminator
	*rule.BaseRule
}

// NewXFEliminateOuterJoinBelowAgg creates a new XFEliminateOuterJoinBelowAgg rule.
func NewXFEliminateOuterJoinBelowAgg() *XFEliminateOuterJoinBelowAgg {
	pa1 := pattern.NewPattern(pattern.OperandAggregation, pattern.EngineTiDBOnly)
	pa2 := pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly)
	pa2.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	pa1.SetChildren(pa2)
	return &XFEliminateOuterJoinBelowAgg{
		BaseRule: rule.NewBaseRule(rule.XFEliminateOuterJoinBelowAgg, pa1),
	}
}

// ID implement the Rule interface.
func (*XFEliminateOuterJoinBelowAgg) ID() uint {
	return uint(rule.XFEliminateOuterJoinBelowAgg)
}

// PreCheck implements the Rule interface.
func (*XFEliminateOuterJoinBelowAgg) PreCheck(projGE corebase.LogicalPlan) bool {
	joinOp := projGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	return joinOp.JoinType == logicalop.LeftOuterJoin || joinOp.JoinType == logicalop.RightOuterJoin
}

// XForm implements the Rule interface.
func (xf *XFEliminateOuterJoinBelowAgg) XForm(aggGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	aggOp := aggGE.GetWrappedLogicalPlan().(*logicalop.LogicalAggregation)
	joinGE := aggGE.Children()[0]
	joinOp := joinGE.GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	ok, innerChildIdx, outerGE, innerGE, outerUniqueIDs := xf.prepareForEliminateOuterJoin(joinGE)
	if !ok {
		return nil, false, nil
	}
	// only when proj only use the columns from outer table can eliminate outer join.
	if !ruleutil.IsColsAllFromOuterTable(aggOp.GetUsedCols(), &outerUniqueIDs) {
		return nil, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions.
	_, aggCols := logicalop.GetDupAgnosticAggCols(aggOp, nil)
	if len(aggCols) > 0 {
		clonedAggOp := aggOp.LogicalAggregationShallowRef()
		clonedAggOp.ReAlloc4Cascades(aggOp.TP(), clonedAggOp)
		clonedAggOp.SetChildren(outerGE)
		return []corebase.LogicalPlan{clonedAggOp}, true, nil
	}
	// check whether join inner side covers the unique key.
	innerJoinKeys := joinOp.ExtractJoinKeys(innerChildIdx)
	if xf.isInnerJoinKeysContainUniqueKey(innerGE, innerJoinKeys) {
		clonedAggOp := aggOp.LogicalAggregationShallowRef()
		clonedAggOp.ReAlloc4Cascades(aggOp.TP(), clonedAggOp)
		clonedAggOp.SetChildren(outerGE)
		// since eliminate outer join is always good, eliminate the old one.
		return []corebase.LogicalPlan{clonedAggOp}, true, nil
	}
	return nil, false, nil
}
