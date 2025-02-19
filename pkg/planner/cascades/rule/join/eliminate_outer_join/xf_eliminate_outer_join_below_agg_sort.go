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

var _ rule.Rule = &XFEliminateOuterJoinBelowAggSort{}

// XFEliminateOuterJoinBelowAggSort eliminate an outer join below an Aggregation.
// todo: this rule couldn't be removed once sort prop is integrated into base logical plan.
type XFEliminateOuterJoinBelowAggSort struct {
	outerJoinEliminator
	*rule.BaseRule
}

// NewXFEliminateOuterJoinBelowAggSort creates a new XFEliminateOuterJoinBelowAgg rule.
func NewXFEliminateOuterJoinBelowAggSort() *XFEliminateOuterJoinBelowAggSort {
	pa1 := pattern.NewPattern(pattern.OperandAggregation, pattern.EngineTiDBOnly)
	pa2 := pattern.NewPattern(pattern.OperandSort, pattern.EngineTiDBOnly)
	pa3 := pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly)
	pa3.SetChildren(pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly), pattern.NewPattern(pattern.OperandAny, pattern.EngineTiDBOnly))
	pa2.SetChildren(pa3)
	pa1.SetChildren(pa2)
	return &XFEliminateOuterJoinBelowAggSort{
		BaseRule: rule.NewBaseRule(rule.XFEliminateOuterJoinBelowAggSort, pa1),
	}
}

// ID implement the Rule interface.
func (*XFEliminateOuterJoinBelowAggSort) ID() uint {
	return uint(rule.XFEliminateOuterJoinBelowAggSort)
}

// PreCheck implements the Rule interface.
func (*XFEliminateOuterJoinBelowAggSort) PreCheck(aggGE corebase.LogicalPlan) bool {
	joinOp := aggGE.Children()[0].Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	return joinOp.JoinType == logicalop.LeftOuterJoin || joinOp.JoinType == logicalop.RightOuterJoin
}

// XForm implements the Rule interface.
func (xf *XFEliminateOuterJoinBelowAggSort) XForm(aggGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	aggOp := aggGE.GetWrappedLogicalPlan().(*logicalop.LogicalAggregation)
	sortGE := aggGE.Children()[0]
	sortOp := sortGE.GetWrappedLogicalPlan().(*logicalop.LogicalSort)
	joinGE := sortGE.Children()[0]
	joinOp := joinGE.GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	ok, innerChildIdx, outerGE, innerGE, outerUniqueIDs := xf.prepareForEliminateOuterJoin(joinGE)
	if !ok {
		return nil, false, nil
	}
	// only when proj only use the columns from outer table can eliminate outer join.
	if !ruleutil.IsColsAllFromOuterTable(aggOp.GetUsedCols(), &outerUniqueIDs) {
		return nil, false, nil
	}
	if !ruleutil.IsColsAllFromOuterTable(sortOp.GetUsedCols(), &outerUniqueIDs) {
		return nil, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions.
	_, aggCols := logicalop.GetDupAgnosticAggCols(aggOp, nil)
	if len(aggCols) > 0 {
		clonedAggOp := aggOp.LogicalAggregationShallowRef()
		clonedAggOp.ReAlloc4Cascades(aggOp.TP(), clonedAggOp)
		clonedSortOp := sortOp.LogicalSortShallowRef()
		clonedSortOp.ReAlloc4Cascades(sortOp.TP(), clonedSortOp)
		clonedAggOp.SetChildren(clonedSortOp)
		clonedSortOp.SetChildren(outerGE)
		return []corebase.LogicalPlan{clonedAggOp}, true, nil
	}
	// check whether join inner side covers the unique key.
	innerJoinKeys := joinOp.ExtractJoinKeys(innerChildIdx)
	if xf.isInnerJoinKeysContainUniqueKey(innerGE, innerJoinKeys) {
		clonedAggOp := aggOp.LogicalAggregationShallowRef()
		clonedAggOp.ReAlloc4Cascades(aggOp.TP(), clonedAggOp)
		clonedSortOp := sortOp.LogicalSortShallowRef()
		clonedSortOp.ReAlloc4Cascades(sortOp.TP(), clonedSortOp)
		clonedAggOp.SetChildren(clonedSortOp)
		clonedSortOp.SetChildren(outerGE)
		// since eliminate outer join is always good, eliminate the old one.
		return []corebase.LogicalPlan{clonedAggOp}, true, nil
	}
	return nil, false, nil
}
