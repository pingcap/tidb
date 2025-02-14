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

package projection

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/cascades/memo"
	"github.com/pingcap/tidb/pkg/planner/cascades/pattern"
	"github.com/pingcap/tidb/pkg/planner/cascades/rule"
	corebase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/util/intset"
)

var _ rule.Rule = &XFEliminateOuterJoinBelowProjection{}

// XFEliminateOuterJoinBelowProjection pull the correlated expression from projection as child of join.
type XFEliminateOuterJoinBelowProjection struct {
	*rule.BaseRule
}

// NewXFEliminateOuterJoinBelowProjection creates a new EliminateProjection rule.
func NewXFEliminateOuterJoinBelowProjection() *XFEliminateOuterJoinBelowProjection {
	pa1 := pattern.NewPattern(pattern.OperandProjection, pattern.EngineTiDBOnly)
	pa1.SetChildren(pattern.NewPattern(pattern.OperandJoin, pattern.EngineTiDBOnly))
	return &XFEliminateOuterJoinBelowProjection{
		BaseRule: rule.NewBaseRule(rule.XFEliminateOuterJoinBelowProjection, pa1),
	}
}

// ID implement the Rule interface.
func (*XFEliminateOuterJoinBelowProjection) ID() uint {
	return uint(rule.XFEliminateOuterJoinBelowProjection)
}

// PreCheck implements the Rule interface.
func (*XFEliminateOuterJoinBelowProjection) PreCheck(projGE corebase.LogicalPlan) bool {
	joinOp := projGE.Children()[0].GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	return joinOp.JoinType == logicalop.LeftOuterJoin || joinOp.JoinType == logicalop.RightOuterJoin
}

// XForm implements the Rule interface.
func (xf *XFEliminateOuterJoinBelowProjection) XForm(projGE corebase.LogicalPlan) ([]corebase.LogicalPlan, bool, error) {
	projOp := projGE.GetWrappedLogicalPlan().(*logicalop.LogicalProjection)
	joinGE := projGE.Children()[0]
	joinOp := joinGE.GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	ok, innerChildIdx, outerGE, innerGE, outerUniqueIDs := xf.prepareForEliminateOuterJoin(joinGE)
	if !ok {
		return nil, false, nil
	}
	// only when proj only use the columns from outer table can eliminate outer join.
	if !ruleutil.IsColsAllFromOuterTable(projOp.GetUsedCols(), outerUniqueIDs) {
		return nil, false, nil
	}
	//
	innerJoinKeys := joinOp.ExtractJoinKeys(innerChildIdx)

}

func (*XFEliminateOuterJoinBelowProjection) prepareForEliminateOuterJoin(joinGE corebase.LogicalPlan) (ok bool, innerChildIdx int, outerGE, innerGE corebase.LogicalPlan, outerUniqueIDs intset.FastIntSet) {
	joinOp := joinGE.GetWrappedLogicalPlan().(*logicalop.LogicalJoin)
	switch joinOp.JoinType {
	case logicalop.LeftOuterJoin:
		innerChildIdx = 1
	case logicalop.RightOuterJoin:
		innerChildIdx = 0
	default:
		ok = false
		return
	}
	outerGE = joinOp.Children()[1^innerChildIdx]
	innerGE = joinOp.Children()[innerChildIdx]

	outerUniqueIDs = intset.NewFastIntSet()
	for _, outerCol := range outerGE.Schema().Columns {
		outerUniqueIDs.Insert(int(outerCol.UniqueID))
	}
	ok = true
	return
}

// check whether one of unique keys sets is contained by inner join keys.
func (*XFEliminateOuterJoinBelowProjection) isInnerJoinKeysContainUniqueKey(innerGroup *memo.GroupExpression, joinKeys *expression.Schema) (bool, error) {
	// builds UniqueKey info of innerGroup.
	innerGroup.BuildKeyInfo()
	for _, keyInfo := range innerGroup.Prop.Schema.PKOrUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	return false, nil
}
