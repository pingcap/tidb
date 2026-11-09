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

package joinorder

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/hint"
)

// JoinMethodHint records the join method hint for a vertex.
type JoinMethodHint struct {
	PreferJoinMethod uint
	HintInfo         *hint.PlanHints
}

// CheckAndGenerateLeadingHint used to check and generate the valid leading hint.
// We are allowed to use at most one leading hint in a join group. When more than one,
// all leading hints in the current join group will be invalid.
// For example: select /*+ leading(t3) */ * from (select /*+ leading(t1) */ t2.b from t1 join t2 on t1.a=t2.a) t4 join t3 on t4.b=t3.b
// The Join Group {t1, t2, t3} contains two leading hints includes leading(t3) and leading(t1).
// Although they are in different query blocks, they are conflicting.
// In addition, the table alias 't4' cannot be recognized because of the join group.
func CheckAndGenerateLeadingHint(hintInfo []*hint.PlanHints) (*hint.PlanHints, bool) {
	leadingHintNum := len(hintInfo)
	var leadingHintInfo *hint.PlanHints
	hasDiffLeadingHint := false
	if leadingHintNum > 0 {
		leadingHintInfo = hintInfo[0]
		// One join group has one leading hint at most. Check whether there are different join order hints.
		for i := 1; i < leadingHintNum; i++ {
			if hintInfo[i] != hintInfo[i-1] {
				hasDiffLeadingHint = true
				break
			}
		}
		if hasDiffLeadingHint {
			leadingHintInfo = nil
		}
	}
	return leadingHintInfo, hasDiffLeadingHint
}

// MarkLeadingHintMatched records source-qualified operands only after the
// complete LEADING tree has been accepted by join reorder.
func MarkLeadingHintMatched(hintInfo *hint.PlanHints) {
	if hintInfo == nil {
		return
	}
	for i := range hintInfo.LeadingJoinOrder {
		hintInfo.LeadingJoinOrder[i].Matched = true
	}
}

// LeadingTreeFinder finds a node by hint and removes it from the available slice.
type LeadingTreeFinder[T any] func(available []T, hint *ast.HintTable) (T, []T, bool)

// LeadingTreeJoiner joins two nodes in the leading tree.
type LeadingTreeJoiner[T any] func(left, right T) (T, bool, error)

// BuildLeadingTreeFromList recursively constructs a LEADING join order tree.
// the `leadingList` argument is derived from a LEADING hint in SQL, e.g.:
//
//	/*+ LEADING(t1, (t2, t3), (t4, (t5, t6, t7))) */
//
// and it is parsed into a nested structure of *ast.LeadingList and *ast.HintTable:
// leadingList.Items = [
//
//	*ast.HintTable{name: "t1"},
//	*ast.LeadingList{ // corresponds to (t2, t3)
//	    Items: [
//	        *ast.HintTable{name: "t2"},
//	        *ast.HintTable{name: "t3"},
//	    ],
//	},
//	*ast.LeadingList{ // corresponds to (t4, (t5, t6, t7))
//	    Items: [
//	        *ast.HintTable{name: "t4"},
//	        *ast.LeadingList{
//	            Items: [
//	                *ast.HintTable{name: "t5"},
//	                *ast.HintTable{name: "t6"},
//	                *ast.HintTable{name: "t7"},
//	            ],
//	        },
//	    ],
//	},
//
// ]
func BuildLeadingTreeFromList[T any](
	leadingList *ast.LeadingList,
	availableGroups []T,
	findAndRemoveByHint LeadingTreeFinder[T],
	checkAndJoin LeadingTreeJoiner[T],
	warn func(),
) (T, []T, bool, error) {
	var zero T
	if leadingList == nil || len(leadingList.Items) == 0 {
		return zero, availableGroups, false, nil
	}

	var (
		currentJoin     T
		err             error
		ok              bool
		remainingGroups = availableGroups
	)

	for i, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			var tableNode T
			tableNode, remainingGroups, ok = findAndRemoveByHint(remainingGroups, element)
			if !ok {
				return zero, availableGroups, false, nil
			}

			if i == 0 {
				currentJoin = tableNode
			} else {
				currentJoin, ok, err = checkAndJoin(currentJoin, tableNode)
				if err != nil {
					return zero, availableGroups, false, err
				}
				if !ok {
					return zero, availableGroups, false, nil
				}
			}
		case *ast.LeadingList:
			var nestedJoin T
			nestedJoin, remainingGroups, ok, err = BuildLeadingTreeFromList(element, remainingGroups, findAndRemoveByHint, checkAndJoin, warn)
			if err != nil {
				return zero, availableGroups, false, err
			}
			if !ok {
				return zero, availableGroups, false, nil
			}

			if i == 0 {
				currentJoin = nestedJoin
			} else {
				currentJoin, ok, err = checkAndJoin(currentJoin, nestedJoin)
				if err != nil {
					return zero, availableGroups, false, err
				}
				if !ok {
					return zero, availableGroups, false, nil
				}
			}
		default:
			if warn != nil {
				warn()
			}
			return zero, availableGroups, false, nil
		}
	}

	return currentJoin, remainingGroups, true, nil
}

// FindAndRemovePlanByAstHint finds the unique plan matching astTbl and removes
// it from plans. Every candidate uses the same LEADING identity policy.
func FindAndRemovePlanByAstHint[T any](
	_ base.PlanContext,
	plans []T,
	astTbl *ast.HintTable,
	targetOwnerOffset int,
	getPlan func(T) base.LogicalPlan,
) (T, []T, bool) {
	var zero T
	if targetOwnerOffset < 0 {
		return zero, plans, false
	}
	matchIdx := -1
	for i, joinGroup := range plans {
		plan := getPlan(joinGroup)
		if !matchLeadingHintToPlan(plan, astTbl, targetOwnerOffset).Matched() {
			continue
		}
		if matchIdx != -1 {
			return zero, plans, false
		}
		matchIdx = i
	}
	if matchIdx != -1 {
		matched := plans[matchIdx]
		newPlans := append(plans[:matchIdx], plans[matchIdx+1:]...)
		return matched, newPlans, true
	}

	return zero, plans, false
}

// IsDerivedTableInLeadingHint checks if a plan node represents a derived table (subquery)
// that is explicitly referenced in the LEADING hint.
func IsDerivedTableInLeadingHint(p base.LogicalPlan, leadingHint *hint.PlanHints, targetOwnerOffset int) bool {
	if leadingHint == nil || leadingHint.LeadingList == nil || targetOwnerOffset < 0 {
		return false
	}
	return containsTableInLeadingList(leadingHint.LeadingList, p, targetOwnerOffset)
}

// containsTableInLeadingList recursively searches for a table name in the LEADING hint structure
func containsTableInLeadingList(leadingList *ast.LeadingList, plan base.LogicalPlan, targetOwnerOffset int) bool {
	if leadingList == nil {
		return false
	}

	for _, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			if matchLeadingHintToPlan(plan, element, targetOwnerOffset).PreserveBoundary() {
				return true
			}
		case *ast.LeadingList:
			if containsTableInLeadingList(element, plan, targetOwnerOffset) {
				return true
			}
		}
	}

	return false
}

// SetNewJoinWithHint sets the join method hint for the join node.
func SetNewJoinWithHint(newJoin *logicalop.LogicalJoin, vertexHints map[int]*JoinMethodHint) {
	if newJoin == nil {
		return
	}
	lChild := newJoin.Children()[0]
	rChild := newJoin.Children()[1]
	if joinMethodHint, ok := vertexHints[lChild.ID()]; ok {
		newJoin.LeftPreferJoinType = joinMethodHint.PreferJoinMethod
		newJoin.HintInfo = joinMethodHint.HintInfo
	}
	if joinMethodHint, ok := vertexHints[rChild.ID()]; ok {
		newJoin.RightPreferJoinType = joinMethodHint.PreferJoinMethod
		newJoin.HintInfo = joinMethodHint.HintInfo
	}
	newJoin.SetPreferredJoinType()
}
