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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// JoinMethodHint records the join method hint for a vertex.
// JoinMethodHint stores join method hint info associated with a vertex.
type JoinMethodHint struct {
	PreferJoinMethod uint
	HintInfo         *hint.PlanHints
}

// checkAndGenerateLeadingHint used to check and generate the valid leading hint.
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

// LeadingTreeFinder finds a node by hint and removes it from the available slice.
type LeadingTreeFinder[T any] func(available []T, hint *ast.HintTable) (T, []T, bool)

// LeadingTreeJoiner joins two nodes in the leading tree.
type LeadingTreeJoiner[T any] func(left, right T) (T, bool, error)

// BuildLeadingTreeFromList constructs a join tree according to a LeadingList.
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
		currentJoin T
		err         error
		ok          bool
	)
	// copy here because findAndRemoveByHint will modify the slice.
	remainingGroups := make([]T, len(availableGroups))
	copy(remainingGroups, availableGroups)

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

// FindAndRemovePlanByAstHint matches a hint table to a plan and removes it from the slice.
// T is usually be *Node or base.LogicalPlan, we use generics because we want to reuse this function in both the old and new join order code.
func FindAndRemovePlanByAstHint[T any](
	ctx base.PlanContext,
	plans []T,
	astTbl *ast.HintTable,
	getPlan func(T) base.LogicalPlan,
) (T, []T, bool) {
	var zero T
	var queryBlockNames []ast.HintTable
	if p := ctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}

	// Step 1: Direct match by table name
	for i, joinGroup := range plans {
		plan := getPlan(joinGroup)
		tableAlias := util.ExtractTableAlias(plan, plan.QueryBlockOffset())
		if tableAlias != nil {
			// Match db/table (supports astTbl.DBName == "*")
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == tableAlias.DBName.L || astTbl.DBName.L == "*"
			tableMatch := astTbl.TableName.L == tableAlias.TblName.L

			// Match query block names
			// Use SelectOffset to match query blocks
			qbMatch := true
			if astTbl.QBName.L != "" {
				expectedOffset := extractSelectOffset(astTbl.QBName.L)
				if expectedOffset > 0 {
					qbMatch = tableAlias.SelectOffset == expectedOffset
				} else {
					// If QBName cannot be parsed, ignore the QB match.
					qbMatch = true
				}
			}
			if dbMatch && tableMatch && qbMatch {
				newPlans := append(plans[:i], plans[i+1:]...)
				return joinGroup, newPlans, true
			}
		}
	}

	// Step 2: Match by query-block alias (subquery name)
	// Only execute this step if no direct table name match was found
	matchIdx := -1
	for i, joinGroup := range plans {
		plan := getPlan(joinGroup)
		blockOffset := plan.QueryBlockOffset()
		if blockOffset > 1 && blockOffset < len(queryBlockNames) {
			blockName := queryBlockNames[blockOffset]
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == blockName.DBName.L
			tableMatch := astTbl.TableName.L == blockName.TableName.L
			if dbMatch && tableMatch {
				if matchIdx != -1 {
					intest.Assert(false, "leading subquery alias matches multiple join groups")
					return zero, plans, false
				}
				matchIdx = i
			}
		}
	}
	if matchIdx != -1 {
		// take the matched plan before slice manipulation. `append(plans[:matchIdx], ...)`
		// may overwrite `plans[matchIdx]` due to shared backing arrays.
		matched := plans[matchIdx]
		newPlans := append(plans[:matchIdx], plans[matchIdx+1:]...)
		return matched, newPlans, true
	}

	return zero, plans, false
}

// extract the number x from 'sel_x'
func extractSelectOffset(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
}

// IsDerivedTableInLeadingHint checks if a plan node represents a derived table (subquery)
// that is explicitly referenced in the LEADING hint.
func IsDerivedTableInLeadingHint(p base.LogicalPlan, leadingHint *hint.PlanHints) bool {
	if leadingHint == nil || leadingHint.LeadingList == nil {
		return false
	}

	// Get the query block names mapping to find derived table aliases
	var queryBlockNames []ast.HintTable
	names := p.SCtx().GetSessionVars().PlannerSelectBlockAsName.Load()
	if names == nil {
		return false
	}
	queryBlockNames = *names

	// Get the block offset of this plan node
	blockOffset := p.QueryBlockOffset()

	// Only blockOffset values in [2, len(queryBlockNames)-1] can represent
	// subqueries / derived tables. Offsets 0 and 1 are typically main query
	// or CTE, and offsets beyond the end of queryBlockNames are invalid.
	if blockOffset <= 1 || blockOffset >= len(queryBlockNames) {
		return false
	}

	// Get the alias name of this derived table
	derivedTableAlias := queryBlockNames[blockOffset].TableName.L
	if derivedTableAlias == "" {
		return false
	}
	derivedDBName := queryBlockNames[blockOffset].DBName.L

	// Check if this alias appears in the LEADING hint
	return containsTableInLeadingList(leadingHint.LeadingList, derivedDBName, derivedTableAlias)
}

// containsTableInLeadingList recursively searches for a table name in the LEADING hint structure
func containsTableInLeadingList(leadingList *ast.LeadingList, dbName, tableName string) bool {
	if leadingList == nil {
		return false
	}

	for _, item := range leadingList.Items {
		switch element := item.(type) {
		case *ast.HintTable:
			// Direct table reference in LEADING hint
			dbMatch := element.DBName.L == "" || element.DBName.L == dbName || element.DBName.L == "*"
			tableMatch := element.TableName.L == tableName
			if dbMatch && tableMatch {
				return true
			}
		case *ast.LeadingList:
			// Nested structure, recursively check
			if containsTableInLeadingList(element, dbName, tableName) {
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
