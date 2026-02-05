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

package core

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
)

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
// T is usually be *jrNode or base.LogicalPlan, we use generics because we want to reuse this function in both the old and new join order code.
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
	groupIdx := -1
	for i, joinGroup := range plans {
		plan := getPlan(joinGroup)
		blockOffset := plan.QueryBlockOffset()
		if blockOffset > 1 && blockOffset < len(queryBlockNames) {
			blockName := queryBlockNames[blockOffset]
			dbMatch := astTbl.DBName.L == "" || astTbl.DBName.L == blockName.DBName.L
			tableMatch := astTbl.TableName.L == blockName.TableName.L
			if dbMatch && tableMatch {
				// this can happen when multiple join groups are from the same block, for example:
				//   select /*+ leading(tx) */ * from (select * from t1, t2 ...) tx, ...
				// `tx` is split to 2 join groups `t1` and `t2`, and they have the same block offset.
				// TODO: currently we skip this case for simplification, we can support it in the future.
				if groupIdx != -1 {
					groupIdx = -1
					break
				}
				groupIdx = i
			}
		}
	}

	if groupIdx != -1 {
		matched := plans[groupIdx]
		newPlans := append(plans[:groupIdx], plans[groupIdx+1:]...)
		return matched, newPlans, true
	}

	return zero, plans, false
}

// extractSelectOffset extracts the number x from 'sel_x'.
func extractSelectOffset(qbName string) int {
	if strings.HasPrefix(qbName, "sel_") {
		if offset, err := strconv.Atoi(qbName[4:]); err == nil {
			return offset
		}
	}
	return -1
}
