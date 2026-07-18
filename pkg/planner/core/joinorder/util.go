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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
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

type exprReplacer func(expr expression.Expression) (newExpr expression.Expression, replaced bool)

// rewriteExprTree rewrites an expression tree in a best-effort, copy-on-write way.
//
// The replacer is applied in pre-order (parent before children). If it replaces a node,
// the returned expression will be rewritten again so callers can implement recursive
// substitutions (e.g. colExprMap chains) without duplicating traversal logic.
func rewriteExprTree(expr expression.Expression, replace exprReplacer) expression.Expression {
	if expr == nil {
		return nil
	}

	if replace != nil {
		if newExpr, replaced := replace(expr); replaced {
			if newExpr == nil {
				return nil
			}
			if newExpr != expr {
				return rewriteExprTree(newExpr, replace)
			}
		}
	}

	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return expr
	}

	// Copy-on-write: only clone the function node when any argument changes.
	oldArgs := sf.GetArgs()
	var newArgs []expression.Expression
	for i, arg := range oldArgs {
		rewrittenArg := rewriteExprTree(arg, replace)
		if newArgs == nil {
			if rewrittenArg == arg {
				continue
			}
			newArgs = make([]expression.Expression, len(oldArgs))
			copy(newArgs, oldArgs[:i])
		}
		newArgs[i] = rewrittenArg
	}
	if newArgs == nil {
		return sf
	}

	newSf := sf.Clone().(*expression.ScalarFunction)
	args := newSf.GetArgs()
	for i := range args {
		args[i] = newArgs[i]
	}
	// Args changed: clear cached hash so CanonicalHashCode reflects rewritten children.
	newSf.CleanHashCode()
	return newSf
}

// SubstituteColsInEqEdges substitutes derived columns in equality edges using colExprMap.
func SubstituteColsInEqEdges(edges []*expression.ScalarFunction, colExprMap map[int64]expression.Expression) []*expression.ScalarFunction {
	result := make([]*expression.ScalarFunction, 0, len(edges))
	for _, edge := range edges {
		substituted := SubstituteColsInExpr(edge, colExprMap)
		if sf, ok := substituted.(*expression.ScalarFunction); ok {
			result = append(result, sf)
		} else {
			result = append(result, edge)
		}
	}
	return result
}

// SubstituteColsInExprs substitutes derived columns in a list of expressions using colExprMap.
func SubstituteColsInExprs(exprs []expression.Expression, colExprMap map[int64]expression.Expression) []expression.Expression {
	result := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		result = append(result, SubstituteColsInExpr(expr, colExprMap))
	}
	return result
}

// SubstituteColsInExpr recursively substitutes derived columns in an expression using colExprMap.
// It replaces column references with their defining expressions from colExprMap.
func SubstituteColsInExpr(expr expression.Expression, colExprMap map[int64]expression.Expression) expression.Expression {
	if len(colExprMap) == 0 {
		return expr
	}
	return rewriteExprTree(expr, func(e expression.Expression) (expression.Expression, bool) {
		col, ok := e.(*expression.Column)
		if !ok {
			return e, false
		}
		if defExpr, ok := colExprMap[col.UniqueID]; ok {
			// Expressions in colExprMap are treated as immutable in join-reorder flow.
			// Reuse pointers to avoid extra clones/allocations; if a future pass starts
			// mutating these expression trees in-place, clone defExpr here before return.
			return defExpr, true
		}
		return e, false
	})
}

// OuterJoinSideFiltersTouchMultipleLeaves checks whether the outer-join filters depend on more than one
// leaf on the outer side. If so, we conservatively disable join reordering for this join node.
//
// When projections are inlined under the outer side, join conditions may reference derived columns that
// are not contained in any leaf schema. We substitute those derived columns via `outerColExprMap` before
// extracting referenced columns.
func OuterJoinSideFiltersTouchMultipleLeaves(
	join *logicalop.LogicalJoin,
	outerGroup []base.LogicalPlan,
	outerColExprMap map[int64]expression.Expression,
	outerIsLeft bool,
) bool {
	if join == nil {
		return false
	}

	checkOtherConds := join.OtherConditions
	checkSideConds := join.RightConditions
	if outerIsLeft {
		checkSideConds = join.LeftConditions
	}
	checkEQConds := expression.ScalarFuncs2Exprs(join.EqualConditions)

	if len(outerColExprMap) > 0 {
		checkOtherConds = SubstituteColsInExprs(checkOtherConds, outerColExprMap)
		checkSideConds = SubstituteColsInExprs(checkSideConds, outerColExprMap)
		checkEQConds = SubstituteColsInExprs(checkEQConds, outerColExprMap)
	}

	extractedCols := make(map[int64]*expression.Column, len(checkOtherConds)+len(checkSideConds)+len(checkEQConds))
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkOtherConds...)
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkSideConds...)
	expression.ExtractColumnsMapFromExpressionsWithReusedMap(extractedCols, nil, checkEQConds...)

	affectedGroups := 0
	for _, outerLeaf := range outerGroup {
		leafSchema := outerLeaf.Schema()
		for _, col := range extractedCols {
			if leafSchema.Contains(col) {
				affectedGroups++
				break
			}
		}
		if affectedGroups > 1 {
			return true
		}
	}
	return false
}

// GetEqEdgeArgsAndCols returns the two arguments of an equality edge and the columns referenced on each side.
func GetEqEdgeArgsAndCols(edge *expression.ScalarFunction) (lArg, rArg expression.Expression, lCols, rCols []*expression.Column, ok bool) {
	if edge == nil {
		return nil, nil, nil, nil, false
	}
	args := edge.GetArgs()
	if len(args) != 2 {
		return nil, nil, nil, nil, false
	}
	lArg, rArg = args[0], args[1]
	lCols = expression.ExtractColumns(lArg)
	rCols = expression.ExtractColumns(rArg)
	return lArg, rArg, lCols, rCols, true
}

// AlignJoinEdgeArgs tries to align a join equality edge arguments to (leftSchema, rightSchema).
//
// It returns (lExpr, rExpr, swapped, ok):
//   - ok is true if the edge connects the two schemas in either direction.
//   - lExpr is guaranteed to be computable from leftSchema and rExpr from rightSchema.
//   - swapped indicates the original args were in reverse order and had to be swapped.
func AlignJoinEdgeArgs(
	lArg, rArg expression.Expression,
	leftSchema, rightSchema *expression.Schema,
) (lExpr, rExpr expression.Expression, swapped, ok bool) {
	if expression.ExprFromSchema(lArg, leftSchema) && expression.ExprFromSchema(rArg, rightSchema) {
		return lArg, rArg, false, true
	}
	if expression.ExprFromSchema(lArg, rightSchema) && expression.ExprFromSchema(rArg, leftSchema) {
		// Swap to match (leftSchema, rightSchema) order.
		return rArg, lArg, true, true
	}
	return nil, nil, false, false
}

// FindAndRemovePlanByAstHint find the plan in `plans` that matches `ast.HintTable` and remove that plan, returning the new slice.
// Matching rules:
//  1. Match by regular table name (db/table/*)
//  2. Match by query-block alias (subquery name, e.g., tx)
//  3. If multiple join groups belong to the same block alias, mark as ambiguous and skip (consistent with old logic)
//
// NOTE: T is usually be *Node or base.LogicalPlan, we use generics because we want to reuse this function in both the old and new join order code.
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
