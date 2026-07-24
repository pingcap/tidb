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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
)

// tryInlineProjectionForJoinGroup tries to extract the join group through a Projection on top of a Join.
//
// It returns (result, true) when the Projection node is handled:
//   - inlined successfully, or
//   - determined unsafe to inline and kept as an atomic leaf.
//
// It returns (nil, false) when the caller should continue with normal extractJoinGroupImpl logic.
func tryInlineProjectionForJoinGroup(p base.LogicalPlan, proj *logicalop.LogicalProjection) (*joinGroupResult, bool) {
	child := proj.Children()[0]
	if _, isJoin := child.(*logicalop.LogicalJoin); !isJoin {
		return nil, false
	}

	if !canInlineProjectionBasic(proj) {
		return nil, false
	}

	childResult := extractJoinGroupImpl(child)
	if !canInlineProjection(proj, childResult) {
		return &joinGroupResult{
			group:              []base.LogicalPlan{p},
			basicJoinGroupInfo: &basicJoinGroupInfo{},
		}, true
	}

	childResult.colExprMap = buildColExprMapForProjection(proj, childResult.colExprMap)
	return childResult, true
}

func buildColExprMapForProjection(
	proj *logicalop.LogicalProjection,
	childColExprMap map[int64]expression.Expression,
) map[int64]expression.Expression {
	colExprMap := make(map[int64]expression.Expression, len(proj.Schema().Columns)+len(childColExprMap))
	for i, outCol := range proj.Schema().Columns {
		expr := proj.Exprs[i]
		if len(childColExprMap) > 0 {
			expr = substituteColsInExpr(expr, childColExprMap)
		}
		if colExpr, isCol := expr.(*expression.Column); isCol && colExpr.UniqueID == outCol.UniqueID {
			continue
		}
		colExprMap[outCol.UniqueID] = expr
	}

	for k, v := range childColExprMap {
		if _, exists := colExprMap[k]; !exists {
			colExprMap[k] = v
		}
	}
	return colExprMap
}

func isInlineableProjectionExpr(expr expression.Expression) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return true
	case *expression.ScalarFunction:
		for _, arg := range x.GetArgs() {
			if !isInlineableProjectionExpr(arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		return x.DeferredExpr == nil
	default:
		return false
	}
}

func canInlineProjectionBasic(proj *logicalop.LogicalProjection) bool {
	if proj.Proj4Expand {
		return false
	}

	for _, expr := range proj.Exprs {
		if len(expression.ExtractColumns(expr)) == 0 {
			return false
		}
		if !isInlineableProjectionExpr(expr) {
			return false
		}
		if expression.IsMutableEffectsExpr(expr) || expression.CheckNonDeterministic(expr) || expr.IsCorrelated() {
			return false
		}
	}
	return true
}

func canInlineProjection(proj *logicalop.LogicalProjection, childResult *joinGroupResult) bool {
	if childResult == nil {
		return false
	}

	leafByColID := make(map[int64]int)
	for leafIdx, leaf := range childResult.group {
		for _, col := range leaf.Schema().Columns {
			if prev, ok := leafByColID[col.UniqueID]; ok && prev != leafIdx {
				return false
			}
			leafByColID[col.UniqueID] = leafIdx
		}
	}

	for _, expr := range proj.Exprs {
		checkExpr := expr
		if len(childResult.colExprMap) > 0 {
			checkExpr = substituteColsInExpr(checkExpr, childResult.colExprMap)
		}
		if childResult.nullExtendedCols != nil && expression.ExprReferenceSchema(checkExpr, childResult.nullExtendedCols) {
			// Values from a null-extended side are produced by the outer join itself, so
			// treating them as ordinary leaf-local expressions can change SQL semantics.
			return false
		}

		cols := expression.ExtractColumns(checkExpr)
		if len(cols) == 0 {
			return false
		}

		leafIdx := -1
		for _, col := range cols {
			idx, ok := leafByColID[col.UniqueID]
			if !ok {
				return false
			}
			if leafIdx == -1 {
				leafIdx = idx
				continue
			}
			if idx != leafIdx {
				return false
			}
		}
	}
	return true
}

type exprReplacer func(expr expression.Expression) (newExpr expression.Expression, replaced bool)

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
	newSf.CleanHashCode()
	return newSf
}

func substituteColsInEqEdges(edges []*expression.ScalarFunction, colExprMap map[int64]expression.Expression) []*expression.ScalarFunction {
	result := make([]*expression.ScalarFunction, 0, len(edges))
	for _, edge := range edges {
		// substituteColsInExpr only rewrites columns inside the edge, so the
		// top-level equality function must remain a scalar function.
		result = append(result, substituteColsInExpr(edge, colExprMap).(*expression.ScalarFunction))
	}
	return result
}

func substituteColsInExprs(exprs []expression.Expression, colExprMap map[int64]expression.Expression) []expression.Expression {
	result := make([]expression.Expression, 0, len(exprs))
	for _, expr := range exprs {
		result = append(result, substituteColsInExpr(expr, colExprMap))
	}
	return result
}

func substituteColsInExpr(expr expression.Expression, colExprMap map[int64]expression.Expression) expression.Expression {
	if len(colExprMap) == 0 {
		return expr
	}
	return rewriteExprTree(expr, func(e expression.Expression) (expression.Expression, bool) {
		col, ok := e.(*expression.Column)
		if !ok {
			return e, false
		}
		if defExpr, ok := colExprMap[col.UniqueID]; ok {
			if col.InOperand {
				return expression.SetExprColumnInOperand(defExpr.Clone()), true
			}
			return defExpr, true
		}
		return e, false
	})
}

func outerJoinSideFiltersTouchMultipleLeaves(
	join *logicalop.LogicalJoin,
	outerGroup []base.LogicalPlan,
	outerColExprMap map[int64]expression.Expression,
	outerIsLeft bool,
) bool {
	checkOtherConds := join.OtherConditions
	checkSideConds := join.RightConditions
	if outerIsLeft {
		checkSideConds = join.LeftConditions
	}
	checkEQConds := expression.ScalarFuncs2Exprs(join.EqualConditions)

	if len(outerColExprMap) > 0 {
		checkOtherConds = substituteColsInExprs(checkOtherConds, outerColExprMap)
		checkSideConds = substituteColsInExprs(checkSideConds, outerColExprMap)
		checkEQConds = substituteColsInExprs(checkEQConds, outerColExprMap)
	}

	extractedCols := make([]*expression.Column, 0, 8)
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, checkOtherConds, nil)
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, checkSideConds, nil)
	extractedCols = expression.ExtractColumnsFromExpressions(extractedCols, checkEQConds, nil)

	affectedGroups := 0
	for _, outerLeaf := range outerGroup {
		for _, col := range extractedCols {
			if outerLeaf.Schema().Contains(col) {
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

func getEqEdgeArgsAndCols(edge *expression.ScalarFunction) (lArg, rArg expression.Expression, lCols, rCols []*expression.Column, ok bool) {
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

func alignJoinEdgeArgs(
	lArg, rArg expression.Expression,
	leftSchema, rightSchema *expression.Schema,
) (lExpr, rExpr expression.Expression, swapped, ok bool) {
	if expression.ExprFromSchema(lArg, leftSchema) && expression.ExprFromSchema(rArg, rightSchema) {
		return lArg, rArg, false, true
	}
	if expression.ExprFromSchema(lArg, rightSchema) && expression.ExprFromSchema(rArg, leftSchema) {
		return rArg, lArg, true, true
	}
	return nil, nil, false, false
}
