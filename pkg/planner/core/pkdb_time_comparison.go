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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type timeComparisonOperand struct {
	expr expression.Expression
	node ast.ExprNode
	name *types.FieldName
}

// castTemporalConstantForTimeColumnComparison aligns a constant temporal value
// with a direct TIME column. A DATETIME value contains a date that is not part
// of a TIME comparison, so retaining the DATETIME comparison would use the
// wrong MySQL coercion rule and can prevent an index range from being built.
func (er *expressionRewriter) castTemporalConstantForTimeColumnComparison(
	lhs, rhs timeComparisonOperand,
) (expression.Expression, expression.Expression) {
	if er.planCtx != nil && er.planCtx.builder != nil && !fixcontrol.GetBoolWithDefault(
		er.planCtx.builder.ctx.GetSessionVars().GetOptimizerFixControlMap(), fixcontrol.Fix70751, true,
	) {
		return lhs.expr, rhs.expr
	}
	operands := [2]timeComparisonOperand{lhs, rhs}
	evalCtx := er.sctx.GetEvalCtx()
	for constantIdx := range operands {
		constant := operands[constantIdx]
		constantType := constant.expr.GetType(evalCtx).GetType()
		if constantType != mysql.TypeDate && constantType != mysql.TypeDatetime && constantType != mysql.TypeTimestamp {
			continue
		}
		columnIdx := 1 - constantIdx
		columnType, ok := er.directTimeColumnType(evalCtx, operands[columnIdx])
		if !ok || !er.isEligibleTimeComparisonConstant(evalCtx, constant) {
			continue
		}
		operands[constantIdx].expr = expression.BuildCastFunction(er.sctx, constant.expr, columnType.Clone())
		return operands[0].expr, operands[1].expr
	}
	return operands[0].expr, operands[1].expr
}

func (er *expressionRewriter) directTimeColumnType(
	ctx expression.EvalContext,
	operand timeComparisonOperand,
) (*types.FieldType, bool) {
	if er.planCtx == nil {
		return nil, false
	}
	if _, ok := unwrapTimeComparisonParentheses(operand.node).(*ast.ColumnNameExpr); !ok {
		return nil, false
	}
	column, ok := operand.expr.(*expression.Column)
	if !ok || operand.name == nil || operand.name.OrigTblName.L == "" || column.VirtualExpr != nil {
		return nil, false
	}
	if !containsDirectTimeComparisonColumn(er.planCtx.plan, column) {
		return nil, false
	}
	tp := column.GetType(ctx)
	if tp.GetType() != mysql.TypeDuration {
		return nil, false
	}
	return tp, true
}

// containsDirectTimeComparisonColumn walks the logical input scope carried by
// the AST rewriter. Only operators that preserve base table columns are
// transparent. A projection is transparent only when its output is an
// identity column reference; expression and set-operation outputs are stopped
// because their expressions may have different temporal semantics.
func containsDirectTimeComparisonColumn(plan base.LogicalPlan, column *expression.Column) bool {
	if plan == nil {
		return false
	}
	switch p := plan.(type) {
	case *logicalop.DataSource:
		return p.Schema().Contains(column)
	case *logicalop.LogicalProjection:
		if p.Proj4Expand || len(p.Children()) != 1 {
			return false
		}
		idx := p.Schema().ColumnIndex(column)
		if idx < 0 || idx >= len(p.Exprs) {
			return false
		}
		source, ok := p.Exprs[idx].(*expression.Column)
		if !ok || source.VirtualExpr != nil {
			return false
		}
		return containsDirectTimeComparisonColumn(p.Children()[0], source)
	case *logicalop.LogicalJoin:
		// A join exposes both child schemas. Its output may coalesce USING
		// columns, but the expression rewriter still resolves qualified base
		// columns against the full child plans.
		for _, child := range p.Children() {
			if containsDirectTimeComparisonColumn(child, column) {
				return true
			}
		}
	case *logicalop.LogicalUnionScan, *logicalop.LogicalApply, *logicalop.LogicalSelection,
		*logicalop.LogicalLimit, *logicalop.LogicalTopN, *logicalop.LogicalSort,
		*logicalop.LogicalMaxOneRow:
		for _, child := range plan.Children() {
			if containsDirectTimeComparisonColumn(child, column) {
				return true
			}
		}
	}
	return false
}

func (er *expressionRewriter) isEligibleTimeComparisonConstant(ctx expression.EvalContext, operand timeComparisonOperand) bool {
	node := unwrapTimeComparisonParentheses(operand.node)
	// A bare parameter marker is typed from its current value. Expressions
	// around a marker retain their own type and are handled by runtime casts.
	_, isParamMarker := node.(ast.ParamMarkerExpr)
	if isParamMarker {
		if er.planCtx != nil && er.planCtx.builder != nil && er.planCtx.builder.ctx.GetSessionVars().GetCallProcedure() {
			return false
		}
	} else {
		if operand.expr.ConstLevel() != expression.ConstStrict {
			return false
		}
		checker := timeComparisonConstantChecker{ctx: er.sctx}
		node.Accept(&checker)
		if checker.dynamic {
			return false
		}
	}
	value, isNull, err := operand.expr.EvalTime(ctx, chunk.Row{})
	return err == nil && !isNull && !value.InvalidZero()
}

func unwrapTimeComparisonParentheses(node ast.ExprNode) ast.ExprNode {
	for {
		parenthesized, ok := node.(*ast.ParenthesesExpr)
		if !ok {
			return node
		}
		node = parenthesized.Expr
	}
}

type timeComparisonConstantChecker struct {
	ctx     expression.BuildContext
	dynamic bool
}

func (c *timeComparisonConstantChecker) Enter(node ast.Node) (ast.Node, bool) {
	switch x := node.(type) {
	case ast.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.VariableExpr, *ast.SubqueryExpr:
		c.dynamic = true
	case *ast.FuncCallExpr:
		c.dynamic = c.dynamic ||
			isUserDefinedStoredFunction(x) ||
			expression.IsDeferredFunctions(c.ctx, x.FnName.L)
	}
	return node, c.dynamic
}

func (c *timeComparisonConstantChecker) Leave(node ast.Node) (ast.Node, bool) {
	return node, !c.dynamic
}
