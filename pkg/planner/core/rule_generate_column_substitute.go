// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util/optimizetrace"
	"github.com/pingcap/tidb/pkg/types"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

type gcSubstituter struct {
}

// ExprColumnMap is used to store all expressions of indexed generated columns in a table,
// and map them to the generated columns,
// thus we can substitute the expression in a query to an indexed generated column.
type ExprColumnMap map[expression.Expression]*expression.Column

// optimize try to replace the expression to indexed virtual generate column in where, group by, order by, and field clause
// so that we can use the index on expression.
// For example: select a+1 from t order by a+1, with a virtual generate column c as (a+1) and
// an index on c. We need to replace a+1 with c so that we can use the index on c.
// See also https://dev.mysql.com/doc/refman/8.0/en/generated-column-index-optimizations.html
func (gc *gcSubstituter) optimize(ctx context.Context, lp base.LogicalPlan, opt *optimizetrace.LogicalOptimizeOp) (base.LogicalPlan, bool, error) {
	planChanged := false
	exprToColumn := make(ExprColumnMap)
	collectGenerateColumn(lp, exprToColumn)
	if len(exprToColumn) == 0 {
		return lp, planChanged, nil
	}
	return gc.substitute(ctx, lp, exprToColumn, opt), planChanged, nil
}

// collectGenerateColumn collect the generate column and save them to a map from their expressions to themselves.
// For the sake of simplicity, we don't collect the stored generate column because we can't get their expressions directly.
// TODO: support stored generate column.
func collectGenerateColumn(lp base.LogicalPlan, exprToColumn ExprColumnMap) {
	if _, ok := lp.(*LogicalCTE); ok {
		return
	}
	for _, child := range lp.Children() {
		collectGenerateColumn(child, exprToColumn)
	}
	ds, ok := lp.(*DataSource)
	if !ok {
		return
	}
	// detect the read_from_storage(tiflash) hints, since virtual column will
	// block the mpp task spreading (only supporting MPP table scan), causing
	// mpp plan fail the cost comparison with tikv index plan.
	if ds.preferStoreType&h.PreferTiFlash != 0 {
		return
	}
	ectx := lp.SCtx().GetExprCtx().GetEvalCtx()
	for _, p := range ds.possibleAccessPaths {
		if p.IsTablePath() {
			continue
		}
		for _, idxPart := range p.Index.Columns {
			colInfo := ds.tableInfo.Columns[idxPart.Offset]
			if colInfo.IsGenerated() && !colInfo.GeneratedStored {
				s := ds.schema.Columns
				col := expression.ColInfo2Col(s, colInfo)
				if col != nil && col.GetType(ectx).PartialEqual(col.VirtualExpr.GetType(ectx), lp.SCtx().GetSessionVars().EnableUnsafeSubstitute) {
					exprToColumn[col.VirtualExpr] = col
				}
			}
		}
	}
}

func tryToSubstituteExpr(expr *expression.Expression, lp base.LogicalPlan, candidateExpr expression.Expression, tp types.EvalType, schema *expression.Schema, col *expression.Column, opt *optimizetrace.LogicalOptimizeOp) bool {
	changed := false
	ectx := lp.SCtx().GetExprCtx().GetEvalCtx()
	if (*expr).Equal(ectx, candidateExpr) && candidateExpr.GetType(ectx).EvalType() == tp &&
		schema.ColumnIndex(col) != -1 {
		*expr = col
		appendSubstituteColumnStep(lp, candidateExpr, col, opt)
		changed = true
	}
	return changed
}

func appendSubstituteColumnStep(lp base.LogicalPlan, candidateExpr expression.Expression, col *expression.Column, opt *optimizetrace.LogicalOptimizeOp) {
	reason := func() string { return "" }
	action := func() string {
		buffer := bytes.NewBufferString("expression:")
		buffer.WriteString(candidateExpr.String())
		buffer.WriteString(" substituted by")
		buffer.WriteString(" column:")
		buffer.WriteString(col.String())
		return buffer.String()
	}
	opt.AppendStepToCurrent(lp.ID(), lp.TP(), reason, action)
}

// SubstituteExpression is Exported for bench
func SubstituteExpression(cond expression.Expression, lp base.LogicalPlan, exprToColumn ExprColumnMap, schema *expression.Schema, opt *optimizetrace.LogicalOptimizeOp) bool {
	return substituteExpression(cond, lp, exprToColumn, schema, opt)
}

func substituteExpression(cond expression.Expression, lp base.LogicalPlan, exprToColumn ExprColumnMap, schema *expression.Schema, opt *optimizetrace.LogicalOptimizeOp) bool {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return false
	}
	changed := false
	collectChanged := func(partial bool) {
		if partial && !changed {
			changed = true
		}
	}
	defer func() {
		// If the argument is not changed, hash code doesn't need to recount again.
		if changed {
			expression.ReHashCode(sf)
		}
	}()
	var expr *expression.Expression
	var tp types.EvalType
	ectx := lp.SCtx().GetExprCtx().GetEvalCtx()
	switch sf.FuncName.L {
	case ast.EQ, ast.LT, ast.LE, ast.GT, ast.GE:
		for candidateExpr, column := range exprToColumn {
			collectChanged(tryToSubstituteExpr(&sf.GetArgs()[1], lp, candidateExpr, sf.GetArgs()[0].GetType(ectx).EvalType(), schema, column, opt))
		}
		for candidateExpr, column := range exprToColumn {
			collectChanged(tryToSubstituteExpr(&sf.GetArgs()[0], lp, candidateExpr, sf.GetArgs()[1].GetType(ectx).EvalType(), schema, column, opt))
		}
	case ast.In:
		expr = &sf.GetArgs()[0]
		tp = sf.GetArgs()[1].GetType(ectx).EvalType()
		canSubstitute := true
		// Can only substitute if all the operands on the right-hand
		// side are the same type.
		for i := 1; i < len(sf.GetArgs()); i++ {
			if sf.GetArgs()[i].GetType(ectx).EvalType() != tp {
				canSubstitute = false
				break
			}
		}
		if canSubstitute {
			for candidateExpr, column := range exprToColumn {
				collectChanged(tryToSubstituteExpr(expr, lp, candidateExpr, tp, schema, column, opt))
			}
		}
	case ast.Like:
		expr = &sf.GetArgs()[0]
		tp = sf.GetArgs()[1].GetType(ectx).EvalType()
		for candidateExpr, column := range exprToColumn {
			collectChanged(tryToSubstituteExpr(expr, lp, candidateExpr, tp, schema, column, opt))
		}
	case ast.LogicOr, ast.LogicAnd:
		collectChanged(substituteExpression(sf.GetArgs()[0], lp, exprToColumn, schema, opt))
		collectChanged(substituteExpression(sf.GetArgs()[1], lp, exprToColumn, schema, opt))
	case ast.UnaryNot:
		collectChanged(substituteExpression(sf.GetArgs()[0], lp, exprToColumn, schema, opt))
	}
	return changed
}

func (gc *gcSubstituter) substitute(ctx context.Context, lp base.LogicalPlan, exprToColumn ExprColumnMap, opt *optimizetrace.LogicalOptimizeOp) base.LogicalPlan {
	var tp types.EvalType
	ectx := lp.SCtx().GetExprCtx().GetEvalCtx()
	switch x := lp.(type) {
	case *LogicalSelection:
		for _, cond := range x.Conditions {
			substituteExpression(cond, lp, exprToColumn, x.Schema(), opt)
		}
	case *LogicalProjection:
		for i := range x.Exprs {
			tp = x.Exprs[i].GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				tryToSubstituteExpr(&x.Exprs[i], lp, candidateExpr, tp, x.Children()[0].Schema(), column, opt)
			}
		}
	case *LogicalSort:
		for i := range x.ByItems {
			tp = x.ByItems[i].Expr.GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				tryToSubstituteExpr(&x.ByItems[i].Expr, lp, candidateExpr, tp, x.Schema(), column, opt)
			}
		}
	case *LogicalAggregation:
		for _, aggFunc := range x.AggFuncs {
			for i := 0; i < len(aggFunc.Args); i++ {
				tp = aggFunc.Args[i].GetType(ectx).EvalType()
				for candidateExpr, column := range exprToColumn {
					if aggFunc.Args[i].Equal(lp.SCtx().GetExprCtx().GetEvalCtx(), candidateExpr) && candidateExpr.GetType(ectx).EvalType() == tp &&
						x.Schema().ColumnIndex(column) != -1 {
						aggFunc.Args[i] = column
						appendSubstituteColumnStep(lp, candidateExpr, column, opt)
					}
				}
			}
		}
		for i := 0; i < len(x.GroupByItems); i++ {
			tp = x.GroupByItems[i].GetType(ectx).EvalType()
			for candidateExpr, column := range exprToColumn {
				if x.GroupByItems[i].Equal(lp.SCtx().GetExprCtx().GetEvalCtx(), candidateExpr) && candidateExpr.GetType(ectx).EvalType() == tp &&
					x.Schema().ColumnIndex(column) != -1 {
					x.GroupByItems[i] = column
					appendSubstituteColumnStep(lp, candidateExpr, column, opt)
				}
			}
		}
	}
	for _, child := range lp.Children() {
		gc.substitute(ctx, child, exprToColumn, opt)
	}
	return lp
}

func (*gcSubstituter) name() string {
	return "generate_column_substitute"
}
