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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

type gcSubstituter struct {
}

func (gc *gcSubstituter) optimize(ctx context.Context, lp LogicalPlan) (LogicalPlan, error) {
	exprToColumn := make(map[expression.Expression]expression.Expression)
	collectGenerateColumn(lp, exprToColumn)
	return gc.substitute(ctx, lp, exprToColumn, lp.SCtx().GetSessionVars().StmtCtx), nil
}

func collectGenerateColumn(lp LogicalPlan, exprToColumn map[expression.Expression]expression.Expression) {
	if ds, ok := lp.(*DataSource); ok {
		tblInfo := ds.tableInfo
		for _, idx := range tblInfo.Indices {
			for _, idxPart := range idx.Columns {
				colInfo := tblInfo.Columns[idxPart.Offset]
				if colInfo.IsGenerated() && !colInfo.GeneratedStored {
					s := ds.schema.Columns
					col := expression.ColInfo2Col(s, colInfo)
					if col != nil {
						exprToColumn[col.VirtualExpr] = col
					}
				}
			}
		}
		return
	}
	for _, child := range lp.Children() {
		collectGenerateColumn(child, exprToColumn)
	}
}

func (gc *gcSubstituter) substitute(ctx context.Context, lp LogicalPlan, exprToColumn map[expression.Expression]expression.Expression, sctx *stmtctx.StatementContext) LogicalPlan {
	var expr *expression.Expression
	var tp types.EvalType
	switch x := lp.(type) {
	case *LogicalSelection:
		for _, cond := range x.Conditions {
			sf, ok := cond.(*expression.ScalarFunction)
			if !ok {
				continue
			}
			switch sf.FuncName.L {
			case ast.EQ, ast.LT, ast.LE, ast.GT, ast.GE:
				if sf.GetArgs()[0].ConstItem(sctx) {
					expr = &sf.GetArgs()[1]
					tp = sf.GetArgs()[0].GetType().EvalType()
				} else if sf.GetArgs()[1].ConstItem(sctx) {
					expr = &sf.GetArgs()[0]
					tp = sf.GetArgs()[1].GetType().EvalType()
				} else {
					continue
				}
				for candidateExpr, column := range exprToColumn {
					if (*expr).Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
						x.Schema().ColumnIndex(column.(*expression.Column)) != -1 {
						*expr = column
					}
				}
			case ast.In:
				expr = &sf.GetArgs()[0]
				tp = sf.GetArgs()[1].GetType().EvalType()
				canSubstitute := true
				// Can only substitute if all the operands on the right-hand
				// side are constants of the same type.
				for i := 1; i < len(sf.GetArgs()); i++ {
					if !sf.GetArgs()[i].ConstItem(sctx) || sf.GetArgs()[i].GetType().EvalType() != tp {
						canSubstitute = false
						break
					}
				}
				if canSubstitute {
					for candidateExpr, column := range exprToColumn {
						if (*expr).Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
							x.Schema().ColumnIndex(column.(*expression.Column)) != -1 {
							*expr = column
						}
					}
				}
			}
		}
	case *LogicalProjection:
		{
			for i := 0; i < len(x.Exprs); i++ {
				tp = x.Exprs[i].GetType().EvalType()
				for candidateExpr, column := range exprToColumn {
					if x.Exprs[i].Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
						x.children[0].Schema().ColumnIndex(column.(*expression.Column)) != -1 {
						x.Exprs[i] = column
					}
				}
			}
		}
	case *LogicalSort:
		for i := 0; i < len(x.ByItems); i++ {
			tp = x.ByItems[i].Expr.GetType().EvalType()
			for candidateExpr, column := range exprToColumn {
				if x.ByItems[i].Expr.Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
					x.Schema().ColumnIndex(column.(*expression.Column)) != -1 {
					x.ByItems[i].Expr = column
				}
			}
		}
	case *LogicalAggregation:
		for _, aggFunc := range x.AggFuncs {
			for i := 0; i < len(aggFunc.Args); i++ {
				tp = aggFunc.Args[i].GetType().EvalType()
				for candidateExpr, column := range exprToColumn {
					if aggFunc.Args[i].Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
						x.Schema().ColumnIndex(column.(*expression.Column)) != -1 {
						aggFunc.Args[i] = column
					}
				}
			}
		}
		for i := 0; i < len(x.GroupByItems); i++ {
			tp = x.GroupByItems[i].GetType().EvalType()
			for candidateExpr, column := range exprToColumn {
				if x.GroupByItems[i].Equal(lp.SCtx(), candidateExpr) && candidateExpr.GetType().EvalType() == tp &&
					x.Schema().ColumnIndex(column.(*expression.Column)) != -1 {
					x.GroupByItems[i] = column
					x.groupByCols = append(x.groupByCols, column.(*expression.Column))
				}
			}
		}
	}
	for _, child := range lp.Children() {
		gc.substitute(ctx, child, exprToColumn, sctx)
	}
	return lp
}

func (*gcSubstituter) name() string {
	return "generate_column_substitute"
}
