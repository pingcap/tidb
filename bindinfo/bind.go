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

package bindinfo

import "github.com/pingcap/parser/ast"

// BindHint will add hints for originStmt according to hintedStmt' hints.
func BindHint(originStmt, hintedStmt ast.StmtNode) ast.StmtNode {
	switch x := originStmt.(type) {
	case *ast.SelectStmt:
		return selectBind(x, hintedStmt.(*ast.SelectStmt))
	default:
		return originStmt
	}
}

func selectBind(originalNode, hintedNode *ast.SelectStmt) *ast.SelectStmt {
	if hintedNode.TableHints != nil {
		originalNode.TableHints = hintedNode.TableHints
	}
	if originalNode.From != nil {
		originalNode.From.TableRefs = resultSetNodeBind(originalNode.From.TableRefs, hintedNode.From.TableRefs).(*ast.Join)
	}
	if originalNode.Where != nil {
		originalNode.Where = exprBind(originalNode.Where, hintedNode.Where).(ast.ExprNode)
	}

	if originalNode.Having != nil {
		originalNode.Having = havingBind(originalNode.Having, hintedNode.Having)
	}

	if originalNode.OrderBy != nil {
		originalNode.OrderBy = orderByBind(originalNode.OrderBy, hintedNode.OrderBy)
	}

	if originalNode.Fields != nil {
		for idx := 0; idx < len(originalNode.Fields.Fields); idx++ {
			originalNode.Fields.Fields[idx] = selectFieldBind(originalNode.Fields.Fields[idx], hintedNode.Fields.Fields[idx])
		}
	}
	return originalNode
}

func selectFieldBind(originalNode, hintedNode *ast.SelectField) *ast.SelectField {
	originalNode.Expr = exprBind(originalNode.Expr, hintedNode.Expr)
	return originalNode
}

func orderByBind(originalNode, hintedNode *ast.OrderByClause) *ast.OrderByClause {
	for idx := 0; idx < len(originalNode.Items); idx++ {
		originalNode.Items[idx].Expr = exprBind(originalNode.Items[idx].Expr, hintedNode.Items[idx].Expr)
	}
	return originalNode
}

func havingBind(originalNode, hintedNode *ast.HavingClause) *ast.HavingClause {
	originalNode.Expr = exprBind(originalNode.Expr, hintedNode.Expr)
	return originalNode
}

func exprBind(where ast.ExprNode, hintedWhere ast.ExprNode) ast.ExprNode {
	switch v := where.(type) {
	case *ast.SubqueryExpr:
		if v.Query != nil {
			v.Query = resultSetNodeBind(v.Query, hintedWhere.(*ast.SubqueryExpr).Query)
		}
	case *ast.ExistsSubqueryExpr:
		if v.Sel != nil {
			v.Sel.(*ast.SubqueryExpr).Query = resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedWhere.(*ast.ExistsSubqueryExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	case *ast.PatternInExpr:
		if v.Sel != nil {
			v.Sel.(*ast.SubqueryExpr).Query = resultSetNodeBind(v.Sel.(*ast.SubqueryExpr).Query, hintedWhere.(*ast.PatternInExpr).Sel.(*ast.SubqueryExpr).Query)
		}
	case *ast.BinaryOperationExpr:
		if v.L != nil {
			v.L = exprBind(v.L, hintedWhere.(*ast.BinaryOperationExpr).L)
		}
		if v.R != nil {
			v.R = exprBind(v.R, hintedWhere.(*ast.BinaryOperationExpr).R)
		}
	case *ast.IsNullExpr:
		if v.Expr != nil {
			v.Expr = exprBind(v.Expr, hintedWhere.(*ast.IsNullExpr).Expr)
		}
	case *ast.IsTruthExpr:
		if v.Expr != nil {
			v.Expr = exprBind(v.Expr, hintedWhere.(*ast.IsTruthExpr).Expr)
		}
	case *ast.PatternLikeExpr:
		if v.Pattern != nil {
			v.Pattern = exprBind(v.Pattern, hintedWhere.(*ast.PatternLikeExpr).Pattern)
		}
	case *ast.CompareSubqueryExpr:
		if v.L != nil {
			v.L = exprBind(v.L, hintedWhere.(*ast.CompareSubqueryExpr).L)
		}
		if v.R != nil {
			v.R = exprBind(v.R, hintedWhere.(*ast.CompareSubqueryExpr).R)
		}
	}
	return where
}

func resultSetNodeBind(originalNode, hintedNode ast.ResultSetNode) ast.ResultSetNode {
	switch x := originalNode.(type) {
	case *ast.Join:
		return joinBind(x, hintedNode.(*ast.Join))
	case *ast.TableSource:
		ts, _ := hintedNode.(*ast.TableSource)
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			x.Source = selectBind(v, ts.Source.(*ast.SelectStmt))
		case *ast.UnionStmt:
			x.Source = unionSelectBind(v, hintedNode.(*ast.TableSource).Source.(*ast.UnionStmt))
		case *ast.TableName:
			x.Source = dataSourceBind(v, ts.Source.(*ast.TableName))
		}
		return x
	case *ast.SelectStmt:
		return selectBind(x, hintedNode.(*ast.SelectStmt))
	case *ast.UnionStmt:
		return unionSelectBind(x, hintedNode.(*ast.UnionStmt))
	default:
		return x
	}
}

func dataSourceBind(originalNode, hintedNode *ast.TableName) *ast.TableName {
	originalNode.IndexHints = hintedNode.IndexHints
	return originalNode
}

func joinBind(originalNode, hintedNode *ast.Join) *ast.Join {
	if originalNode.Left != nil {
		originalNode.Left = resultSetNodeBind(originalNode.Left, hintedNode.Left)
	}

	if hintedNode.Right != nil {
		originalNode.Right = resultSetNodeBind(originalNode.Right, hintedNode.Right)
	}

	return originalNode
}

func unionSelectBind(originalNode, hintedNode *ast.UnionStmt) ast.ResultSetNode {
	selects := originalNode.SelectList.Selects
	for i := len(selects) - 1; i >= 0; i-- {
		originalNode.SelectList.Selects[i] = selectBind(selects[i], hintedNode.SelectList.Selects[i])
	}

	return originalNode
}
