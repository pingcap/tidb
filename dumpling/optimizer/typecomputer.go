// Copyright 2015 PingCAP, Inc.
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

package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/optimizer/evaluator"
)

func computeType(node ast.Node) error {
	var computer typeComputer
	node.Accept(&computer)
	return computer.err
}

func rewriteStatic(ctx context.Context, node ast.Node) error {
	rewriter := staticRewriter{ctx: ctx}
	node.Accept(&rewriter)
	return rewriter.err
}

// typeComputer is an ast Visitor that
// computes result type for ast.ExprNode.
type typeComputer struct {
	err error
}

func (v *typeComputer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

func (v *typeComputer) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.ColumnNameExpr:
		x.SetType(&x.Refer.Column.FieldType)
	case *ast.FuncCastExpr:
		x.SetType(x.Tp)
	case *ast.SelectStmt:
		rf := x.GetResultFields()
		for _, val := range rf {
			if val.Column.ID == 0 && val.Expr.GetType() != nil {
				val.Column.FieldType = *(val.Expr.GetType())
			}
		}
	}
	return in, true
}

// staticRewriter rewrites static expression to value expression.
type staticRewriter struct {
	ctx context.Context
	err error
}

func (r *staticRewriter) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (r *staticRewriter) Leave(in ast.Node) (ast.Node, bool) {
	if expr, ok := in.(ast.ExprNode); ok {
		if _, ok = expr.(*ast.ValueExpr); ok {
			return in, true
		} else if expr.IsStatic() {
			val, err := evaluator.Eval(r.ctx, expr)
			if err != nil {
				r.err = err
				return in, false
			}
			valExpr := &ast.ValueExpr{}
			valExpr.SetText(expr.Text())
			valExpr.SetType(expr.GetType())
			valExpr.SetValue(val)
			return valExpr, true
		}
	}
	return in, true
}
