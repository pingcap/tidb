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

// inferType infers result type for ast.ExprNode.
func inferType(node ast.Node) error {
	var inferrer typeInferrer
	node.Accept(&inferrer)
	return inferrer.err
}

// preEvaluate evaluates preEvaluable expression and rewrites constant expression to value expression.
func preEvaluate(ctx context.Context, node ast.Node) error {
	pe := preEvaluator{ctx: ctx}
	node.Accept(&pe)
	return pe.err
}

type typeInferrer struct {
	err error
}

func (v *typeInferrer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

func (v *typeInferrer) Leave(in ast.Node) (out ast.Node, ok bool) {
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

type preEvaluator struct {
	ctx context.Context
	err error
}

func (r *preEvaluator) Enter(in ast.Node) (ast.Node, bool) {
	return in, false
}

func (r *preEvaluator) Leave(in ast.Node) (ast.Node, bool) {
	if expr, ok := in.(ast.ExprNode); ok {
		if _, ok = expr.(*ast.ValueExpr); ok {
			return in, true
		} else if ast.IsPreEvaluable(expr) {
			val, err := evaluator.Eval(r.ctx, expr)
			if err != nil {
				r.err = err
				return in, false
			}
			if expr.GetFlag() == ast.FlagConstant {
				// The expression is constant, rewrite the expression to value expression.
				valExpr := &ast.ValueExpr{}
				valExpr.SetText(expr.Text())
				valExpr.SetType(expr.GetType())
				valExpr.SetValue(val)
				return valExpr, true
			}
			expr.SetValue(val)
		}
	}
	return in, true
}
