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
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/optimizer/evaluator"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// InferType infers result type for ast.ExprNode.
func InferType(node ast.Node) error {
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
		v.selectStmt(x)
	case *ast.ParamMarkerExpr:
		x.SetType(types.DefaultTypeForValue(x.GetValue()))
	case *ast.BinaryOperationExpr:
		v.binaryOperation(x)
	case *ast.UnaryOperationExpr:
		v.unaryOperation(x)
	case *ast.BetweenExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.CompareSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.ExistsSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.PatternInExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.PatternLikeExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.PatternRegexpExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.IsNullExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.IsTruthExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
	case *ast.ParenthesesExpr:
		x.SetType(x.Expr.GetType())
	case *ast.AggregateFuncExpr:
		v.aggregateFunc(x)
		// TODO: handle all expression types.
	}
	return in, true
}

func (v *typeInferrer) selectStmt(x *ast.SelectStmt) {
	rf := x.GetResultFields()
	for _, val := range rf {
		// column ID is 0 means it is not a real column from table, but a temporary column,
		// so its type is not pre-defined, we need to set it.
		if val.Column.ID == 0 && val.Expr.GetType() != nil {
			val.Column.FieldType = *(val.Expr.GetType())
		}
	}
}

func (v *typeInferrer) aggregateFunc(x *ast.AggregateFuncExpr) {
	name := strings.ToLower(x.F)
	switch name {
	case "count":
		ft := types.NewFieldType(mysql.TypeLonglong)
		ft.Flen = 21
		x.SetType(ft)
	}
}

func (v *typeInferrer) binaryOperation(x *ast.BinaryOperationExpr) {
	switch x.Op {
	case opcode.AndAnd, opcode.OrOr, opcode.LogicXor:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
	case opcode.LT, opcode.LE, opcode.GE, opcode.GT, opcode.EQ, opcode.NE, opcode.NullEQ:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
	case opcode.RightShift, opcode.LeftShift, opcode.And, opcode.Or, opcode.Xor:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
		x.Type.Flag |= mysql.UnsignedFlag
	case opcode.IntDiv:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
	case opcode.Plus, opcode.Minus, opcode.Mul, opcode.Mod:
		if x.L.GetType() != nil && x.R.GetType() != nil {
			xTp := mergeArithType(x.L.GetType().Tp, x.R.GetType().Tp)
			x.Type = types.NewFieldType(xTp)
			leftUnsigned := x.L.GetType().Flag & mysql.UnsignedFlag
			rightUnsigned := x.R.GetType().Flag & mysql.UnsignedFlag
			// If both operands are unsigned, result is unsigned.
			x.Type.Flag |= (leftUnsigned & rightUnsigned)
		}
	case opcode.Div:
		if x.L.GetType() != nil && x.R.GetType() != nil {
			xTp := mergeArithType(x.L.GetType().Tp, x.R.GetType().Tp)
			if xTp == mysql.TypeLonglong {
				xTp = mysql.TypeDecimal
			}
			x.Type = types.NewFieldType(xTp)
		}
	}
}

func mergeArithType(a, b byte) byte {
	switch a {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat:
		return mysql.TypeDouble
	}
	switch b {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat:
		return mysql.TypeDouble
	}
	if a == mysql.TypeNewDecimal || b == mysql.TypeNewDecimal {
		return mysql.TypeNewDecimal
	}
	return mysql.TypeLonglong
}

func (v *typeInferrer) unaryOperation(x *ast.UnaryOperationExpr) {
	switch x.Op {
	case opcode.Not:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
	case opcode.BitNeg:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
		x.Type.Flag |= mysql.UnsignedFlag
	case opcode.Plus:
		x.Type = x.V.GetType()
	case opcode.Minus:
		x.Type = types.NewFieldType(mysql.TypeLonglong)
		if x.V.GetType() != nil {
			switch x.V.GetType().Tp {
			case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat:
				x.Type.Tp = mysql.TypeDouble
			case mysql.TypeNewDecimal:
				x.Type.Tp = mysql.TypeNewDecimal
			}
		}
	}
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
			if ast.IsConstant(expr) {
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
