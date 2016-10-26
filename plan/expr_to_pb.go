// Copyright 2016 PingCAP, Inc.
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

package plan

import (
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func expressionsToPB(exprs []expression.Expression, client kv.Client) (pbExpr *tipb.Expr, remained []expression.Expression) {
	for _, expr := range exprs {
		v := exprToPB(client, expr)
		if v == nil {
			remained = append(remained, expr)
			continue
		}
		if pbExpr == nil {
			pbExpr = v
		} else {
			// merge multiple converted pb expression into an AND expression.
			pbExpr = &tipb.Expr{
				Tp:       tipb.ExprType_And,
				Children: []*tipb.Expr{pbExpr, v}}
		}
	}
	return
}

func exprToPB(client kv.Client, expr expression.Expression) *tipb.Expr {
	switch x := expr.(type) {
	case *expression.Constant:
		return datumToPBExpr(client, x.Value)
	case *expression.Column:
		return columnToPBExpr(client, x)
	case *expression.ScalarFunction:
		return scalarFuncToPBExpr(client, x)
	}
	return nil
}

func datumToPBExpr(client kv.Client, d types.Datum) *tipb.Expr {
	var tp tipb.ExprType
	var val []byte
	switch d.Kind() {
	case types.KindNull:
		tp = tipb.ExprType_Null
	case types.KindInt64:
		tp = tipb.ExprType_Int64
		val = codec.EncodeInt(nil, d.GetInt64())
	case types.KindUint64:
		tp = tipb.ExprType_Uint64
		val = codec.EncodeUint(nil, d.GetUint64())
	case types.KindString:
		tp = tipb.ExprType_String
		val = d.GetBytes()
	case types.KindBytes:
		tp = tipb.ExprType_Bytes
		val = d.GetBytes()
	case types.KindFloat32:
		tp = tipb.ExprType_Float32
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindFloat64:
		tp = tipb.ExprType_Float64
		val = codec.EncodeFloat(nil, d.GetFloat64())
	case types.KindMysqlDuration:
		tp = tipb.ExprType_MysqlDuration
		val = codec.EncodeInt(nil, int64(d.GetMysqlDuration().Duration))
	case types.KindMysqlDecimal:
		tp = tipb.ExprType_MysqlDecimal
		val = codec.EncodeDecimal(nil, d)
	default:
		return nil
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	return &tipb.Expr{Tp: tp, Val: val}
}

func columnToPBExpr(client kv.Client, column *expression.Column) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ColumnRef)) {
		return nil
	}
	switch column.GetType().Tp {
	case mysql.TypeBit, mysql.TypeSet, mysql.TypeEnum, mysql.TypeGeometry, mysql.TypeDecimal:
		return nil
	}

	if column.Correlated {
		return nil
	}

	id := column.ID
	// Zero Column ID is not a column from table, can not support for now.
	if id == 0 || id == -1 {
		return nil
	}

	return &tipb.Expr{
		Tp:  tipb.ExprType_ColumnRef,
		Val: codec.EncodeInt(nil, id)}
}

func scalarFuncToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	switch expr.FuncName.L {
	case ast.LT, ast.LE, ast.EQ, ast.NE, ast.GE, ast.GT,
		ast.NullEQ, ast.In, ast.Like:
		return compareOpsToPBExpr(client, expr)
	case ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.Mod, ast.IntDiv:
		return arithmeticalOpsToPBExpr(client, expr)
	case ast.AndAnd, ast.OrOr, ast.UnaryNot:
		return logicalOpsToPBExpr(client, expr)
	case ast.Case, ast.Coalesce:
		return builtinFuncToPBExpr(client, expr)
	default:
		return nil
	}
}

func compareOpsToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.LT:
		tp = tipb.ExprType_LT
	case ast.LE:
		tp = tipb.ExprType_LE
	case ast.EQ:
		tp = tipb.ExprType_EQ
	case ast.NE:
		tp = tipb.ExprType_NE
	case ast.GE:
		tp = tipb.ExprType_GE
	case ast.GT:
		tp = tipb.ExprType_GT
	case ast.NullEQ:
		tp = tipb.ExprType_NullEQ
	case ast.In:
		return inToPBExpr(client, expr)
	case ast.Like:
		// Only patterns like 'abc', '%abc', 'abc%', '%abc%' can be converted to *tipb.Expr for now.
		escape := expr.Args[2].(*expression.Constant).Value
		if escape.IsNull() || byte(escape.GetInt64()) != '\\' {
			return nil
		}
		pattern, ok := expr.Args[1].(*expression.Constant)
		if !ok || pattern.Value.Kind() != types.KindString {
			return nil
		}
		for i, b := range pattern.Value.GetString() {
			switch b {
			case '\\', '_':
				return nil
			case '%':
				if i != 0 && i != len(pattern.Value.GetString())-1 {
					return nil
				}
			}
		}
		tp = tipb.ExprType_Like
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	expr0 := exprToPB(client, expr.Args[0])
	if expr0 == nil {
		return nil
	}
	expr1 := exprToPB(client, expr.Args[1])
	if expr1 == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tp,
		Children: []*tipb.Expr{expr0, expr1}}
}

func arithmeticalOpsToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.Plus:
		tp = tipb.ExprType_Plus
	case ast.Minus:
		tp = tipb.ExprType_Minus
	case ast.Mul:
		tp = tipb.ExprType_Mul
	case ast.Div:
		tp = tipb.ExprType_Div
	case ast.Mod:
		tp = tipb.ExprType_Mod
	case ast.IntDiv:
		tp = tipb.ExprType_IntDiv
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	expr0 := exprToPB(client, expr.Args[0])
	if expr0 == nil {
		return nil
	}
	expr1 := exprToPB(client, expr.Args[1])
	if expr1 == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tp,
		Children: []*tipb.Expr{expr0, expr1}}
}

func logicalOpsToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.AndAnd:
		tp = tipb.ExprType_And
	case ast.OrOr:
		tp = tipb.ExprType_Or
	case ast.UnaryNot:
		return notToPBExpr(client, expr)
	}
	expr0 := exprToPB(client, expr.Args[0])
	if expr0 == nil {
		return nil
	}
	expr1 := exprToPB(client, expr.Args[1])
	if expr1 == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tp,
		Children: []*tipb.Expr{expr0, expr1}}
}

func inToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_In)) {
		return nil
	}

	pbExpr := exprToPB(client, expr.Args[0])
	if pbExpr == nil {
		return nil
	}
	listExpr := constListToPB(client, expr.Args[1:])
	if listExpr == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tipb.ExprType_In,
		Children: []*tipb.Expr{pbExpr, listExpr}}
}

func notToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_Not)) {
		return nil
	}

	child := exprToPB(client, expr.Args[0])
	if child == nil {
		return nil
	}
	return &tipb.Expr{
		Tp:       tipb.ExprType_Not,
		Children: []*tipb.Expr{child}}
}

func constListToPB(client kv.Client, list []expression.Expression) *tipb.Expr {
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tipb.ExprType_ValueList)) {
		return nil
	}

	// Only list of *expression.Constant can be push down.
	datums := make([]types.Datum, 0, len(list))
	for _, expr := range list {
		v, ok := expr.(*expression.Constant)
		if !ok {
			return nil
		}
		d := datumToPBExpr(client, v.Value)
		if d == nil {
			return nil
		}
		datums = append(datums, v.Value)
	}
	return datumsToValueList(datums)
}

func datumsToValueList(datums []types.Datum) *tipb.Expr {
	// Don't push value list that has different datum kind.
	prevKind := types.KindNull
	for _, d := range datums {
		if prevKind == types.KindNull {
			prevKind = d.Kind()
		}
		if !d.IsNull() && d.Kind() != prevKind {
			return nil
		}
	}
	err := types.SortDatums(datums)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	val, err := codec.EncodeValue(nil, datums...)
	if err != nil {
		log.Error(err.Error())
		return nil
	}
	return &tipb.Expr{Tp: tipb.ExprType_ValueList, Val: val}
}

func groupByItemToPB(client kv.Client, expr expression.Expression) *tipb.ByItem {
	e := exprToPB(client, expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e}
}

func sortByItemToPB(client kv.Client, expr expression.Expression, desc bool) *tipb.ByItem {
	e := exprToPB(client, expr)
	if e == nil {
		return nil
	}
	return &tipb.ByItem{Expr: e, Desc: desc}
}

func aggFuncToPBExpr(client kv.Client, aggFunc expression.AggregationFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch aggFunc.GetName() {
	case ast.AggFuncCount:
		tp = tipb.ExprType_Count
	case ast.AggFuncFirstRow:
		tp = tipb.ExprType_First
	case ast.AggFuncGroupConcat:
		tp = tipb.ExprType_GroupConcat
	case ast.AggFuncMax:
		tp = tipb.ExprType_Max
	case ast.AggFuncMin:
		tp = tipb.ExprType_Min
	case ast.AggFuncSum:
		tp = tipb.ExprType_Sum
	case ast.AggFuncAvg:
		tp = tipb.ExprType_Avg
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}

	children := make([]*tipb.Expr, 0, len(aggFunc.GetArgs()))
	for _, arg := range aggFunc.GetArgs() {
		pbArg := exprToPB(client, arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp, Children: children}
}

func builtinFuncToPBExpr(client kv.Client, expr *expression.ScalarFunction) *tipb.Expr {
	var tp tipb.ExprType
	switch expr.FuncName.L {
	case ast.Case:
		tp = tipb.ExprType_Case
	case ast.Coalesce:
		tp = tipb.ExprType_Coalesce
	}
	if !client.SupportRequestType(kv.ReqTypeSelect, int64(tp)) {
		return nil
	}
	children := make([]*tipb.Expr, 0, len(expr.Args))
	for _, arg := range expr.Args {
		pbArg := exprToPB(client, arg)
		if pbArg == nil {
			return nil
		}
		children = append(children, pbArg)
	}
	return &tipb.Expr{Tp: tp, Children: children}
}
