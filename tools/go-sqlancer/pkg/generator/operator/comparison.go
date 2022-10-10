// Copyright 2022 PingCAP, Inc.
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

package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	// NULL-safe equal. This operator performs an equality comparison like the = operator,
	// but returns 1 rather than NULL if both operands are NULL, and 0 rather than NULL if one operand is NULL.
	nullEq = types.NewOp(opcode.NullEQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull && b.Kind() == tidb_types.KindNull {
			e.SetValue(1)
			return e, nil
		}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetValue(0)
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) == 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.NullEQ))

	// Tests whether a value is NULL.
	isNull = types.NewOp(opcode.IsNull, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 1 {
			panic("error param numbers")
		}
		e := parser_driver.ValueExpr{}
		e.SetValue(v[0].Kind() == tidb_types.KindNull)
		return e, nil
	}, func(u ...uint64) (uint64, bool, error) {
		return u[0], false, nil
	}, func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{}, fmt.Errorf("cannot find valid param for type(%d) returned", ret)
		}
		arg := argList[rand.Intn(len(argList))]
		expr, value, err := cb(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		value, err = op.Eval(value)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.IsNullExpr{
			Expr: expr,
			Not:  false,
		}
		return node, value, nil
	})

	// IN (value,...)
	in = types.NewOp(opcode.In, 2, 5, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) < 2 {
			panic("error param numbers")
		}
		expr := v[0]
		e := parser_driver.ValueExpr{}
		if expr.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		hasNull := false
		for _, b := range v[1:] {
			if b.Kind() == tidb_types.KindNull {
				hasNull = true
				continue
			}
			if util.CompareValueExpr(expr, b) == 0 {
				e.SetValue(1)
				return e, nil
			}
		}
		if hasNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(0)
		return e, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		exprType := argTyps[0]
		for i := 1; i < len(argTyps); i++ {
			if exprType != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		return types.TypeIntArg | types.TypeFloatArg, false, nil
	}, func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		arg, err := op.GetArgTable().RandByFilter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		expr, value, err := cb(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		var list []ast.ExprNode
		v := []parser_driver.ValueExpr{value}
		for i := 1; i < len(arg)-1; i++ {
			expr, value, err := cb(arg[i])
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			list = append(list, expr)
			v = append(v, value)
		}
		value, err = op.Eval(v...)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.PatternInExpr{
			Expr: expr,
			List: list,
		}
		return node, value, nil
	})

	// expr BETWEEN min AND max
	// refs: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_between
	between = types.NewFn("BETWEEN", 3, 3, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 3 {
			panic("error param numbers")
		}
		expr, min, max := v[0], v[1], v[2]
		//  This is equivalent to the expression (min <= expr AND expr <= max) if all the arguments are of the same type
		leftExpr := parser_driver.ValueExpr{}
		if min.Kind() == tidb_types.KindNull || expr.Kind() == tidb_types.KindNull {
			leftExpr.SetNull()
		} else {
			leftExpr.SetValue(util.CompareValueExpr(min, expr) <= 0)
		}
		rightExpr := parser_driver.ValueExpr{}
		if expr.Kind() == tidb_types.KindNull || max.Kind() == tidb_types.KindNull {
			rightExpr.SetNull()
		} else {
			rightExpr.SetValue(util.CompareValueExpr(expr, max) <= 0)
		}
		return LogicAnd.Eval(leftExpr, rightExpr)
	}, func(argTyps ...uint64) (uint64, bool, error) {
		// for the sake of simplicity, we require expr, min and max are same type
		exprType := argTyps[0]
		for i := 1; i < len(argTyps); i++ {
			if exprType != argTyps[i] {
				return 0, false, errors.New("invalid type")
			}
		}
		return types.TypeIntArg | types.TypeFloatArg, false, nil
	}, func(genExpr types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) < 4 {
			return nil, parser_driver.ValueExpr{}, fmt.Errorf("cannot find valid param for type(%d) returned", ret)
		}
		arg := argList[rand.Intn(len(argList))]
		expr, exprValue, err := genExpr(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		min, minValue, err := genExpr(arg[1])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		max, maxValue, err := genExpr(arg[2])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.BetweenExpr{
			Expr:  expr,
			Left:  min,
			Right: max,
			Not:   false,
		}
		result, err := op.Eval(exprValue, minValue, maxValue)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		return node, result, nil
	})

	// STRCMP(expr1,expr2)
	// STRCMP() returns 0 if the strings are the same, -1 if the first argument is smaller than the second according to the current sort order, and 1 otherwise.
	strCmp = types.NewFn("STRCMP", 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		expr1, expr2 := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if expr1.Kind() == tidb_types.KindNull || expr2.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		exprs := []parser_driver.ValueExpr{expr1, expr2}
		for i, expr := range exprs {
			var strValue string
			switch expr.Kind() {
			case tidb_types.KindInt64:
				strValue = fmt.Sprintf("%d", expr.GetInt64())
				exprs[i] = parser_driver.ValueExpr{}
				exprs[i].SetString(strValue, "")
			case tidb_types.KindUint64:
				strValue = fmt.Sprintf("%d", expr.GetUint64())
				exprs[i] = parser_driver.ValueExpr{}
				exprs[i].SetString(strValue, "")
			case tidb_types.KindMysqlTime:
				strValue = expr.GetMysqlTime().String()
				exprs[i] = parser_driver.ValueExpr{}
				exprs[i].SetString(strValue, "")
			case tidb_types.KindNull:
			case tidb_types.KindFloat32:
				strValue = fmt.Sprintf("%f", expr.GetFloat32())
				exprs[i] = parser_driver.ValueExpr{}
				exprs[i].SetString(strValue, "")
			case tidb_types.KindFloat64:
				strValue = fmt.Sprintf("%f", expr.GetFloat64())
				exprs[i] = parser_driver.ValueExpr{}
				exprs[i].SetString(strValue, "")
			case tidb_types.KindString:
			case tidb_types.KindBytes:
			case tidb_types.KindBinaryLiteral:
			case tidb_types.KindMysqlDecimal:
			case tidb_types.KindMysqlDuration:
			case tidb_types.KindMysqlEnum:
			case tidb_types.KindMysqlBit:
			case tidb_types.KindMysqlSet:
			case tidb_types.KindInterface:
			case tidb_types.KindMinNotNull:
			case tidb_types.KindMaxValue:
			case tidb_types.KindRaw:
				panic("KindRaw!")
			case tidb_types.KindMysqlJSON:
				panic("KindMysqlJSON!")
			default:
				panic("unreachable!")
			}
		}
		e.SetValue(util.CompareValueExpr(exprs[0], exprs[1]))
		return e, nil
	}, func(argTyps ...uint64) (uint64, bool, error) {
		// for the sake of simplicity, we require expr, min and max are same type
		expr1Type, expr2Type := argTyps[0], argTyps[1]
		// we only allow string or integer, ignore float and other types
		if expr1Type&^(types.TypeStringArg|types.TypeIntArg) != 0 {
			return 0, false, errors.New("invalid type")
		}
		if expr2Type&^(types.TypeStringArg|types.TypeIntArg) != 0 {
			return 0, false, errors.New("invalid type")
		}
		return types.TypeIntArg | types.TypeFloatArg, false, nil
	}, func(genExpr types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) < 3 {
			return nil, parser_driver.ValueExpr{}, fmt.Errorf("cannot find valid param for type(%d) returned", ret)
		}
		arg := argList[rand.Intn(len(argList))]
		expr1, expr1Value, err := genExpr(arg[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		expr2, expr2Value, err := genExpr(arg[1])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.FuncCallExpr{
			FnName: model.NewCIStr(op.GetName()),
			Args:   []ast.ExprNode{expr1, expr2},
		}
		result, err := op.Eval(expr1Value, expr2Value)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		return node, result, nil
	})
)
