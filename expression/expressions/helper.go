// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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

// Copyright 2014 The TiDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found PatternIn the LICENSE file.

package expressions

import (
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

const (
	// ExprEvalFn is the key saving Call expression.
	ExprEvalFn = "$fn"
	// ExprEvalArgCtx is the key saving Context for a Call expression.
	ExprEvalArgCtx = "$ctx"
	// ExprAggDone is the key indicating that aggregate function is done.
	ExprAggDone = "$aggDone"
	// ExprEvalArgAggEmpty is the key to evaluate the aggregate function for empty table.
	ExprEvalArgAggEmpty = "$agg0"
	// ExprEvalDefaultName is the key saving default column name for Default expression.
	ExprEvalDefaultName = "$defaultName"
	// ExprEvalIdentFunc is the key saving a function to retrieve value for identifier name.
	ExprEvalIdentFunc = "$identFunc"
	// ExprEvalPositionFunc is the key saving a Position expresion.
	ExprEvalPositionFunc = "$positionFunc"
	// ExprAggDistinct is the key saving a distinct aggregate.
	ExprAggDistinct = "$aggDistinct"
	// ExprEvalValuesFunc is the key saving a function to retrieve value for column name.
	ExprEvalValuesFunc = "$valuesFunc"
)

var (
	// CurrentTimestamp is the keyword getting default value for datetime and timestamp type.
	CurrentTimestamp = "CURRENT_TIMESTAMP"
	// CurrentTimeExpr is the expression retireving default value for datetime and timestamp type.
	CurrentTimeExpr = &Ident{model.NewCIStr(CurrentTimestamp)}
	// ZeroTimestamp shows the zero datetime and timestamp.
	ZeroTimestamp = "0000-00-00 00:00:00"
)

var (
	errDivByZero    = errors.New("division by zero")
	errDefaultValue = errors.New("invalid default value")
)

// TypeStar is the type for *
type TypeStar string

// Expr removes parenthese expression, e.g, (expr) -> expr.
func Expr(v interface{}) expression.Expression {
	e := v.(expression.Expression)
	for {
		x, ok := e.(*PExpr)
		if !ok {
			return e
		}
		e = x.Expr
	}
}

func cloneExpressionList(list []expression.Expression) ([]expression.Expression, error) {
	r := make([]expression.Expression, len(list))
	var err error
	for i, v := range list {
		if r[i], err = v.Clone(); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// FastEval evaluates Value and static +/- Unary expression and returns its value.
func FastEval(v interface{}) interface{} {
	switch x := v.(type) {
	case Value:
		return x.Val
	case int64, uint64:
		return v
	case *UnaryOperation:
		if x.Op != opcode.Plus && x.Op != opcode.Minus {
			return nil
		}
		if !x.IsStatic() {
			return nil
		}
		m := map[interface{}]interface{}{}
		return Eval(x, nil, m)
	default:
		return nil
	}
}

// IsQualified returns whether name contains ".".
func IsQualified(name string) bool {
	return strings.Contains(name, ".")
}

// Eval is a helper function evaluates expression v and do a panic if evaluating error.
func Eval(v expression.Expression, ctx context.Context, env map[interface{}]interface{}) (y interface{}) {
	var err error
	y, err = v.Eval(ctx, env)
	if err != nil {
		panic(err) // panic ok here
	}
	return
}

// MentionedAggregateFuncs returns a list of the Call expression which is aggregate function.
func MentionedAggregateFuncs(e expression.Expression) []expression.Expression {
	var m []expression.Expression
	mentionedAggregateFuncs(e, &m)
	return m
}

func mentionedAggregateFuncs(e expression.Expression, m *[]expression.Expression) {
	switch x := e.(type) {
	case Value, *Value, *Variable,
		*Default, *Ident, *SubQuery, *Position:
		// nop
	case *BinaryOperation:
		mentionedAggregateFuncs(x.L, m)
		mentionedAggregateFuncs(x.R, m)
	case *Call:
		f, ok := builtin[strings.ToLower(x.F)]
		if !ok {
			log.Errorf("unknown function %s", x.F)
			return
		}

		if f.isAggregate {
			// if f is aggregate function, we don't need check the arguments,
			// because using an aggregate function in the aggregate arg like count(max(c1)) is invalid
			// TODO: check whether argument contains an aggregate function and return error.
			*m = append(*m, e)
			return
		}

		for _, e := range x.Args {
			mentionedAggregateFuncs(e, m)
		}
	case *IsNull:
		mentionedAggregateFuncs(x.Expr, m)
	case *PExpr:
		mentionedAggregateFuncs(x.Expr, m)
	case *PatternIn:
		mentionedAggregateFuncs(x.Expr, m)
		for _, e := range x.List {
			mentionedAggregateFuncs(e, m)
		}
	case *PatternLike:
		mentionedAggregateFuncs(x.Expr, m)
		mentionedAggregateFuncs(x.Pattern, m)
	case *UnaryOperation:
		mentionedAggregateFuncs(x.V, m)
	case *ParamMarker:
		if x.Expr != nil {
			mentionedAggregateFuncs(x.Expr, m)
		}
	case *FunctionCast:
		if x.Expr != nil {
			mentionedAggregateFuncs(x.Expr, m)
		}
	case *FunctionConvert:
		if x.Expr != nil {
			mentionedAggregateFuncs(x.Expr, m)
		}
	case *FunctionSubstring:
		if x.StrExpr != nil {
			mentionedAggregateFuncs(x.StrExpr, m)
		}
		if x.Pos != nil {
			mentionedAggregateFuncs(x.Pos, m)
		}
		if x.Len != nil {
			mentionedAggregateFuncs(x.Len, m)
		}
	case *FunctionCase:
		if x.Value != nil {
			mentionedAggregateFuncs(x.Value, m)
		}
		for _, w := range x.WhenClauses {
			mentionedAggregateFuncs(w, m)
		}
		if x.ElseClause != nil {
			mentionedAggregateFuncs(x.ElseClause, m)
		}
	case *WhenClause:
		mentionedAggregateFuncs(x.Expr, m)
		mentionedAggregateFuncs(x.Result, m)
	case *IsTruth:
		mentionedAggregateFuncs(x.Expr, m)
	case *Between:
		mentionedAggregateFuncs(x.Expr, m)
		mentionedAggregateFuncs(x.Left, m)
		mentionedAggregateFuncs(x.Right, m)
	default:
		log.Errorf("Unknown Expression: %T", e)
	}
}

// ContainAggregateFunc checks whether expression e contains an aggregate function, like count(*) or other.
func ContainAggregateFunc(e expression.Expression) bool {
	m := MentionedAggregateFuncs(e)
	return len(m) > 0
}

func mentionedColumns(e expression.Expression, m map[string]bool, names *[]string) {
	switch x := e.(type) {
	case Value, *Value, *Variable,
		*Default, *SubQuery, *Position:
		// nop
	case *BinaryOperation:
		mentionedColumns(x.L, m, names)
		mentionedColumns(x.R, m, names)
	case *Call:
		for _, e := range x.Args {
			mentionedColumns(e, m, names)
		}
	case *Ident:
		name := x.L
		if !m[name] {
			m[name] = true
			*names = append(*names, name)
		}
	case *IsNull:
		mentionedColumns(x.Expr, m, names)
	case *PExpr:
		mentionedColumns(x.Expr, m, names)
	case *PatternIn:
		mentionedColumns(x.Expr, m, names)
		for _, e := range x.List {
			mentionedColumns(e, m, names)
		}
	case *PatternLike:
		mentionedColumns(x.Expr, m, names)
		mentionedColumns(x.Pattern, m, names)
	case *UnaryOperation:
		mentionedColumns(x.V, m, names)
	case *ParamMarker:
		if x.Expr != nil {
			mentionedColumns(x.Expr, m, names)
		}
	case *FunctionCast:
		if x.Expr != nil {
			mentionedColumns(x.Expr, m, names)
		}
	case *FunctionConvert:
		if x.Expr != nil {
			mentionedColumns(x.Expr, m, names)
		}
	case *FunctionSubstring:
		if x.StrExpr != nil {
			mentionedColumns(x.StrExpr, m, names)
		}
		if x.Pos != nil {
			mentionedColumns(x.Pos, m, names)
		}
		if x.Len != nil {
			mentionedColumns(x.Len, m, names)
		}
	case *FunctionCase:
		if x.Value != nil {
			mentionedColumns(x.Value, m, names)
		}
		for _, w := range x.WhenClauses {
			mentionedColumns(w, m, names)
		}
		if x.ElseClause != nil {
			mentionedColumns(x.ElseClause, m, names)
		}
	case *WhenClause:
		mentionedColumns(x.Expr, m, names)
		mentionedColumns(x.Result, m, names)
	case *IsTruth:
		mentionedColumns(x.Expr, m, names)
	case *Between:
		mentionedColumns(x.Expr, m, names)
		mentionedColumns(x.Left, m, names)
		mentionedColumns(x.Right, m, names)
	default:
		log.Errorf("Unknown Expression: %T", e)
	}
}

// MentionedColumns returns a list of names for Ident expression.
func MentionedColumns(e expression.Expression) []string {
	var names []string
	m := make(map[string]bool)
	mentionedColumns(e, m, &names)
	return names
}

func staticExpr(e expression.Expression) (expression.Expression, error) {
	if e.IsStatic() {
		v, err := e.Eval(nil, nil)
		if err != nil {
			return nil, err
		}

		if v == nil {
			return Value{nil}, nil
		}

		return Value{v}, nil
	}

	return e, nil
}

type exprTab struct {
	expr  expression.Expression
	table string
}

// IsCurrentTimeExpr returns whether e is CurrentTimeExpr.
func IsCurrentTimeExpr(e expression.Expression) bool {
	x, ok := e.(*Ident)
	if !ok {
		return false
	}

	return x.Equal(CurrentTimeExpr)
}

func getSystemTimestamp(ctx context.Context) (time.Time, error) {
	value := time.Now()

	if ctx == nil {
		return value, nil
	}

	// check whether use timestamp varibale
	sessionVars := variable.GetSessionVars(ctx)
	if v, ok := sessionVars.Systems["timestamp"]; ok {
		if v != "" {
			timestamp, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return time.Time{}, errors.Trace(err)
			}

			if timestamp <= 0 {
				return value, nil
			}

			return time.Unix(timestamp, 0), nil
		}
	}

	return value, nil
}

// GetTimeValue gets the time value with type tp.
func GetTimeValue(ctx context.Context, v interface{}, tp byte, fsp int) (interface{}, error) {
	return getTimeValue(ctx, v, tp, fsp)
}

func getTimeValue(ctx context.Context, v interface{}, tp byte, fsp int) (interface{}, error) {
	value := mysql.Time{
		Type: tp,
		Fsp:  fsp,
	}

	defaultTime, err := getSystemTimestamp(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch x := v.(type) {
	case string:
		if x == CurrentTimestamp {
			value.Time = defaultTime
		} else if x == ZeroTimestamp {
			value, _ = mysql.ParseTimeFromNum(0, tp, fsp)
		} else {
			value, err = mysql.ParseTime(x, tp, fsp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	case Value:
		switch xval := x.Val.(type) {
		case string:
			value, err = mysql.ParseTime(xval, tp, fsp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case int64:
			value, err = mysql.ParseTimeFromNum(int64(xval), tp, fsp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		case nil:
			return nil, nil
		default:
			return nil, errors.Trace(errDefaultValue)
		}
	case *Ident:
		if x.Equal(CurrentTimeExpr) {
			return CurrentTimestamp, nil
		}

		return nil, errors.Trace(errDefaultValue)
	case *UnaryOperation:
		// support some expression, like `-1`
		m := map[interface{}]interface{}{}
		v := Eval(x, nil, m)
		ft := types.NewFieldType(mysql.TypeLonglong)
		xval, err := types.Convert(v, ft)
		if err != nil {
			return nil, errors.Trace(err)
		}

		value, err = mysql.ParseTimeFromNum(xval.(int64), tp, fsp)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		return nil, nil
	}

	return value, nil
}

// EvalBoolExpr evaluates an expression and convert its return value to bool.
func EvalBoolExpr(ctx context.Context, expr expression.Expression, m map[interface{}]interface{}) (bool, error) {
	val, err := expr.Eval(ctx, m)
	if err != nil {
		return false, err
	}
	if val == nil {
		return false, nil
	}

	x, err := types.ToBool(val)
	if err != nil {
		return false, err
	}

	return x != 0, nil
}
