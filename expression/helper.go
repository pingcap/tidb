// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

const (
	// ExprEvalDefaultName is the key saving default column name for Default expression.
	ExprEvalDefaultName = "$defaultName"
	// ExprEvalIdentFunc is the key saving a function to retrieve value for identifier name.
	ExprEvalIdentFunc = "$identFunc"
	// ExprEvalPositionFunc is the key saving a Position expresion.
	ExprEvalPositionFunc = "$positionFunc"
	// ExprEvalValuesFunc is the key saving a function to retrieve value for column name.
	ExprEvalValuesFunc = "$valuesFunc"
	// ExprEvalIdentReferFunc is the key saving a function to retrieve value with identifier reference index.
	ExprEvalIdentReferFunc = "$identReferFunc"
)

var (
	// CurrentTimestamp is the keyword getting default value for datetime and timestamp type.
	CurrentTimestamp = "CURRENT_TIMESTAMP"
	// CurrentTimeExpr is the expression retireving default value for datetime and timestamp type.
	CurrentTimeExpr = &Ident{CIStr: model.NewCIStr(CurrentTimestamp)}
	// ZeroTimestamp shows the zero datetime and timestamp.
	ZeroTimestamp = "0000-00-00 00:00:00"
)

var (
	errDefaultValue = errors.New("invalid default value")
)

// TypeStar is the type for *
type TypeStar string

// Expr removes parenthese expression, e.g, (expr) -> expr.
func Expr(v interface{}) Expression {
	e := v.(Expression)
	for {
		x, ok := e.(*PExpr)
		if !ok {
			return e
		}
		e = x.Expr
	}
}

func cloneExpressionList(list []Expression) []Expression {
	r := make([]Expression, len(list))
	for i, v := range list {
		r[i] = v.Clone()
	}
	return r
}

// FastEval evaluates Value and static +/- Unary expression and returns its value.
func FastEval(v interface{}) interface{} {
	v = types.RawData(v)
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

// Eval is a helper function evaluates expression v and do a panic if evaluating error.
func Eval(v Expression, ctx context.Context, env map[interface{}]interface{}) (y interface{}) {
	var err error
	y, err = v.Eval(ctx, env)
	if err != nil {
		panic(err) // panic ok here
	}
	y = types.RawData(y)
	return
}

// MentionedAggregateFuncs returns a list of the Call expression which is aggregate function.
func MentionedAggregateFuncs(e Expression) ([]Expression, error) {
	mafv := newMentionedAggregateFuncsVisitor()
	_, err := e.Accept(mafv)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return mafv.exprs, nil
}

// ContainAggregateFunc checks whether expression e contains an aggregate function, like count(*) or other.
func ContainAggregateFunc(e Expression) bool {
	m, _ := MentionedAggregateFuncs(e)
	return len(m) > 0
}

type mentionedAggregateFuncsVisitor struct {
	BaseVisitor
	exprs []Expression
}

func newMentionedAggregateFuncsVisitor() *mentionedAggregateFuncsVisitor {
	v := &mentionedAggregateFuncsVisitor{}
	v.BaseVisitor.V = v
	return v
}

func (v *mentionedAggregateFuncsVisitor) VisitCall(c *Call) (Expression, error) {
	isAggregate := IsAggregateFunc(c.F)

	if isAggregate {
		v.exprs = append(v.exprs, c)
	}

	n := len(v.exprs)
	for _, e := range c.Args {
		_, err := e.Accept(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if isAggregate && len(v.exprs) != n {
		// aggregate function can't use aggregate function as the arg.
		// here means we have aggregate function in arg.
		return nil, errors.Errorf("Invalid use of group function")
	}

	return c, nil
}

// IsAggregateFunc checks whether name is an aggregate function or not.
func IsAggregateFunc(name string) bool {
	// TODO: use switch defined aggregate name "sum", "count", etc... directly.
	// Maybe we can remove builtin IsAggregate field later.
	f, ok := builtin.Funcs[strings.ToLower(name)]
	if !ok {
		return false
	}
	return f.IsAggregate
}

// MentionedColumns returns a list of names for Ident expression.
func MentionedColumns(e Expression) []string {
	var names []string
	mcv := newMentionedColumnsVisitor()
	e.Accept(mcv)
	for k := range mcv.columns {
		names = append(names, k)
	}
	return names
}

type mentionedColumnsVisitor struct {
	BaseVisitor
	columns map[string]struct{}
}

func newMentionedColumnsVisitor() *mentionedColumnsVisitor {
	v := &mentionedColumnsVisitor{columns: map[string]struct{}{}}
	v.BaseVisitor.V = v
	return v
}

func (v *mentionedColumnsVisitor) VisitIdent(i *Ident) (Expression, error) {
	v.columns[i.L] = struct{}{}
	return i, nil
}

func staticExpr(e Expression) (Expression, error) {
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

// IsCurrentTimeExpr returns whether e is CurrentTimeExpr.
func IsCurrentTimeExpr(e Expression) bool {
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
		x.Val = types.RawData(x.Val)
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
func EvalBoolExpr(ctx context.Context, expr Expression, m map[interface{}]interface{}) (bool, error) {
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

// CheckOneColumn checks whether expression e has only one column for the evaluation result.
// Now most of the expressions have one column except Row expression.
func CheckOneColumn(ctx context.Context, e Expression) error {
	n, err := columnCount(ctx, e)
	if err != nil {
		return errors.Trace(err)
	}

	if n != 1 {
		return errors.Errorf("Operand should contain 1 column(s)")
	}

	return nil
}

// CheckAllOneColumns checks all expressions have one column.
func CheckAllOneColumns(ctx context.Context, args ...Expression) error {
	for _, e := range args {
		if err := CheckOneColumn(ctx, e); err != nil {
			return err
		}
	}

	return nil
}

func columnCount(ctx context.Context, e Expression) (int, error) {
	switch x := e.(type) {
	case *Row:
		n := len(x.Values)
		if n <= 1 {
			return 0, errors.Errorf("Operand should contain >= 2 columns for Row")
		}
		return n, nil
	case SubQuery:
		return x.ColumnCount(ctx)
	default:
		return 1, nil
	}
}

func hasSameColumnCount(ctx context.Context, e Expression, args ...Expression) error {
	l, err := columnCount(ctx, e)
	if err != nil {
		return errors.Trace(err)
	}
	var n int
	for _, arg := range args {
		n, err = columnCount(ctx, arg)
		if err != nil {
			return errors.Trace(err)
		}

		if n != l {
			return errors.Errorf("Operand should contain %d column(s)", l)
		}
	}

	return nil
}
