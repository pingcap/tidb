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
	mysql "github.com/pingcap/tidb/mysqldef"
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
func Eval(v Expression, ctx context.Context, env map[interface{}]interface{}) (y interface{}) {
	var err error
	y, err = v.Eval(ctx, env)
	if err != nil {
		panic(err) // panic ok here
	}
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
	for _, e := range c.Args {
		_, err := e.Accept(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	f, ok := builtin.Funcs[strings.ToLower(c.F)]
	if !ok {
		return nil, errors.Errorf("unknown function %s", c.F)
	}
	if f.IsAggregate {
		v.exprs = append(v.exprs, c)
	}
	return c, nil
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

func extractTimeNum(unit string, t mysql.Time) (int64, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return int64(t.Nanosecond() / 1000), nil
	case "SECOND":
		return int64(t.Second()), nil
	case "MINUTE":
		return int64(t.Minute()), nil
	case "HOUR":
		return int64(t.Hour()), nil
	case "DAY":
		return int64(t.Day()), nil
	case "WEEK":
		_, week := t.ISOWeek()
		return int64(week), nil
	case "MONTH":
		return int64(t.Month()), nil
	case "QUARTER":
		m := int64(t.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return int64(t.Year()), nil
	case "SECOND_MICROSECOND":
		return int64(t.Second())*1000000 + int64(t.Nanosecond())/1000, nil
	case "MINUTE_MICROSECOND":
		_, m, s := t.Clock()
		return int64(m)*100000000 + int64(s)*1000000 + int64(t.Nanosecond())/1000, nil
	case "MINUTE_SECOND":
		_, m, s := t.Clock()
		return int64(m*100 + s), nil
	case "HOUR_MICROSECOND":
		h, m, s := t.Clock()
		return int64(h)*10000000000 + int64(m)*100000000 + int64(s)*1000000 + int64(t.Nanosecond())/1000, nil
	case "HOUR_SECOND":
		h, m, s := t.Clock()
		return int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "HOUR_MINUTE":
		h, m, _ := t.Clock()
		return int64(h)*100 + int64(m), nil
	case "DAY_MICROSECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d*1000000+h*10000+m*100+s)*1000000 + int64(t.Nanosecond())/1000, nil
	case "DAY_SECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return int64(d)*1000000 + int64(h)*10000 + int64(m)*100 + int64(s), nil
	case "DAY_MINUTE":
		h, m, _ := t.Clock()
		d := t.Day()
		return int64(d)*10000 + int64(h)*100 + int64(m), nil
	case "DAY_HOUR":
		h, _, _ := t.Clock()
		d := t.Day()
		return int64(d)*100 + int64(h), nil
	case "YEAR_MONTH":
		y, m, _ := t.Date()
		return int64(y)*100 + int64(m), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}

func extractSingleTimeValue(unit string, format string) (int64, int64, int64, time.Duration, error) {
	iv, err := strconv.ParseInt(format, 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	v := time.Duration(iv)
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return 0, 0, 0, v * time.Microsecond, nil
	case "SECOND":
		return 0, 0, 0, v * time.Second, nil
	case "MINUTE":
		return 0, 0, 0, v * time.Minute, nil
	case "HOUR":
		return 0, 0, 0, v * time.Hour, nil
	case "DAY":
		return 0, 0, 1, 0, nil
	case "WEEK":
		return 0, 0, 7, 0, nil
	case "MONTH":
		return 0, 1, 0, 0, nil
	case "QUARTER":
		return 0, 3, 0, 0, nil
	case "YEAR":
		return 1, 0, 0, 0, nil
	}

	return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
}

// Format is `SS.FFFFFF`.
func extractSecondMicrosecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ".")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	microsecond, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, time.Duration(seconds)*time.Second + time.Duration(microsecond)*time.Microsecond, nil
}

// Format is `MM:SS.FFFFFF`.
func extractMinuteMicrosecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractSecondMicrosecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}

	return 0, 0, 0, time.Duration(minutes)*time.Minute + value, nil
}

// Format is `MM:SS`.
func extractMinuteSecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second, nil
}

// Format is `HH:MM:SS.FFFFFF`.
func extractHourMicrosecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 3 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractSecondMicrosecond(fields[2])
	if err != nil {
		return 0, 0, 0, 0, errors.Trace(err)
	}

	return 0, 0, 0, time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute + value, nil
}

// Format is `HH:MM:SS`.
func extractHourSecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 3 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	seconds, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute + time.Duration(seconds)*time.Second, nil
}

// Format is `HH:MM`.
func extractHourMinute(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, ":")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	minutes, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, 0, time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute, nil
}

// Format is `DD HH:MM:SS.FFFFFF`.
func extractDayMicrosecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourMicrosecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH:MM:SS`.
func extractDaySecond(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourSecond(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH:MM`.
func extractDayMinute(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	_, _, _, value, err := extractHourMinute(fields[1])
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, value, nil
}

// Format is `DD HH`.
func extractDayHour(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, time.Duration(hours) * time.Hour, nil
}

// Format is `YYYY-MM`.
func extractYearMonth(format string) (int64, int64, int64, time.Duration, error) {
	fields := strings.Split(format, " ")
	if len(fields) != 2 {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	days, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	hours, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errors.Errorf("invalid time format - %s", format)
	}

	return 0, 0, days, time.Duration(hours) * time.Hour, nil
}

func extractTimeValue(unit string, format string) (int64, int64, int64, time.Duration, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		return extractSingleTimeValue(unit, format)
	case "SECOND_MICROSECOND":
		return extractSecondMicrosecond(format)
	case "MINUTE_MICROSECOND":
		return extractMinuteMicrosecond(format)
	case "MINUTE_SECOND":
		return extractMinuteSecond(format)
	case "HOUR_MICROSECOND":
		return extractHourMicrosecond(format)
	case "HOUR_SECOND":
		return extractHourSecond(format)
	case "HOUR_MINUTE":
		return extractHourMinute(format)
	case "DAY_MICROSECOND":
		return extractDayMicrosecond(format)
	case "DAY_SECOND":
		return extractDaySecond(format)
	case "DAY_MINUTE":
		return extractDayMinute(format)
	case "DAY_HOUR":
		return extractDayHour(format)
	case "YEAR_MONTH":
		return extractYearMonth(format)
	default:
		return 0, 0, 0, 0, errors.Errorf("invalid singel timeunit - %s", unit)
	}
}
