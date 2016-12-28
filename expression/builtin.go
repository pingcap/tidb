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

package expression

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

type baseBuiltinFunc struct {
	args          []Expression
	argValues     []types.Datum
	deterministic bool
	ctx           context.Context
	self          builtinFunc
}

func newBaseBuiltinFunc(args []Expression, deterministic bool, ctx context.Context) baseBuiltinFunc {
	return baseBuiltinFunc{
		args:          args,
		deterministic: deterministic,
		argValues:     make([]types.Datum, len(args)),
		ctx:           ctx,
	}
}

func (b *baseBuiltinFunc) constantFold(args []types.Datum) (types.Datum, error) {
	oldargs := b.args
	defer func() {
		b.args = oldargs
	}()
	newArgs := make([]Expression, 0, len(args))
	b.argValues = make([]types.Datum, len(args))
	for _, arg := range args {
		newArgs = append(newArgs, &Constant{Value: arg})
	}
	b.args = newArgs
	return b.self.eval(nil)
}

func (b *baseBuiltinFunc) evalArgs(row []types.Datum) (_ []types.Datum, err error) {
	for i, arg := range b.args {
		b.argValues[i], err = arg.Eval(row, b.ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return b.argValues, nil
}

func (b *baseBuiltinFunc) isDeterministic() bool {
	return b.deterministic
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

func (b *baseBuiltinFunc) equal(fun builtinFunc) bool {
	if !b.deterministic || !fun.isDeterministic() {
		return false
	}
	funArgs := fun.getArgs()
	if len(funArgs) != len(b.args) {
		return false
	}
	for i := range b.args {
		if !b.args[i].Equal(funArgs[i], b.ctx) {
			return false
		}
	}
	return true
}

func (b *baseBuiltinFunc) getCtx() context.Context {
	return b.ctx
}

type builtinFunc interface {
	eval([]types.Datum) (types.Datum, error)
	constantFold([]types.Datum) (types.Datum, error)
	getArgs() []Expression
	isDeterministic() bool
	equal(builtinFunc) bool
	getCtx() context.Context
}

// Func is for a builtin function.
type functionClass interface {
	getFunction(args []Expression, ctx context.Context) (builtinFunc, error)
}

// Funcs holds all registered builtin functions.
var Funcs = map[string]functionClass{
	// common functions
	ast.Coalesce: &coalesceFuncClass{baseFuncClass{ast.Coalesce, 1, -1}},
	ast.IsNull:   &isNullFuncClass{baseFuncClass{ast.IsNull, 1, 1}},
	ast.Greatest: &greatestFuncClass{baseFuncClass{ast.Greatest, 2, -1}},

	// math functions
	ast.Abs:     &absFuncClass{baseFuncClass{ast.Abs, 1, 1}},
	ast.Ceil:    &ceilFuncClass{baseFuncClass{ast.Ceil, 1, 1}},
	ast.Ceiling: &ceilFuncClass{baseFuncClass{ast.Ceiling, 1, 1}},
	ast.Ln:      &logFuncClass{baseFuncClass{ast.Ln, 1, 1}},
	ast.Log:     &logFuncClass{baseFuncClass{ast.Log, 1, 2}},
	ast.Log2:    &log2FuncClass{baseFuncClass{ast.Log2, 1, 1}},
	ast.Log10:   &log10FuncClass{baseFuncClass{ast.Log10, 1, 1}},
	ast.Pow:     &powFuncClass{baseFuncClass{ast.Pow, 2, 2}},
	ast.Power:   &powFuncClass{baseFuncClass{ast.Power, 2, 2}},
	ast.Rand:    &randFuncClass{baseFuncClass{ast.Rand, 0, 1}},
	ast.Round:   &roundFuncClass{baseFuncClass{ast.Round, 1, 2}},

	// time functions
	ast.Curdate:          &currentDateFuncClass{baseFuncClass{ast.Curdate, 0, 0}},
	ast.CurrentDate:      &currentDateFuncClass{baseFuncClass{ast.CurrentDate, 0, 0}},
	ast.Curtime:          &currentTimeFuncClass{baseFuncClass{ast.Curtime, 0, 1}},
	ast.CurrentTime:      &currentTimeFuncClass{baseFuncClass{ast.CurrentTime, 0, 1}},
	ast.Date:             &currentDateFuncClass{baseFuncClass{ast.Date, 1, 1}},
	ast.DateArith:        &dateArithFuncClass{baseFuncClass{ast.DateArith, 3, 3}},
	ast.DateFormat:       &dateFormatFuncClass{baseFuncClass{ast.DateFormat, 2, 2}},
	ast.Now:              &nowFuncClass{baseFuncClass{ast.Now, 0, 1}},
	ast.CurrentTimestamp: &nowFuncClass{baseFuncClass{ast.CurrentTimestamp, 0, 1}},
	ast.Day:              &dayOfMonthFuncClass{baseFuncClass{ast.Day, 1, 1}},
	ast.DayName:          &dayNameFuncClass{baseFuncClass{ast.DayName, 1, 1}},
	ast.DayOfMonth:       &dayOfMonthFuncClass{baseFuncClass{ast.DayOfMonth, 1, 1}},
	ast.DayOfWeek:        &dayOfWeekFuncClass{baseFuncClass{ast.DayOfWeek, 1, 1}},
	ast.DayOfYear:        &dayOfYearFuncClass{baseFuncClass{ast.DayOfYear, 1, 1}},
	ast.Extract:          &extractFuncClass{baseFuncClass{ast.Extract, 2, 2}},
	ast.Hour:             &hourFuncClass{baseFuncClass{ast.Hour, 1, 1}},
	ast.MicroSecond:      &microSecondFuncClass{baseFuncClass{ast.MicroSecond, 1, 1}},
	ast.Minute:           &minuteFuncClass{baseFuncClass{ast.Minute, 1, 1}},
	ast.Month:            &monthFuncClass{baseFuncClass{ast.Month, 1, 1}},
	ast.MonthName:        &monthNameFuncClass{baseFuncClass{ast.MonthName, 1, 1}},
	ast.Second:           &secondFuncClass{baseFuncClass{ast.Second, 1, 1}},
	ast.StrToDate:        &strToDateFuncClass{baseFuncClass{ast.StrToDate, 2, 2}},
	ast.Sysdate:          &nowFuncClass{baseFuncClass{ast.Sysdate, 0, 1}},
	ast.Time:             &timeFuncClass{baseFuncClass{ast.Time, 1, 1}},
	ast.UTCDate:          &utcDateFuncClass{baseFuncClass{ast.UTCDate, 0, 0}},
	ast.Week:             &weekFuncClass{baseFuncClass{ast.Week, 1, 2}},
	ast.Weekday:          &weekDayFuncClass{baseFuncClass{ast.Weekday, 1, 1}},
	ast.WeekOfYear:       &weekOfYearFuncClass{baseFuncClass{ast.WeekOfYear, 1, 1}},
	ast.Year:             &yearFuncClass{baseFuncClass{ast.Year, 1, 1}},
	ast.YearWeek:         &yearWeekFuncClass{baseFuncClass{ast.YearWeek, 1, 2}},
	ast.FromUnixTime:     &fromUnixTimeFuncClass{baseFuncClass{ast.FromUnixTime, 1, 2}},
	ast.TimeDiff:         &timeDiffFuncClass{baseFuncClass{ast.TimeDiff, 2, 2}},

	// string functions
	ast.ASCII:          &asciiFuncClass{baseFuncClass{ast.ASCII, 1, 1}},
	ast.Concat:         &concatFuncClass{baseFuncClass{ast.Concat, 1, -1}},
	ast.ConcatWS:       &concatWSFuncClass{baseFuncClass{ast.ConcatWS, 2, -1}},
	ast.Convert:        &convertFuncClass{baseFuncClass{ast.Convert, 2, 2}},
	ast.Lcase:          &lowerFuncClass{baseFuncClass{ast.Lcase, 1, 1}},
	ast.Left:           &leftFuncClass{baseFuncClass{ast.Left, 2, 2}},
	ast.Length:         &lengthFuncClass{baseFuncClass{ast.Length, 1, 1}},
	ast.Locate:         &locateFuncClass{baseFuncClass{ast.Locate, 2, 3}},
	ast.Lower:          &lowerFuncClass{baseFuncClass{ast.Lower, 1, 1}},
	ast.Ltrim:          &lTrimFuncClass{baseFuncClass{ast.Ltrim, 1, 1}},
	ast.Repeat:         &repeatFuncClass{baseFuncClass{ast.Repeat, 2, 2}},
	ast.Replace:        &replaceFuncClass{baseFuncClass{ast.Replace, 3, 3}},
	ast.Reverse:        &reverseFuncClass{baseFuncClass{ast.Reverse, 1, 1}},
	ast.Rtrim:          &rTrimFuncClass{baseFuncClass{ast.Rtrim, 1, 1}},
	ast.Space:          &spaceFuncClass{baseFuncClass{ast.Space, 1, 1}},
	ast.Strcmp:         &strcmpFuncClass{baseFuncClass{ast.Strcmp, 2, 2}},
	ast.Substring:      &substringFuncClass{baseFuncClass{ast.Substring, 2, 3}},
	ast.SubstringIndex: &substringIndexFuncClass{baseFuncClass{ast.SubstringIndex, 3, 3}},
	ast.Trim:           &trimFuncClass{baseFuncClass{ast.Trim, 1, 3}},
	ast.Upper:          &upperFuncClass{baseFuncClass{ast.Upper, 1, 1}},
	ast.Ucase:          &upperFuncClass{baseFuncClass{ast.Upper, 1, 1}},
	ast.Hex:            &hexFuncClass{baseFuncClass{ast.Hex, 1, 1}},
	ast.Unhex:          &unHexFuncClass{baseFuncClass{ast.Unhex, 1, 1}},
	ast.Rpad:           &rpadFuncClass{baseFuncClass{ast.Rpad, 3, 3}},
	ast.BitLength:      &bitLengthFuncClass{baseFuncClass{ast.BitLength, 1, 1}},
	ast.CharFunc:       &charFuncClass{baseFuncClass{ast.CharFunc, 2, -1}},
	ast.CharLength:     &charLengthFuncClass{baseFuncClass{ast.CharLength, 1, 1}},

	// information functions
	ast.ConnectionID: &connectionIDFuncClass{baseFuncClass{ast.ConnectionID, 0, 0}},
	ast.CurrentUser:  &currentUserFuncClass{baseFuncClass{ast.CurrentUser, 0, 0}},
	ast.Database:     &databaseFuncClass{baseFuncClass{ast.Database, 0, 0}},
	// This function is a synonym for DATABASE().
	// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_schema
	ast.Schema:       &databaseFuncClass{baseFuncClass{ast.Database, 0, 0}},
	ast.FoundRows:    &foundRowsFuncClass{baseFuncClass{ast.FoundRows, 0, 0}},
	ast.LastInsertId: &lastInsertIDFuncClass{baseFuncClass{ast.LastInsertId, 0, 1}},
	ast.User:         &userFuncClass{baseFuncClass{ast.User, 0, 0}},
	ast.Version:      &versionFuncClass{baseFuncClass{ast.Version, 0, 0}},

	// control functions
	ast.If:     &ifFuncClass{baseFuncClass{ast.If, 3, 3}},
	ast.Ifnull: &ifNullFuncClass{baseFuncClass{ast.Ifnull, 2, 2}},
	ast.Nullif: &nullIfFuncClass{baseFuncClass{ast.Nullif, 2, 2}},

	// miscellaneous functions
	ast.Sleep: &sleepFuncClass{baseFuncClass{ast.Sleep, 1, 1}},

	// get_lock() and release_lock() is parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	ast.GetLock:     &lockFuncClass{baseFuncClass{ast.GetLock, 2, 2}},
	ast.ReleaseLock: &releaseLockFuncClass{baseFuncClass{ast.ReleaseLock, 1, 1}},

	ast.AndAnd:     &andAndFuncClass{baseFuncClass{ast.AndAnd, 2, 2}},
	ast.OrOr:       &orOrFuncClass{baseFuncClass{ast.OrOr, 2, 2}},
	ast.GE:         &compareFuncClass{baseFuncClass{ast.GE, 2, 2}, opcode.GE},
	ast.LE:         &compareFuncClass{baseFuncClass{ast.LE, 2, 2}, opcode.LE},
	ast.EQ:         &compareFuncClass{baseFuncClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.NE:         &compareFuncClass{baseFuncClass{ast.NE, 2, 2}, opcode.NE},
	ast.LT:         &compareFuncClass{baseFuncClass{ast.LT, 2, 2}, opcode.LT},
	ast.GT:         &compareFuncClass{baseFuncClass{ast.GT, 2, 2}, opcode.GT},
	ast.NullEQ:     &compareFuncClass{baseFuncClass{ast.NullEQ, 2, 2}, opcode.NullEQ},
	ast.Plus:       &arithmeticFuncClass{baseFuncClass{ast.Plus, 2, 2}, opcode.Plus},
	ast.Minus:      &arithmeticFuncClass{baseFuncClass{ast.Minus, 2, 2}, opcode.Minus},
	ast.Mod:        &arithmeticFuncClass{baseFuncClass{ast.Mod, 2, 2}, opcode.Mod},
	ast.Div:        &arithmeticFuncClass{baseFuncClass{ast.Div, 2, 2}, opcode.Div},
	ast.Mul:        &arithmeticFuncClass{baseFuncClass{ast.Mul, 2, 2}, opcode.Mul},
	ast.IntDiv:     &arithmeticFuncClass{baseFuncClass{ast.IntDiv, 2, 2}, opcode.IntDiv},
	ast.LeftShift:  &bitOpFuncClass{baseFuncClass{ast.LeftShift, 2, 2}, opcode.LeftShift},
	ast.RightShift: &bitOpFuncClass{baseFuncClass{ast.RightShift, 2, 2}, opcode.RightShift},
	ast.And:        &bitOpFuncClass{baseFuncClass{ast.And, 2, 2}, opcode.And},
	ast.Or:         &bitOpFuncClass{baseFuncClass{ast.Or, 2, 2}, opcode.Or},
	ast.Xor:        &bitOpFuncClass{baseFuncClass{ast.Xor, 2, 2}, opcode.Xor},
	ast.LogicXor:   &logicXorFuncClass{baseFuncClass{ast.LogicXor, 2, 2}},
	ast.UnaryNot:   &unaryFuncClass{baseFuncClass{ast.UnaryNot, 1, 1}, opcode.Not},
	ast.BitNeg:     &unaryFuncClass{baseFuncClass{ast.BitNeg, 1, 1}, opcode.BitNeg},
	ast.UnaryPlus:  &unaryFuncClass{baseFuncClass{ast.UnaryPlus, 1, 1}, opcode.Plus},
	ast.UnaryMinus: &unaryFuncClass{baseFuncClass{ast.UnaryMinus, 1, 1}, opcode.Minus},
	ast.In:         &inFuncClass{baseFuncClass{ast.In, 1, -1}},
	ast.IsTruth:    &isTrueFuncClass{baseFuncClass{ast.IsTruth, 1, 1}, opcode.IsTruth},
	ast.IsFalsity:  &isTrueFuncClass{baseFuncClass{ast.IsFalsity, 1, 1}, opcode.IsFalsity},
	ast.Like:       &likeFuncClass{baseFuncClass{ast.Like, 3, 3}},
	ast.Regexp:     &regexpFuncClass{baseFuncClass{ast.Regexp, 2, 2}},
	ast.Case:       &caseWhenFuncClass{baseFuncClass{ast.Case, 1, -1}},
	ast.RowFunc:    &rowFuncClass{baseFuncClass{ast.RowFunc, 2, -1}},
	ast.SetVar:     &setVarFuncClass{baseFuncClass{ast.SetVar, 2, 2}},
	ast.GetVar:     &getVarFuncClass{baseFuncClass{ast.GetVar, 1, 1}},
}

// DynamicFuncs are those functions that
// use input parameter ctx or
// return an uncertain result would not be constant folded
// the value 0 means nothing
var DynamicFuncs = map[string]int{
	"rand":           0,
	"connection_id":  0,
	"current_user":   0,
	"database":       0,
	"found_rows":     0,
	"last_insert_id": 0,
	"user":           0,
	"version":        0,
	"sleep":          0,
	ast.GetVar:       0,
	ast.SetVar:       0,
	ast.Values:       0,
}

type baseFuncClass struct {
	funcName string
	MinArgs  int
	MaxArgs  int
}

func (b *baseFuncClass) checkValid(args []Expression) error {
	l := len(args)
	if l < b.MinArgs || (l > b.MaxArgs && b.MaxArgs != -1) {
		return errWrongParamCount.GenByArgs(b.funcName)
	}
	return nil
}

type coalesceFuncClass struct {
	baseFuncClass
}

func (b *coalesceFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	// TODO: Implement type resolver for every function.
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinCoalesce{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

type builtinCoalesce struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func (b *builtinCoalesce) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return
	}
	for _, d = range args {
		if !d.IsNull() {
			return d, nil
		}
	}
	return d, nil
}

type isNullFuncClass struct {
	baseFuncClass
}

func (b *isNullFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinIsNull{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

type builtinIsNull struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_isnull
func (b *builtinIsNull) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return
	}
	if args[0].IsNull() {
		d.SetInt64(1)
	} else {
		d.SetInt64(0)
	}
	return d, nil
}

type greatestFuncClass struct {
	baseFuncClass
}

func (b *greatestFuncClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := b.checkValid(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := &builtinGreatest{baseBuiltinFunc: newBaseBuiltinFunc(args, true, ctx)}
	f.self = f
	return f, nil
}

type builtinGreatest struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatest) eval(args []types.Datum) (d types.Datum, err error) {
	if args, err = b.evalArgs(args); err != nil {
		return
	}
	max := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(args); i++ {
		if args[i].IsNull() {
			d.SetNull()
			return
		}

		var cmp int
		if cmp, err = args[i].CompareDatum(sc, args[max]); err != nil {
			return
		}

		if cmp > 0 {
			max = i
		}
	}
	d = args[max]
	return
}
