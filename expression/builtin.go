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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type baseBuiltinFunc struct {
	args      []Expression
	argValues []types.Datum
	ctx       context.Context
	self      builtinFunc
}

func newBaseBuiltinFunc(args []Expression, ctx context.Context) baseBuiltinFunc {
	return baseBuiltinFunc{
		args:      args,
		argValues: make([]types.Datum, len(args)),
		ctx:       ctx,
	}
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

// isDeterministic will be true by default. Non-deterministic function will override this function.
func (b *baseBuiltinFunc) isDeterministic() bool {
	return true
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

// equal only checks if both functions are non-deterministic and if these arguments are same.
// Function name will be checked outside.
func (b *baseBuiltinFunc) equal(fun builtinFunc) bool {
	if !b.self.isDeterministic() || !fun.isDeterministic() {
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

// builtinFunc stands for a particular function signature.
type builtinFunc interface {
	// eval does evaluation by the given row.
	eval([]types.Datum) (types.Datum, error)
	// getArgs returns the arguments expressions.
	getArgs() []Expression
	// isDeterministic checks if a function is deterministic.
	// A function is deterministic if it returns same results for same inputs.
	// e.g. random is non-deterministic.
	isDeterministic() bool
	// equal check if this function equals to another function.
	equal(builtinFunc) bool
	// getCtx returns this function's context.
	getCtx() context.Context
}

// builtinFunc stands for a class for a function which may contains multiple functions.
type functionClass interface {
	// getFunction gets a function signature by the types and the counts of given arguments.
	getFunction(args []Expression, ctx context.Context) (builtinFunc, error)
}

// BuiltinFunc is the function signature for builtin functions
type BuiltinFunc func([]types.Datum, context.Context) (types.Datum, error)

// Func is for a builtin function.
type Func struct {
	// F is the specific calling function.
	F BuiltinFunc
	// MinArgs is the minimal arguments needed,
	MinArgs int
	// MaxArgs is the maximal arguments needed, -1 for infinity.
	MaxArgs int
}

// Funcs holds all registered builtin functions.
var Funcs = map[string]Func{
	// common functions
	ast.Coalesce: {builtinCoalesce, 1, -1},
	ast.IsNull:   {builtinIsNull, 1, 1},
	ast.Greatest: {builtinGreatest, 2, -1},

	// math functions
	ast.Abs:     {builtinAbs, 1, 1},
	ast.Ceil:    {builtinCeil, 1, 1},
	ast.Ceiling: {builtinCeil, 1, 1},
	ast.Ln:      {builtinLog, 1, 1},
	ast.Log:     {builtinLog, 1, 2},
	ast.Log2:    {builtinLog2, 1, 1},
	ast.Log10:   {builtinLog10, 1, 1},
	ast.Pow:     {builtinPow, 2, 2},
	ast.Power:   {builtinPow, 2, 2},
	ast.Rand:    {builtinRand, 0, 1},
	ast.Round:   {builtinRound, 1, 2},

	// time functions
	ast.Curdate:          {builtinCurrentDate, 0, 0},
	ast.CurrentDate:      {builtinCurrentDate, 0, 0},
	ast.CurrentTime:      {builtinCurrentTime, 0, 1},
	ast.Date:             {builtinDate, 1, 1},
	ast.DateArith:        {builtinDateArith, 3, 3},
	ast.DateFormat:       {builtinDateFormat, 2, 2},
	ast.CurrentTimestamp: {builtinNow, 0, 1},
	ast.Curtime:          {builtinCurrentTime, 0, 1},
	ast.Day:              {builtinDay, 1, 1},
	ast.DayName:          {builtinDayName, 1, 1},
	ast.DayOfMonth:       {builtinDayOfMonth, 1, 1},
	ast.DayOfWeek:        {builtinDayOfWeek, 1, 1},
	ast.DayOfYear:        {builtinDayOfYear, 1, 1},
	ast.Extract:          {builtinExtract, 2, 2},
	ast.Hour:             {builtinHour, 1, 1},
	ast.MicroSecond:      {builtinMicroSecond, 1, 1},
	ast.Minute:           {builtinMinute, 1, 1},
	ast.Month:            {builtinMonth, 1, 1},
	ast.MonthName:        {builtinMonthName, 1, 1},
	ast.Now:              {builtinNow, 0, 1},
	ast.Second:           {builtinSecond, 1, 1},
	ast.StrToDate:        {builtinStrToDate, 2, 2},
	ast.Sysdate:          {builtinSysDate, 0, 1},
	ast.Time:             {builtinTime, 1, 1},
	ast.UTCDate:          {builtinUTCDate, 0, 0},
	ast.Week:             {builtinWeek, 1, 2},
	ast.Weekday:          {builtinWeekDay, 1, 1},
	ast.WeekOfYear:       {builtinWeekOfYear, 1, 1},
	ast.Year:             {builtinYear, 1, 1},
	ast.YearWeek:         {builtinYearWeek, 1, 2},
	ast.FromUnixTime:     {builtinFromUnixTime, 1, 2},
	ast.TimeDiff:         {builtinTimeDiff, 2, 2},

	// string functions
	ast.ASCII:          {builtinASCII, 1, 1},
	ast.Concat:         {builtinConcat, 1, -1},
	ast.ConcatWS:       {builtinConcatWS, 2, -1},
	ast.Convert:        {builtinConvert, 2, 2},
	ast.Lcase:          {builtinLower, 1, 1},
	ast.Left:           {builtinLeft, 2, 2},
	ast.Length:         {builtinLength, 1, 1},
	ast.Locate:         {builtinLocate, 2, 3},
	ast.Lower:          {builtinLower, 1, 1},
	ast.Ltrim:          {trimFn(strings.TrimLeft, spaceChars), 1, 1},
	ast.Repeat:         {builtinRepeat, 2, 2},
	ast.Replace:        {builtinReplace, 3, 3},
	ast.Reverse:        {builtinReverse, 1, 1},
	ast.Rtrim:          {trimFn(strings.TrimRight, spaceChars), 1, 1},
	ast.Space:          {builtinSpace, 1, 1},
	ast.Strcmp:         {builtinStrcmp, 2, 2},
	ast.Substring:      {builtinSubstring, 2, 3},
	ast.SubstringIndex: {builtinSubstringIndex, 3, 3},
	ast.Trim:           {builtinTrim, 1, 3},
	ast.Upper:          {builtinUpper, 1, 1},
	ast.Ucase:          {builtinUpper, 1, 1},
	ast.Hex:            {builtinHex, 1, 1},
	ast.Unhex:          {builtinUnHex, 1, 1},
	ast.Rpad:           {builtinRpad, 3, 3},
	ast.BitLength:      {builtinBitLength, 1, 1},
	ast.CharFunc:       {builtinChar, 2, -1},
	ast.CharLength:     {builtinCharLength, 1, 1},

	// information functions
	ast.ConnectionID: {builtinConnectionID, 0, 0},
	ast.CurrentUser:  {builtinCurrentUser, 0, 0},
	ast.Database:     {builtinDatabase, 0, 0},
	// This function is a synonym for DATABASE().
	// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_schema
	ast.Schema:       {builtinDatabase, 0, 0},
	ast.FoundRows:    {builtinFoundRows, 0, 0},
	ast.LastInsertId: {builtinLastInsertID, 0, 1},
	ast.User:         {builtinUser, 0, 0},
	ast.Version:      {builtinVersion, 0, 0},

	// control functions
	ast.If:     {builtinIf, 3, 3},
	ast.Ifnull: {builtinIfNull, 2, 2},
	ast.Nullif: {builtinNullIf, 2, 2},

	// miscellaneous functions
	ast.Sleep: {builtinSleep, 1, 1},

	// get_lock() and release_lock() is parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	ast.GetLock:     {builtinLock, 2, 2},
	ast.ReleaseLock: {builtinReleaseLock, 1, 1},

	// only used by new plan
	ast.AndAnd:     {builtinAndAnd, 2, 2},
	ast.OrOr:       {builtinOrOr, 2, 2},
	ast.GE:         {compareFuncFactory(opcode.GE), 2, 2},
	ast.LE:         {compareFuncFactory(opcode.LE), 2, 2},
	ast.EQ:         {compareFuncFactory(opcode.EQ), 2, 2},
	ast.NE:         {compareFuncFactory(opcode.NE), 2, 2},
	ast.LT:         {compareFuncFactory(opcode.LT), 2, 2},
	ast.GT:         {compareFuncFactory(opcode.GT), 2, 2},
	ast.NullEQ:     {compareFuncFactory(opcode.NullEQ), 2, 2},
	ast.Plus:       {arithmeticFuncFactory(opcode.Plus), 2, 2},
	ast.Minus:      {arithmeticFuncFactory(opcode.Minus), 2, 2},
	ast.Mod:        {arithmeticFuncFactory(opcode.Mod), 2, 2},
	ast.Div:        {arithmeticFuncFactory(opcode.Div), 2, 2},
	ast.Mul:        {arithmeticFuncFactory(opcode.Mul), 2, 2},
	ast.IntDiv:     {arithmeticFuncFactory(opcode.IntDiv), 2, 2},
	ast.LeftShift:  {bitOpFactory(opcode.LeftShift), 2, 2},
	ast.RightShift: {bitOpFactory(opcode.RightShift), 2, 2},
	ast.And:        {bitOpFactory(opcode.And), 2, 2},
	ast.Or:         {bitOpFactory(opcode.Or), 2, 2},
	ast.Xor:        {bitOpFactory(opcode.Xor), 2, 2},
	ast.LogicXor:   {builtinLogicXor, 2, 2},
	ast.UnaryNot:   {unaryOpFactory(opcode.Not), 1, 1},
	ast.BitNeg:     {unaryOpFactory(opcode.BitNeg), 1, 1},
	ast.UnaryPlus:  {unaryOpFactory(opcode.Plus), 1, 1},
	ast.UnaryMinus: {unaryOpFactory(opcode.Minus), 1, 1},
	ast.In:         {builtinIn, 1, -1},
	ast.IsTruth:    {isTrueOpFactory(opcode.IsTruth), 1, 1},
	ast.IsFalsity:  {isTrueOpFactory(opcode.IsFalsity), 1, 1},
	ast.Like:       {builtinLike, 3, 3},
	ast.Regexp:     {builtinRegexp, 2, 2},
	ast.Case:       {builtinCaseWhen, 1, -1},
	ast.RowFunc:    {builtinRow, 2, -1},
	ast.SetVar:     {builtinSetVar, 2, 2},
	ast.GetVar:     {builtinGetVar, 1, 1},
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

// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func builtinCoalesce(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	for _, d = range args {
		if !d.IsNull() {
			return d, nil
		}
	}
	return d, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_isnull
func builtinIsNull(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	if args[0].IsNull() {
		d.SetInt64(1)
	} else {
		d.SetInt64(0)
	}
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func builtinGreatest(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	max := 0
	sc := ctx.GetSessionVars().StmtCtx
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
