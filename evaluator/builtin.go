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

package evaluator

import (
	"strings"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

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
	ast.Sysdate:          {builtinSysDate, 0, 1},
	ast.Time:             {builtinTime, 1, 1},
	ast.UTCDate:          {builtinUTCDate, 0, 0},
	ast.Week:             {builtinWeek, 1, 2},
	ast.Weekday:          {builtinWeekDay, 1, 1},
	ast.WeekOfYear:       {builtinWeekOfYear, 1, 1},
	ast.Year:             {builtinYear, 1, 1},
	ast.YearWeek:         {builtinYearWeek, 1, 2},

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

	// information functions
	ast.ConnectionID: {builtinConnectionID, 0, 0},
	ast.CurrentUser:  {builtinCurrentUser, 0, 0},
	ast.Database:     {builtinDatabase, 0, 0},
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
func builtinGreatest(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	max := 0
	for i := 0; i < len(args); i++ {
		if args[i].IsNull() {
			d.SetNull()
			return
		}

		var cmp int
		if cmp, err = args[i].CompareDatum(args[max]); err != nil {
			return
		}

		if cmp > 0 {
			max = i
		}
	}
	d = args[max]
	return
}
