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
	"coalesce": {builtinCoalesce, 1, -1},
	"isnull":   {builtinIsNull, 1, 1},

	// math functions
	"abs":   {builtinAbs, 1, 1},
	"pow":   {builtinPow, 2, 2},
	"power": {builtinPow, 2, 2},
	"rand":  {builtinRand, 0, 1},
	"round": {builtinRound, 1, 2},

	// time functions
	"curdate":           {builtinCurrentDate, 0, 0},
	"current_date":      {builtinCurrentDate, 0, 0},
	"current_time":      {builtinCurrentTime, 0, 1},
	"current_timestamp": {builtinNow, 0, 1},
	"curtime":           {builtinCurrentTime, 0, 1},
	"date":              {builtinDate, 1, 1},
	"date_arith":        {builtinDateArith, 3, 3},
	"date_format":       {builtinDateFormat, 2, 2},
	"day":               {builtinDay, 1, 1},
	"dayname":           {builtinDayName, 1, 1},
	"dayofmonth":        {builtinDayOfMonth, 1, 1},
	"dayofweek":         {builtinDayOfWeek, 1, 1},
	"dayofyear":         {builtinDayOfYear, 1, 1},
	"extract":           {builtinExtract, 2, 2},
	"hour":              {builtinHour, 1, 1},
	"microsecond":       {builtinMicroSecond, 1, 1},
	"minute":            {builtinMinute, 1, 1},
	"month":             {builtinMonth, 1, 1},
	"monthname":         {builtinMonthName, 1, 1},
	"now":               {builtinNow, 0, 1},
	"second":            {builtinSecond, 1, 1},
	"sysdate":           {builtinSysDate, 0, 1},
	"time":              {builtinTime, 1, 1},
	"utc_date":          {builtinUTCDate, 0, 0},
	"week":              {builtinWeek, 1, 2},
	"weekday":           {builtinWeekDay, 1, 1},
	"weekofyear":        {builtinWeekOfYear, 1, 1},
	"year":              {builtinYear, 1, 1},
	"yearweek":          {builtinYearWeek, 1, 2},

	// string functions
	"ascii":           {builtinASCII, 1, 1},
	"concat":          {builtinConcat, 1, -1},
	"concat_ws":       {builtinConcatWS, 2, -1},
	"convert":         {builtinConvert, 2, 2},
	"lcase":           {builtinLower, 1, 1},
	"left":            {builtinLeft, 2, 2},
	"length":          {builtinLength, 1, 1},
	"locate":          {builtinLocate, 2, 3},
	"lower":           {builtinLower, 1, 1},
	"ltrim":           {trimFn(strings.TrimLeft, spaceChars), 1, 1},
	"repeat":          {builtinRepeat, 2, 2},
	"replace":         {builtinReplace, 3, 3},
	"reverse":         {builtinReverse, 1, 1},
	"rtrim":           {trimFn(strings.TrimRight, spaceChars), 1, 1},
	"strcmp":          {builtinStrcmp, 2, 2},
	"substring":       {builtinSubstring, 2, 3},
	"substring_index": {builtinSubstringIndex, 3, 3},
	"trim":            {builtinTrim, 1, 3},
	"upper":           {builtinUpper, 1, 1},
	"ucase":           {builtinUpper, 1, 1},

	// information functions
	"connection_id":  {builtinConnectionID, 0, 0},
	"current_user":   {builtinCurrentUser, 0, 0},
	"database":       {builtinDatabase, 0, 0},
	"found_rows":     {builtinFoundRows, 0, 0},
	"last_insert_id": {builtinLastInsertID, 0, 1},
	"user":           {builtinUser, 0, 0},
	"version":        {builtinVersion, 0, 0},

	// control functions
	"if":     {builtinIf, 3, 3},
	"ifnull": {builtinIfNull, 2, 2},
	"nullif": {builtinNullIf, 2, 2},

	// only used by new plan
	"&&":         {builtinAndAnd, 2, 2},
	"||":         {builtinOrOr, 2, 2},
	">=":         {compareFuncFactory(opcode.GE), 2, 2},
	"<=":         {compareFuncFactory(opcode.LE), 2, 2},
	"=":          {compareFuncFactory(opcode.EQ), 2, 2},
	"!=":         {compareFuncFactory(opcode.NE), 2, 2},
	"<":          {compareFuncFactory(opcode.LT), 2, 2},
	">":          {compareFuncFactory(opcode.GT), 2, 2},
	"<=>":        {compareFuncFactory(opcode.NullEQ), 2, 2},
	"+":          {arithmeticFuncFactory(opcode.Plus), 2, 2},
	"-":          {arithmeticFuncFactory(opcode.Minus), 2, 2},
	"%":          {arithmeticFuncFactory(opcode.Mod), 2, 2},
	"/":          {arithmeticFuncFactory(opcode.Div), 2, 2},
	"*":          {arithmeticFuncFactory(opcode.Mul), 2, 2},
	"DIV":        {arithmeticFuncFactory(opcode.IntDiv), 2, 2},
	"<<":         {bitOpFactory(opcode.LeftShift), 2, 2},
	">>":         {bitOpFactory(opcode.RightShift), 2, 2},
	"&":          {bitOpFactory(opcode.And), 2, 2},
	"|":          {bitOpFactory(opcode.Or), 2, 2},
	"^":          {bitOpFactory(opcode.Xor), 2, 2},
	"XOR":        {builtinLogicXor, 2, 2},
	"not":        {unaryOpFactory(opcode.Not), 1, 1},
	"bitneg":     {unaryOpFactory(opcode.BitNeg), 1, 1},
	"unaryplus":  {unaryOpFactory(opcode.Plus), 1, 1},
	"unaryminus": {unaryOpFactory(opcode.Minus), 1, 1},
}

// See: http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func builtinCoalesce(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	for _, d = range args {
		if !d.IsNull() {
			return d, nil
		}
	}
	return d, nil
}

// See: https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_isnull
func builtinIsNull(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	if args[0].IsNull() {
		d.SetInt64(1)
	} else {
		d.SetInt64(0)
	}
	return d, nil
}
