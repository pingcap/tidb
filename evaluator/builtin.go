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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
)

// Func is for a builtin function.
type Func struct {
	// F is the specific calling function.
	F func([]interface{}, context.Context) (interface{}, error)
	// MinArgs is the minimal arguments needed,
	MinArgs int
	// MaxArgs is the maximal arguments needed, -1 for infinity.
	MaxArgs int
	// IsStatic shows whether this function can be called statically.
	IsStatic bool
	// IsAggregate represents whether this function is an aggregate function or not.
	IsAggregate bool
}

// Funcs holds all registered builtin functions.
var Funcs = map[string]Func{
	// common functions
	"coalesce": {builtinCoalesce, 1, -1, true, false},

	// math functions
	"abs":   {builtinAbs, 1, 1, true, false},
	"pow":   {builtinPow, 2, 2, true, false},
	"power": {builtinPow, 2, 2, true, false},
	"rand":  {builtinRand, 0, 1, true, false},

	// time functions
	"curdate":           {builtinCurrentDate, 0, 0, false, false},
	"current_date":      {builtinCurrentDate, 0, 0, false, false},
	"current_time":      {builtinCurrentTime, 0, 1, false, false},
	"current_timestamp": {builtinNow, 0, 1, false, false},
	"curtime":           {builtinCurrentTime, 0, 1, false, false},
	"date":              {builtinDate, 1, 1, true, false},
	"day":               {builtinDay, 1, 1, true, false},
	"dayname":           {builtinDayName, 1, 1, true, false},
	"dayofmonth":        {builtinDayOfMonth, 1, 1, true, false},
	"dayofweek":         {builtinDayOfWeek, 1, 1, true, false},
	"dayofyear":         {builtinDayOfYear, 1, 1, true, false},
	"hour":              {builtinHour, 1, 1, true, false},
	"microsecond":       {builtinMicroSecond, 1, 1, true, false},
	"minute":            {builtinMinute, 1, 1, true, false},
	"month":             {builtinMonth, 1, 1, true, false},
	"now":               {builtinNow, 0, 1, false, false},
	"second":            {builtinSecond, 1, 1, true, false},
	"sysdate":           {builtinSysDate, 0, 1, false, false},
	"week":              {builtinWeek, 1, 2, true, false},
	"weekday":           {builtinWeekDay, 1, 1, true, false},
	"weekofyear":        {builtinWeekOfYear, 1, 1, true, false},
	"year":              {builtinYear, 1, 1, true, false},
	"yearweek":          {builtinYearWeek, 1, 2, true, false},
	"extract":           {builtinExtract, 2, 2, true, false},
	"date_arith":        {builtinDateArith, 3, 3, true, false},

	// control functions
	"if":     {builtinIf, 3, 3, true, false},
	"ifnull": {builtinIfNull, 2, 2, true, false},
	"nullif": {builtinNullIf, 2, 2, true, false},

	// string functions
	"concat":          {builtinConcat, 1, -1, true, false},
	"concat_ws":       {builtinConcatWS, 2, -1, true, false},
	"left":            {builtinLeft, 2, 2, true, false},
	"length":          {builtinLength, 1, 1, true, false},
	"lower":           {builtinLower, 1, 1, true, false},
	"repeat":          {builtinRepeat, 2, 2, true, false},
	"replace":         {builtinReplace, 3, 3, true, false},
	"upper":           {builtinUpper, 1, 1, true, false},
	"strcmp":          {builtinStrcmp, 2, 2, true, false},
	"convert":         {builtinConvert, 2, 2, true, false},
	"substring":       {builtinSubstring, 2, 3, true, false},
	"substring_index": {builtinSubstringIndex, 3, 3, true, false},
	"locate":          {builtinLocate, 2, 3, true, false},
	"trim":            {builtinTrim, 1, 3, true, false},

	// information functions
	"current_user":  {builtinCurrentUser, 0, 0, false, false},
	"database":      {builtinDatabase, 0, 0, false, false},
	"found_rows":    {builtinFoundRows, 0, 0, false, false},
	"user":          {builtinUser, 0, 0, false, false},
	"connection_id": {builtinConnectionID, 0, 0, true, false},
	"version":       {builtinVersion, 0, 0, true, false},
}

func invArg(arg interface{}, s string) error {
	return errors.Errorf("invalid argument %v (type %T) for %s", arg, arg, s)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func builtinCoalesce(args []interface{}, ctx context.Context) (v interface{}, err error) {
	for _, v := range args {
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}
