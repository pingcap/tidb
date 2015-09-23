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
	"fmt"
	"strings"

	"github.com/juju/errors"
)

var builtin = map[string]struct {
	f           func([]interface{}, map[interface{}]interface{}) (interface{}, error)
	minArgs     int
	maxArgs     int
	isStatic    bool
	isAggregate bool
}{
	// common functions
	"database": {builtinDatabase, 0, 0, false, false},
	"coalesce": {builtinCoalesce, 1, -1, true, false},

	// math functions
	"abs": {builtinAbs, 1, 1, true, false},

	// group by functions
	"avg":          {builtinAvg, 1, 1, false, true},
	"count":        {builtinCount, 1, 1, false, true},
	"group_concat": {builtinGroupConcat, 1, -1, false, true},
	"max":          {builtinMax, 1, 1, false, true},
	"min":          {builtinMin, 1, 1, false, true},
	"sum":          {builtinSum, 1, 1, false, true},

	// time functions
	"current_timestamp": {builtinNow, 0, 1, false, false},
	"date":              {builtinDate, 8, 8, true, false},
	"day":               {builtinDay, 1, 1, true, false},
	"dayofmonth":        {builtinDayOfMonth, 1, 1, true, false},
	"dayofweek":         {builtinDayOfWeek, 1, 1, true, false},
	"dayofyear":         {builtinDayOfYear, 1, 1, true, false},
	"hour":              {builtinHour, 1, 1, true, false},
	"microsecond":       {builtinMicroSecond, 1, 1, true, false},
	"minute":            {builtinMinute, 1, 1, true, false},
	"month":             {builtinMonth, 1, 1, true, false},
	"now":               {builtinNow, 0, 1, false, false},
	"second":            {builtinSecond, 1, 1, true, false},
	"week":              {builtinWeek, 1, 2, true, false},
	"weekday":           {builtinWeekDay, 1, 1, true, false},
	"weekofyear":        {builtinWeekOfYear, 1, 1, true, false},
	"year":              {builtinYear, 1, 1, true, false},
	"yearweek":          {builtinYearWeek, 1, 2, true, false},

	// control functions
	"if":     {builtinIf, 3, 3, true, false},
	"ifnull": {builtinIfNull, 2, 2, true, false},
	"nullif": {builtinNullIf, 2, 2, true, false},

	// string functions
	"concat":    {builtinConcat, 1, -1, true, false},
	"concat_ws": {builtinConcatWS, 2, -1, true, false},
	"left":      {builtinLeft, 2, 2, true, false},
	"length":    {builtinLength, 1, 1, true, false},
	"repeat":    {builtinRepeat, 2, 2, true, false},

	// information functions
	"found_rows": {builtinFoundRows, 0, 0, false, false},
}

func badNArgs(min int, s string, args []interface{}) error {
	a := []string{}
	for _, v := range args {
		a = append(a, fmt.Sprintf("%v", v))
	}
	switch len(args) < min {
	case true:
		return errors.Errorf("missing argument to %s(%s)", s, strings.Join(a, ", "))
	default: //case false:
		return errors.Errorf("too many arguments to %s(%s)", s, strings.Join(a, ", "))
	}
}

func invArg(arg interface{}, s string) error {
	return errors.Errorf("invalid argument %v (type %T) for %s", arg, arg, s)
}

// See: http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_coalesce
func builtinCoalesce(args []interface{}, ctx map[interface{}]interface{}) (v interface{}, err error) {
	for _, v := range args {
		if v != nil {
			return v, nil
		}
	}
	return nil, nil
}
