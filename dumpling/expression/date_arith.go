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
	"regexp"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

// DateArithType is type for DateArith option.
type DateArithType byte

const (
	// DateAdd is to run adddate or date_add function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateAdd DateArithType = iota + 1
	// DateSub is to run subdate or date_sub function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateSub
)

// DateArith is used for dealing with addition and substraction of time.
type DateArith struct {
	// Op is used for distinguishing date_add and date_sub.
	Op       DateArithType
	Date     Expression
	Unit     string
	Interval Expression
}

type evalArgsResult struct {
	time     mysql.Time
	year     int64
	month    int64
	day      int64
	duration time.Duration
}

func (da *DateArith) isAdd() bool {
	if da.Op == DateAdd {
		return true
	}

	return false
}

// Clone implements the Expression Clone interface.
func (da *DateArith) Clone() Expression {
	n := *da
	return &n
}

// IsStatic implements the Expression IsStatic interface.
func (da *DateArith) IsStatic() bool {
	return da.Date.IsStatic() && da.Interval.IsStatic()
}

// Accept implements the Visitor Accept interface.
func (da *DateArith) Accept(v Visitor) (Expression, error) {
	return v.VisitDateArith(da)
}

// String implements the Expression String interface.
func (da *DateArith) String() string {
	var str string
	if da.isAdd() {
		str = "DATE_ADD"
	} else {
		str = "DATE_SUB"
	}

	return fmt.Sprintf("%s(%s, INTERVAL %s %s)", str, da.Date, da.Interval, strings.ToUpper(da.Unit))
}

// Eval implements the Expression Eval interface.
func (da *DateArith) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	val, err := da.evalArgs(ctx, args)
	if val.time.IsZero() || err != nil {
		return nil, errors.Trace(err)
	}

	if !da.isAdd() {
		val.year, val.month, val.day, val.duration =
			-val.year, -val.month, -val.day, -val.duration
	}
	val.time.Time = val.time.Time.Add(val.duration)
	val.time.Time = val.time.Time.AddDate(int(val.year), int(val.month), int(val.day))

	// "2011-11-11 10:10:20.000000" outputs "2011-11-11 10:10:20".
	if val.time.Time.Nanosecond() == 0 {
		val.time.Fsp = 0
	}

	return val.time, nil
}

func (da *DateArith) evalArgs(ctx context.Context, args map[interface{}]interface{}) (
	*evalArgsResult, error) {
	ret := &evalArgsResult{time: mysql.ZeroTimestamp}

	dVal, err := da.Date.Eval(ctx, args)
	if dVal == nil || err != nil {
		return ret, errors.Trace(err)
	}
	dValStr, err := types.ToString(dVal)
	if err != nil {
		return ret, errors.Trace(err)
	}
	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp
	dVal, err = types.Convert(dValStr, f)
	if dVal == nil || err != nil {
		return ret, errors.Trace(err)
	}

	var ok bool
	ret.time, ok = dVal.(mysql.Time)
	if !ok {
		return ret, errors.Errorf("need time type, but got %T", dVal)
	}

	iVal, err := da.Interval.Eval(ctx, args)
	if iVal == nil || err != nil {
		ret.time = mysql.ZeroTimestamp
		return ret, errors.Trace(err)
	}
	// handle adddate(expr,days) or subdate(expr,days) form
	if strings.ToLower(da.Unit) == "day" {
		if iVal, err = da.evalDaysForm(iVal); err != nil {
			return ret, errors.Trace(err)
		}
	}

	iValStr, err := types.ToString(iVal)
	if err != nil {
		return ret, errors.Trace(err)
	}
	ret.year, ret.month, ret.day, ret.duration, err = mysql.ExtractTimeValue(da.Unit, strings.TrimSpace(iValStr))
	if err != nil {
		return ret, errors.Trace(err)
	}

	return ret, nil
}

var reg = regexp.MustCompile(`[\d]+`)

func (da *DateArith) evalDaysForm(val interface{}) (interface{}, error) {
	switch val.(type) {
	case string:
		if strings.ToLower(val.(string)) == "false" {
			return 0, nil
		}
		if strings.ToLower(val.(string)) == "true" {
			return 1, nil
		}
		val = reg.FindString(val.(string))
	}

	return types.ToInt64(val)
}
