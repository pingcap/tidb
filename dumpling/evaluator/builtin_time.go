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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func convertToTime(arg interface{}, tp byte) (interface{}, error) {
	f := types.NewFieldType(tp)
	f.Decimal = mysql.MaxFsp

	v, err := types.Convert(arg, f)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	t, ok := v.(mysql.Time)
	if !ok {
		return nil, errors.Errorf("need time type, but got %T", v)
	}

	return t, nil
}

func convertToDuration(arg interface{}, fsp int) (interface{}, error) {
	f := types.NewFieldType(mysql.TypeDuration)
	f.Decimal = fsp

	v, err := types.Convert(arg, f)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, nil
	}

	t, ok := v.(mysql.Duration)
	if !ok {
		return nil, errors.Errorf("need duration type, but got %T", v)
	}

	return t, nil
}

func builtinDate(args []interface{}, _ context.Context) (interface{}, error) {
	return convertToTime(args[0], mysql.TypeDate)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_day
// day is a synonym for DayOfMonth
func builtinDay(args []interface{}, ctx context.Context) (interface{}, error) {
	return builtinDayOfMonth(args, ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func builtinHour(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToDuration(args[0], mysql.MaxFsp)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	d := v.(mysql.Duration)
	return int64(d.Hour()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func builtinMinute(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToDuration(args[0], mysql.MaxFsp)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	d := v.(mysql.Duration)
	return int64(d.Minute()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func builtinSecond(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToDuration(args[0], mysql.MaxFsp)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	d := v.(mysql.Duration)
	return int64(d.Second()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func builtinMicroSecond(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToDuration(args[0], mysql.MaxFsp)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	d := v.(mysql.Duration)
	return int64(d.MicroSecond()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func builtinMonth(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		return int64(0), nil
	}

	return int64(t.Month()), nil
}

func builtinNow(args []interface{}, _ context.Context) (interface{}, error) {
	// TODO: if NOW is used in stored function or trigger, NOW will return the beginning time
	// of the execution.
	fsp := 0
	if len(args) == 1 {
		var err error
		if fsp, err = checkFsp(args[0]); err != nil {
			return nil, errors.Trace(err)
		}
	}

	t := mysql.Time{
		Time: time.Now(),
		Type: mysql.TypeDatetime,
		// set unspecified for later round
		Fsp: mysql.UnspecifiedFsp,
	}

	return t.RoundFrac(int(fsp))
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func builtinDayName(args []interface{}, ctx context.Context) (interface{}, error) {
	v, err := builtinWeekDay(args, ctx)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	weekday := v.(int64)
	if (weekday < 0) || (weekday >= int64(len(mysql.WeekdayNames))) {
		return nil, errors.Errorf("no name for invalid weekday: %d.", weekday)
	}
	return mysql.WeekdayNames[weekday], nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func builtinDayOfMonth(args []interface{}, _ context.Context) (interface{}, error) {
	// TODO: some invalid format like 2000-00-00 will return 0 too.
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		return int64(0), nil
	}

	return int64(t.Day()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func builtinDayOfWeek(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		// TODO: log warning or return error?
		return nil, nil
	}

	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	return int64(t.Weekday()) + 1, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func builtinDayOfYear(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	t := v.(mysql.Time)
	if t.IsZero() {
		// TODO: log warning or return error?
		return nil, nil
	}

	return int64(t.YearDay()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func builtinWeek(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		// TODO: log warning or return error?
		return nil, nil
	}

	// TODO: support multi mode for week
	_, week := t.ISOWeek()
	return int64(week), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekday
func builtinWeekDay(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		// TODO: log warning or return error?
		return nil, nil
	}

	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	w := (int64(t.Weekday()) + 6) % 7
	return w, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func builtinWeekOfYear(args []interface{}, ctx context.Context) (interface{}, error) {
	// WeekOfYear is equivalent to to Week(date, 3)
	return builtinWeek([]interface{}{args[0], 3}, ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func builtinYear(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		return int64(0), nil
	}

	return int64(t.Year()), nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func builtinYearWeek(args []interface{}, _ context.Context) (interface{}, error) {
	v, err := convertToTime(args[0], mysql.TypeDate)
	if err != nil || v == nil {
		return v, err
	}

	// No need to check type here.
	t := v.(mysql.Time)
	if t.IsZero() {
		// TODO: log warning or return error?
		return nil, nil
	}

	// TODO: support multi mode for week
	year, week := t.ISOWeek()
	return int64(year*100 + week), nil
}

func builtinSysDate(args []interface{}, ctx context.Context) (interface{}, error) {
	// SYSDATE is not the same as NOW if NOW is used in a stored function or trigger.
	// But here we can just think they are the same because we don't support stored function
	// and trigger now.
	return builtinNow(args, ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func builtinCurrentDate(args []interface{}, _ context.Context) (interface{}, error) {
	year, month, day := time.Now().Date()
	return mysql.Time{
		Time: time.Date(year, month, day, 0, 0, 0, 0, time.Local),
		Type: mysql.TypeDate, Fsp: 0}, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curtime
func builtinCurrentTime(args []interface{}, _ context.Context) (interface{}, error) {
	fsp := 0
	if len(args) == 1 {
		var err error
		if fsp, err = checkFsp(args[0]); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return convertToDuration(time.Now().Format("15:04:05.000000"), fsp)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func builtinExtract(args []interface{}, _ context.Context) (interface{}, error) {
	unit := args[0].(string)
	val := args[1]

	if val == nil {
		return nil, nil
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp
	var err error
	val, err = types.Convert(val, f)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if val == nil {
		return nil, nil
	}

	t, ok := val.(mysql.Time)
	if !ok {
		return nil, errors.Errorf("need time type, but got %T", val)
	}
	n, err1 := mysql.ExtractTimeNum(unit, t)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}
	return n, nil
}

func checkFsp(arg interface{}) (int, error) {
	fsp, err := types.ToInt64(arg)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if int(fsp) > mysql.MaxFsp {
		return 0, errors.Errorf("Too big precision %d specified. Maximum is 6.", fsp)
	} else if fsp < 0 {
		return 0, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	return int(fsp), nil
}

func builtinDateArith(args []interface{}, ctx context.Context) (interface{}, error) {
	// Op is used for distinguishing date_add and date_sub.
	// args[0] -> Op
	// args[1] -> Date
	// args[2] -> DateArithInterval
	// health check for date and interval
	if args[1] == nil {
		return nil, nil
	}
	nodeDate := args[1]
	nodeInterval := args[2].(ast.DateArithInterval)
	nodeIntervalVal := nodeInterval.Interval.GetValue()
	if nodeIntervalVal == nil {
		return nil, nil
	}
	// parse date
	fieldType := mysql.TypeDate
	var resultField *types.FieldType
	switch x := nodeDate.(type) {
	case mysql.Time:
		if (x.Type == mysql.TypeDatetime) || (x.Type == mysql.TypeTimestamp) {
			fieldType = mysql.TypeDatetime
		}
	case string:
		if !mysql.IsDateFormat(x) {
			fieldType = mysql.TypeDatetime
		}
	case int64:
		if t, err := mysql.ParseTimeFromInt64(x); err == nil {
			if (t.Type == mysql.TypeDatetime) || (t.Type == mysql.TypeTimestamp) {
				fieldType = mysql.TypeDatetime
			}
		}
	}
	if mysql.IsClockUnit(nodeInterval.Unit) {
		fieldType = mysql.TypeDatetime
	}
	resultField = types.NewFieldType(fieldType)
	resultField.Decimal = mysql.MaxFsp
	value, err := types.Convert(nodeDate, resultField)
	if err != nil {
		return nil, ErrInvalidOperation.Gen("DateArith invalid args, need date but get %T", nodeDate)
	}
	if value == nil {
		return nil, ErrInvalidOperation.Gen("DateArith invalid args, need date but get %v", value)
	}
	result, ok := value.(mysql.Time)
	if !ok {
		return nil, ErrInvalidOperation.Gen("DateArith need time type, but got %T", value)
	}
	// parse interval
	var interval string
	if strings.ToLower(nodeInterval.Unit) == "day" {
		day, err2 := parseDayInterval(nodeIntervalVal)
		if err2 != nil {
			return nil, ErrInvalidOperation.Gen("DateArith invalid day interval, need int but got %T", nodeIntervalVal)
		}
		interval = fmt.Sprintf("%d", day)
	} else {
		interval = fmt.Sprintf("%v", nodeIntervalVal)
	}
	year, month, day, duration, err := mysql.ExtractTimeValue(nodeInterval.Unit, interval)
	if err != nil {
		return nil, errors.Trace(err)
	}
	op := args[0].(ast.DateArithType)
	if op == ast.DateSub {
		year, month, day, duration = -year, -month, -day, -duration
	}
	result.Time = result.Time.Add(duration)
	result.Time = result.Time.AddDate(int(year), int(month), int(day))
	if result.Time.Nanosecond() == 0 {
		result.Fsp = 0
	}
	return result, nil
}

var reg = regexp.MustCompile(`[\d]+`)

func parseDayInterval(value interface{}) (int64, error) {
	switch v := value.(type) {
	case string:
		s := strings.ToLower(v)
		if s == "false" {
			return 0, nil
		} else if s == "true" {
			return 1, nil
		}
		value = reg.FindString(v)
	}

	return types.ToInt64(value)
}
