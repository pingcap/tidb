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
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

const (
	add = "ADD"
	sub = "SUB"
)

// DateCast is used for dealing with addition and substraction of time.
// If the Op value is ADD, then do date_add function.
// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
// If the Op value is SUB, then do date_sub function.
// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
type DateCast struct {
	Op       string
	Unit     string
	Date     Expression
	Interval Expression
}

func (dc *DateCast) isAdd() bool {
	if dc.Op == add {
		return true
	}

	return false
}

// Clone implements the Expression Clone interface.
func (dc *DateCast) Clone() Expression {
	n := *dc
	return &n
}

// IsStatic implements the Expression IsStatic interface.
func (dc *DateCast) IsStatic() bool {
	return dc.Date.IsStatic() && dc.Interval.IsStatic()
}

// Accept implements the Visitor Accept interface.
func (dc *DateCast) Accept(v Visitor) (Expression, error) {
	return v.VisitDateCast(dc)
}

// String implements the Expression String interface.
func (dc *DateCast) String() string {
	return fmt.Sprintf("DATE_%s(%s, INTERVAL %s %s)", dc.Op, dc.Date, dc.Interval, strings.ToUpper(dc.Unit))
}

// Eval implements the Expression Eval interface.
func (dc *DateCast) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	t, years, months, days, durations, err := dc.evalArgs(ctx, args)
	if t.IsZero() || err != nil {
		return nil, errors.Trace(err)
	}

	if !dc.isAdd() {
		years, months, days, durations = -years, -months, -days, -durations
	}
	t.Time = t.Time.Add(durations)
	t.Time = t.Time.AddDate(int(years), int(months), int(days))

	// "2011-11-11 10:10:20.000000" outputs "2011-11-11 10:10:20".
	if t.Time.Nanosecond() == 0 {
		t.Fsp = 0
	}

	return t, nil
}

func (dc *DateCast) evalArgs(ctx context.Context, args map[interface{}]interface{}) (
	mysql.Time, int64, int64, int64, time.Duration, error) {
	dv, err := dc.Date.Eval(ctx, args)
	if dv == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	sv, err := types.ToString(dv)
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp
	dv, err = types.Convert(sv, f)
	if dv == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	t, ok := dv.(mysql.Time)
	if !ok {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Errorf("need time type, but got %T", dv)
	}

	iv, err := dc.Interval.Eval(ctx, args)
	if iv == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	format, err := types.ToString(iv)
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	years, months, days, durations, err := mysql.ExtractTimeValue(dc.Unit, strings.TrimSpace(format))
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}

	return t, years, months, days, durations, nil
}
