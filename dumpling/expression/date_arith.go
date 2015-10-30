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
	// DateAdd is to run date_add function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateAdd = 1
	// DateSub is to run date_sub function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateSub = 2
)

// DateArith is used for dealing with addition and substraction of time.
type DateArith struct {
	Op       int
	Unit     string
	Date     Expression
	Interval Expression
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
	t, years, months, days, durations, err := da.evalArgs(ctx, args)
	if t.IsZero() || err != nil {
		return nil, errors.Trace(err)
	}

	if !da.isAdd() {
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

func (da *DateArith) evalArgs(ctx context.Context, args map[interface{}]interface{}) (
	mysql.Time, int64, int64, int64, time.Duration, error) {
	dVal, err := da.Date.Eval(ctx, args)
	if dVal == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	dValStr, err := types.ToString(dVal)
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp
	dVal, err = types.Convert(dValStr, f)
	if dVal == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	t, ok := dVal.(mysql.Time)
	if !ok {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Errorf("need time type, but got %T", dVal)
	}

	iVal, err := da.Interval.Eval(ctx, args)
	if iVal == nil || err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	iValStr, err := types.ToString(iVal)
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}
	years, months, days, durations, err := mysql.ExtractTimeValue(da.Unit, strings.TrimSpace(iValStr))
	if err != nil {
		return mysql.ZeroTimestamp, 0, 0, 0, 0, errors.Trace(err)
	}

	return t, years, months, days, durations, nil
}
