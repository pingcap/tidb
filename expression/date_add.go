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
	"github.com/pingcap/tidb/context"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/util/types"
)

// DateAdd is for time date_add function.
// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
type DateAdd struct {
	Unit     string
	Date     Expression
	Interval Expression
}

// Clone implements the Expression Clone interface.
func (da *DateAdd) Clone() Expression {
	n := *da
	return &n
}

// Eval implements the Expression Eval interface.
func (da *DateAdd) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	dv, err := da.Date.Eval(ctx, args)
	if dv == nil || err != nil {
		return nil, errors.Trace(err)
	}

	sv, err := types.ToString(dv)
	if err != nil {
		return nil, errors.Trace(err)
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp

	dv, err = types.Convert(sv, f)
	if dv == nil || err != nil {
		return nil, errors.Trace(err)
	}

	t, ok := dv.(mysql.Time)
	if !ok {
		return nil, errors.Errorf("need time type, but got %T", dv)
	}

	iv, err := da.Interval.Eval(ctx, args)
	if iv == nil || err != nil {
		return nil, errors.Trace(err)
	}

	format, err := types.ToString(iv)
	if err != nil {
		return nil, errors.Trace(err)
	}

	years, months, days, durations, err := extractTimeValue(da.Unit, format)
	if err != nil {
		return nil, errors.Trace(err)
	}

	t.Time = t.Time.Add(durations)
	t.Time = t.Time.AddDate(int(years), int(months), int(days))
	return t, nil
}

// IsStatic implements the Expression IsStatic interface.
func (da *DateAdd) IsStatic() bool {
	return da.Date.IsStatic() && da.Interval.IsStatic()
}

// String implements the Expression String interface.
func (da *DateAdd) String() string {
	return fmt.Sprintf("DATE_ADD(%s, INTERVAL %s %s)", da.Date, da.Interval, strings.ToUpper(da.Unit))
}

// Accept implements the Visitor Accept interface.
func (da *DateAdd) Accept(v Visitor) (Expression, error) {
	return v.VisitDateAdd(da)
}
