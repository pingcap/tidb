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

// Extract is for time extract function.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type Extract struct {
	Unit string
	Date Expression
}

// Clone implements the Expression Clone interface.
func (e *Extract) Clone() Expression {
	n := *e
	return &n
}

// Eval implements the Expression Eval interface.
func (e *Extract) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	v, err := e.Date.Eval(ctx, args)
	if v == nil || err != nil {
		return nil, errors.Trace(err)
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = mysql.MaxFsp

	v, err = types.Convert(v, f)
	if v == nil || err != nil {
		return nil, errors.Trace(err)
	}

	t, ok := v.(mysql.Time)
	if !ok {
		return nil, errors.Errorf("need time type, but got %T", v)
	}

	n, err1 := extractTime(e.Unit, t)
	if err1 != nil {
		return nil, errors.Trace(err1)
	}

	return int64(n), nil
}

// IsStatic implements the Expression IsStatic interface.
func (e *Extract) IsStatic() bool {
	return e.Date.IsStatic()
}

// String implements the Expression String interface.
func (e *Extract) String() string {
	return fmt.Sprintf("EXTRACT(%s FROM %s)", strings.ToUpper(e.Unit), e.Date)
}

// Accept implements the Visitor Accept interface.
func (e *Extract) Accept(v Visitor) (Expression, error) {
	return v.VisitExtract(e)
}

func extractTime(unit string, t mysql.Time) (int, error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND":
		return t.Nanosecond() / 1000, nil
	case "SECOND":
		return t.Second(), nil
	case "MINUTE":
		return t.Minute(), nil
	case "HOUR":
		return t.Hour(), nil
	case "DAY":
		return t.Day(), nil
	case "WEEK":
		_, week := t.ISOWeek()
		return week, nil
	case "MONTH":
		return int(t.Month()), nil
	case "QUARTER":
		m := int(t.Month())
		// 1 - 3 -> 1
		// 4 - 6 -> 2
		// 7 - 9 -> 3
		// 10 - 12 -> 4
		return (m + 2) / 3, nil
	case "YEAR":
		return t.Year(), nil
	case "SECOND_MICROSECOND":
		return t.Second()*1000000 + t.Nanosecond()/1000, nil
	case "MINUTE_MICROSECOND":
		_, m, s := t.Clock()
		return m*100000000 + s*1000000 + t.Nanosecond()/1000, nil
	case "MINUTE_SECOND":
		_, m, s := t.Clock()
		return m*100 + s, nil
	case "HOUR_MICROSECOND":
		h, m, s := t.Clock()
		return h*10000000000 + m*100000000 + s*1000000 + t.Nanosecond()/1000, nil
	case "HOUR_SECOND":
		h, m, s := t.Clock()
		return h*10000 + m*100 + s, nil
	case "HOUR_MINUTE":
		h, m, _ := t.Clock()
		return h*100 + m, nil
	case "DAY_MICROSECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return (d*1000000+h*10000+m*100+s)*1000000 + t.Nanosecond()/1000, nil
	case "DAY_SECOND":
		h, m, s := t.Clock()
		d := t.Day()
		return d*1000000 + h*10000 + m*100 + s, nil
	case "DAY_MINUTE":
		h, m, _ := t.Clock()
		d := t.Day()
		return d*10000 + h*100 + m, nil
	case "DAY_HOUR":
		h, _, _ := t.Clock()
		d := t.Day()
		return d*100 + h, nil
	case "YEAR_MONTH":
		y, m, _ := t.Date()
		return y*100 + int(m), nil
	default:
		return 0, errors.Errorf("invalid unit %s", unit)
	}
}
