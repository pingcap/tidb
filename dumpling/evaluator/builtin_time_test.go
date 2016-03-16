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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestDate(c *C) {
	tblDate := []struct {
		Input  interface{}
		Expect interface{}
	}{
		{"2011-11-11", "2011-11-11"},
		{nil, nil},
		{"2011-11-11 10:10:10", "2011-11-11"},
	}
	dtblDate := tblToDtbl(tblDate)

	for _, t := range dtblDate {
		v, err := builtinDate(t["Input"], nil)
		c.Assert(err, IsNil)
		if v.Kind() != types.KindMysqlTime {
			c.Assert(v, DatumEquals, t["Expect"][0])
		} else {
			c.Assert(v.GetMysqlTime().String(), Equals, t["Expect"][0].GetString())
		}
	}

	// test year, month and day
	tbl := []struct {
		Input      string
		Year       int64
		Month      int64
		DayOfMonth int64
		DayOfWeek  int64
		DayOfYear  int64
		WeekDay    int64
		DayName    string
		Week       int64
		WeekOfYear int64
		YearWeek   int64
	}{
		{"2000-01-01", 2000, 1, 1, 7, 1, 5, "Saturday", 52, 52, 199952},
		{"2011-11-11", 2011, 11, 11, 6, 315, 4, "Friday", 45, 45, 201145},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		args := t["Input"]
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Year"][0])

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Month"][0])

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfMonth"][0])

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfWeek"][0])

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfYear"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["WeekDay"][0])

		v, err = builtinDayName(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayName"][0])

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Week"][0])

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["WeekOfYear"][0])

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["YearWeek"][0])
	}

	// test nil
	tblNil := []struct {
		Input      interface{}
		Year       interface{}
		Month      interface{}
		DayOfMonth interface{}
		DayOfWeek  interface{}
		DayOfYear  interface{}
		WeekDay    interface{}
		DayName    interface{}
		Week       interface{}
		WeekOfYear interface{}
		YearWeek   interface{}
	}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{"0000-00-00", int64(0), int64(0), int64(0), nil, nil, nil, nil, nil, nil, nil},
	}

	dtblNil := tblToDtbl(tblNil)
	for _, t := range dtblNil {
		args := t["Input"]
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Year"][0])

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Month"][0])

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfMonth"][0])

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfWeek"][0])

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayOfYear"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["WeekDay"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["DayName"][0])

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Week"][0])

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["WeekOfYear"][0])

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["YearWeek"][0])
	}
}

func (s *testEvaluatorSuite) TestClock(c *C) {
	// test hour, minute, second, micro second

	tbl := []struct {
		Input       string
		Hour        int64
		Minute      int64
		Second      int64
		MicroSecond int64
	}{
		{"10:10:10.123456", 10, 10, 10, 123456},
		{"11:11:11.11", 11, 11, 11, 110000},
		{"2010-10-10 11:11:11.11", 11, 11, 11, 110000},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		v, err := builtinHour(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Hour"][0])

		v, err = builtinMinute(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Minute"][0])

		v, err = builtinSecond(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["Second"][0])

		v, err = builtinMicroSecond(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, DatumEquals, t["MicroSecond"][0])
	}

	// nil
	v, err := builtinHour(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	v, err = builtinMinute(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	v, err = builtinSecond(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	v, err = builtinMicroSecond(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	// test error
	errTbl := []string{
		"2011-11-11T10:10:10.11",
	}

	for _, t := range errTbl {
		td := types.MakeDatums(t)
		_, err := builtinHour(td, nil)
		c.Assert(err, NotNil)

		_, err = builtinMinute(td, nil)
		c.Assert(err, NotNil)

		_, err = builtinSecond(td, nil)
		c.Assert(err, NotNil)

		_, err = builtinMicroSecond(td, nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestNow(c *C) {
	v, err := builtinNow(nil, nil)
	c.Assert(err, IsNil)
	t := v.GetMysqlTime()
	// we canot use a constant value to check now, so here
	// just to check whether has fractional seconds part.
	c.Assert(strings.Contains(t.String(), "."), IsFalse)

	v, err = builtinNow(types.MakeDatums(6), nil)
	c.Assert(err, IsNil)
	t = v.GetMysqlTime()
	c.Assert(strings.Contains(t.String(), "."), IsTrue)

	_, err = builtinNow(types.MakeDatums(8), nil)
	c.Assert(err, NotNil)

	_, err = builtinNow(types.MakeDatums(-2), nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestSysDate(c *C) {
	last := time.Now()
	v, err := builtinSysDate(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.TimeFormat))

	v, err = builtinSysDate(types.MakeDatums(6), nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.TimeFormat))

	_, err = builtinSysDate(types.MakeDatums(-2), nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestCurrentDate(c *C) {
	last := time.Now()
	v, err := builtinCurrentDate(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.DateFormat))
}

func (s *testEvaluatorSuite) TestCurrentTime(c *C) {
	tfStr := "15:04:05"

	last := time.Now()
	v, err := builtinCurrentTime(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime(types.MakeDatums(3), nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 12)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime(types.MakeDatums(6), nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 15)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime(types.MakeDatums(-1), nil)
	c.Assert(err, NotNil)

	v, err = builtinCurrentTime(types.MakeDatums(7), nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestDateArith(c *C) {
	ctx := mock.NewContext()

	// list all test cases
	tests := []struct {
		Date      interface{}
		Interval  interface{}
		Unit      string
		AddResult interface{}
		SubResult interface{}
		error     bool
	}{
		// basic test
		{"2011-11-11", 1, "DAY", "2011-11-12", "2011-11-10", false},
		// nil test
		{nil, 1, "DAY", nil, nil, false},
		{"2011-11-11", nil, "DAY", nil, nil, false},
		// tests for different units
		{"2011-11-11 10:10:10", 1000, "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000", false},
		{"2011-11-11 10:10:10", "10", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00", false},
		{"2011-11-11 10:10:10", "10", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10", false},
		{"2011-11-11 10:10:10", "10", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10", false},
		{"2011-11-11 10:10:10", "11", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10", false},
		{"2011-11-11 10:10:10", "4", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10", false},
		{"2011-11-11 10:10:10", "2", "YEAR", "2013-11-11 10:10:10", "2009-11-11 10:10:10", false},
		{"2011-11-11 10:10:10", "10.00100000", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000", false},
		{"2011-11-11 10:10:10", "10.0010000000", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50", false},
		{"2011-11-11 10:10:10", "10.0010000010", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990", false},
		{"2011-11-11 10:10:10", "10:10.100", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000", false},
		{"2011-11-11 10:10:10", "10:10", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00", false},
		{"2011-11-11 10:10:10", "10:10:10.100", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000", false},
		{"2011-11-11 10:10:10", "10:10:10", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00", false},
		{"2011-11-11 10:10:10", "10:10", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10", false},
		{"2011-11-11 10:10:10", "11 10:10:10.100", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000", false},
		{"2011-11-11 10:10:10", "11 10:10:10", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00", false},
		{"2011-11-11 10:10:10", "11 10:10", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10", false},
		{"2011-11-11 10:10:10", "11 10", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10", false},
		{"2011-11-11 10:10:10", "11-1", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10", false},
		{"2011-11-11 10:10:10", "11-11", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10", false},
		// tests for interval in day forms
		{"2011-11-11 10:10:10", "20", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", 19.88, "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "19.88", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10", false},
		{"2011-11-11 10:10:10", "prefix19suffix", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10", false},
		{"2011-11-11 10:10:10", "20-11", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "20,11", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10", false},
		{"2011-11-11 10:10:10", "1000", "dAy", "2014-08-07 10:10:10", "2009-02-14 10:10:10", false},
		{"2011-11-11 10:10:10", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10", false},
		{"2011-11-11 10:10:10", true, "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10", false},
		// test for different return data types
		{"2011-11-11", 1, "DAY", "2011-11-12", "2011-11-10", false},
		{"2011-11-11", 10, "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00", false},
		{"2011-11-11", 10, "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00", false},
		{"2011-11-11", 10, "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50", false},
		{"2011-11-11", "10:10", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00", false},
		{"2011-11-11", "10:10:10", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50", false},
		{"2011-11-11", "10:10:10.101010", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990", false},
		{"2011-11-11", "10:10", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50", false},
		{"2011-11-11", "10:10.101010", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990", false},
		{"2011-11-11", "10.101010", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990", false},
		{"2011-11-11 00:00:00", 1, "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00", false},
		{"2011-11-11 00:00:00", 10, "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00", false},
		{"2011-11-11 00:00:00", 10, "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00", false},
		{"2011-11-11 00:00:00", 10, "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50", false},
		// tests for invalid input
		{"2011-11-11", "abc1000", "MICROSECOND", nil, nil, true},
		{"20111111 10:10:10", "1", "DAY", nil, nil, true},
		{"2011-11-11", "10", "SECOND_MICROSECOND", nil, nil, true},
		{"2011-11-11", "10.0000", "MINUTE_MICROSECOND", nil, nil, true},
		{"2011-11-11", "10:10:10", "MINUTE_MICROSECOND", nil, nil, true},
	}

	// run the test cases
	for _, t := range tests {
		op := ast.NewValueExpr(ast.DateAdd)
		dateArithInterval := ast.NewValueExpr(
			ast.DateArithInterval{
				Unit:     t.Unit,
				Interval: ast.NewValueExpr(t.Interval),
			},
		)
		date := ast.NewValueExpr(t.Date)
		expr := &ast.FuncCallExpr{
			FnName: model.NewCIStr("DATE_ARITH"),
			Args: []ast.ExprNode{
				op,
				date,
				dateArithInterval,
			},
		}
		v, err := Eval(ctx, expr)
		if t.error == true {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if v == nil {
				c.Assert(v, Equals, t.AddResult)
			} else {
				value, ok := v.(mysql.Time)
				c.Assert(ok, IsTrue)
				c.Assert(value.String(), Equals, t.AddResult)
			}
		}

		op = ast.NewValueExpr(ast.DateSub)
		expr.Args[0] = op
		v, err = Eval(ctx, expr)
		if t.error == true {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if v == nil {
				c.Assert(v, Equals, t.AddResult)
			} else {
				value, ok := v.(mysql.Time)
				c.Assert(ok, IsTrue)
				c.Assert(value.String(), Equals, t.SubResult)
			}
		}
	}
}
