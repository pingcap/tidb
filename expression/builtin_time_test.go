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
	"math"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/timeutil"
)

func (s *testEvaluatorSuite) TestDate(c *C) {
	defer testleak.AfterTest(c)()
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
		fc := funcs[ast.Date]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Expect"][0])
	}

	// test year, month and day
	tbl := []struct {
		Input      string
		Year       int64
		Month      int64
		MonthName  string
		DayOfMonth int64
		DayOfWeek  int64
		DayOfYear  int64
		WeekDay    int64
		DayName    string
		Week       int64
		WeekOfYear int64
		YearWeek   int64
	}{
		{"2000-01-01", 2000, 1, "January", 1, 7, 1, 5, "Saturday", 0, 52, 199952},
		{"2011-11-11", 2011, 11, "November", 11, 6, 315, 4, "Friday", 45, 45, 201145},
		{"0000-01-01", int64(0), 1, "January", 1, 7, 1, 5, "Saturday", 1, 52, 1},
	}

	dtbl := tblToDtbl(tbl)
	for ith, t := range dtbl {
		fc := funcs[ast.Year]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		fc = funcs[ast.Month]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		fc = funcs[ast.MonthName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		fc = funcs[ast.DayOfMonth]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		fc = funcs[ast.DayOfWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		fc = funcs[ast.DayOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		fc = funcs[ast.Weekday]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		fc = funcs[ast.DayName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		fc = funcs[ast.Week]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0], Commentf("no.%d", ith))

		fc = funcs[ast.WeekOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		fc = funcs[ast.YearWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["YearWeek"][0], Commentf("no.%d", ith))
	}

	// test nil
	tblNil := []struct {
		Input      interface{}
		Year       interface{}
		Month      interface{}
		MonthName  interface{}
		DayOfMonth interface{}
		DayOfWeek  interface{}
		DayOfYear  interface{}
		WeekDay    interface{}
		DayName    interface{}
		Week       interface{}
		WeekOfYear interface{}
		YearWeek   interface{}
	}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{"0000-00-00 00:00:00", 0, 0, nil, 0, nil, nil, nil, nil, nil, nil, nil},
		{"0000-00-00", 0, 0, nil, 0, nil, nil, nil, nil, nil, nil, nil},
	}

	dtblNil := tblToDtbl(tblNil)
	for _, t := range dtblNil {
		fc := funcs[ast.Year]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		fc = funcs[ast.Month]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		fc = funcs[ast.MonthName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		fc = funcs[ast.DayOfMonth]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		fc = funcs[ast.DayOfWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		fc = funcs[ast.DayOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		fc = funcs[ast.Weekday]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		fc = funcs[ast.DayName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		fc = funcs[ast.Week]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		fc = funcs[ast.WeekOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		fc = funcs[ast.YearWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["YearWeek"][0])
	}

	// test nil with 'NO_ZERO_DATE' set in sql_mode
	tblNil = []struct {
		Input      interface{}
		Year       interface{}
		Month      interface{}
		MonthName  interface{}
		DayOfMonth interface{}
		DayOfWeek  interface{}
		DayOfYear  interface{}
		WeekDay    interface{}
		DayName    interface{}
		Week       interface{}
		WeekOfYear interface{}
		YearWeek   interface{}
	}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{"0000-00-00 00:00:00", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{"0000-00-00", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
	}

	dtblNil = tblToDtbl(tblNil)
	s.ctx.GetSessionVars().SetSystemVar("sql_mode", "NO_ZERO_DATE")
	for _, t := range dtblNil {
		fc := funcs[ast.Year]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		fc = funcs[ast.Month]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		fc = funcs[ast.MonthName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		fc = funcs[ast.DayOfMonth]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		fc = funcs[ast.DayOfWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		fc = funcs[ast.DayOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		fc = funcs[ast.Weekday]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		fc = funcs[ast.DayName]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		fc = funcs[ast.Week]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		fc = funcs[ast.WeekOfYear]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		fc = funcs[ast.YearWeek]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["YearWeek"][0])
	}
}

func (s *testEvaluatorSuite) TestMonthName(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	defer testleak.AfterTest(c)()
	cases := []struct {
		args     interface{}
		expected string
		isNil    bool
		getErr   bool
	}{
		{"2017-12-01", "December", false, false},
		{"2017-00-01", "", true, false},
		{"0000-00-00", "", true, false},
		{"0000-00-00 00:00:00.000000", "", true, false},
		{"0000-00-00 00:00:11.000000", "", true, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.MonthName, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.MonthName].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDayName(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	defer testleak.AfterTest(c)()
	cases := []struct {
		args     interface{}
		expected string
		isNil    bool
		getErr   bool
	}{
		{"2017-12-01", "Friday", false, false},
		{"0000-12-01", "Friday", false, false},
		{"2017-00-01", "", true, false},
		{"2017-01-00", "", true, false},
		{"0000-00-00", "", true, false},
		{"0000-00-00 00:00:00.000000", "", true, false},
		{"0000-00-00 00:00:11.000000", "", true, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.DayName, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.DayName].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDayOfWeek(c *C) {
	defer testleak.AfterTest(c)()
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	cases := []struct {
		args     interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"2017-12-01", 6, false, false},
		{"0000-00-00", 1, true, false},
		{"2018-00-00", 1, true, false},
		{"2017-00-00 12:12:12", 1, true, false},
		{"0000-00-00 12:12:12", 1, true, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.DayOfWeek, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.DayOfWeek].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDayOfMonth(c *C) {
	defer testleak.AfterTest(c)()
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	cases := []struct {
		args     interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"2017-12-01", 1, false, false},
		{"0000-00-00", 1, true, false},
		{"2018-00-00", 0, false, false},
		{"2017-00-00 12:12:12", 0, false, false},
		{"0000-00-00 12:12:12", 0, false, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.DayOfMonth, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.DayOfMonth].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDayOfYear(c *C) {
	defer testleak.AfterTest(c)()
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	cases := []struct {
		args     interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"2017-12-01", 335, false, false},
		{"0000-00-00", 1, true, false},
		{"2018-00-00", 0, true, false},
		{"2017-00-00 12:12:12", 0, true, false},
		{"0000-00-00 12:12:12", 0, true, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.DayOfYear, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.DayOfYear].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestDateFormat(c *C) {
	defer testleak.AfterTest(c)()

	// Test case for https://github.com/pingcap/tidb/issues/2908
	// SELECT DATE_FORMAT(null,'%Y-%M-%D')
	args := []types.Datum{types.NewDatum(nil), types.NewStringDatum("%Y-%M-%D")}
	fc := funcs[ast.DateFormat]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), Equals, true)

	tblDate := []struct {
		Input  []string
		Expect interface{}
	}{
		{[]string{"2010-01-07 23:12:34.12345",
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`},
			`Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`},
		{[]string{"2012-12-21 23:12:34.123456",
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`},
			"Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %"},
		{[]string{"0000-01-01 00:00:00.123456",
			// Functions week() and yearweek() don't support multi mode,
			// so the result of "%U %u %V %Y" is different from MySQL.
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%`},
			`Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 4294967295 0000 00 %`},
		{[]string{"2016-09-3 00:59:59.123456",
			`abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`},
			`abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`},
		{[]string{"2012-10-01 00:00:00",
			`%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%`},
			`Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`},
	}
	dtblDate := tblToDtbl(tblDate)
	for i, t := range dtblDate {
		fc := funcs[ast.DateFormat]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Expect"][0], Commentf(`no.%d \nobtain:%v \nexpect:%v\n`, i,
			v.GetValue(), t["Expect"][0].GetValue()))
	}
}

func (s *testEvaluatorSuite) TestClock(c *C) {
	defer testleak.AfterTest(c)()
	// test hour, minute, second, micro second

	tbl := []struct {
		Input       string
		Hour        int64
		Minute      int64
		Second      int64
		MicroSecond int64
		Time        string
	}{
		{"10:10:10.123456", 10, 10, 10, 123456, "10:10:10.123456"},
		{"11:11:11.11", 11, 11, 11, 110000, "11:11:11.11"},
		{"2010-10-10 11:11:11.11", 11, 11, 11, 110000, "11:11:11.11"},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		fc := funcs[ast.Hour]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Hour"][0])

		fc = funcs[ast.Minute]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Minute"][0])

		fc = funcs[ast.Second]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Second"][0])

		fc = funcs[ast.MicroSecond]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MicroSecond"][0])

		fc = funcs[ast.Time]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Time"][0])
	}

	// nil
	fc := funcs[ast.Hour]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Minute]
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Second]
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.MicroSecond]
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Time]
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	// test error
	errTbl := []string{
		"2011-11-11T10:10:10.11",
		"2011-11-11 10:10:10.11.12",
	}

	for _, t := range errTbl {
		td := types.MakeDatums(t)
		fc := funcs[ast.Hour]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(td))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		fc = funcs[ast.Minute]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(td))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		fc = funcs[ast.Second]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(td))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		fc = funcs[ast.MicroSecond]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(td))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)

		fc = funcs[ast.Time]
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(td))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestTime(c *C) {
	defer testleak.AfterTest(c)()

	cases := []struct {
		args     interface{}
		expected string
		isNil    bool
		getErr   bool
	}{
		{"2003-12-31 01:02:03", "01:02:03", false, false},
		{"2003-12-31 01:02:03.000123", "01:02:03.000123", false, false},
		{"01:02:03.000123", "01:02:03.000123", false, false},
		{"01:02:03", "01:02:03", false, false},
		{"-838:59:59.000000", "-838:59:59.000000", false, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Time, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		tp := f.GetType()
		c.Assert(tp.Tp, Equals, mysql.TypeDuration)
		c.Assert(tp.Charset, Equals, charset.CharsetBin)
		c.Assert(tp.Collate, Equals, charset.CollationBin)
		c.Assert(tp.Flag&uint(mysql.BinaryFlag), Equals, uint(mysql.BinaryFlag))
		c.Assert(tp.Flen, Equals, mysql.MaxDurationWidthWithFsp)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetMysqlDuration().String(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.Time].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func resetStmtContext(ctx sessionctx.Context) {
	ctx.GetSessionVars().StmtCtx.ResetNowTs()
}

func (s *testEvaluatorSuite) TestNowAndUTCTimestamp(c *C) {
	defer testleak.AfterTest(c)()

	gotime := func(t types.Time, l *time.Location) time.Time {
		tt, err := t.Time.GoTime(l)
		c.Assert(err, IsNil)
		return tt
	}

	for _, x := range []struct {
		fc  functionClass
		now func() time.Time
	}{
		{funcs[ast.Now], func() time.Time { return time.Now() }},
		{funcs[ast.UTCTimestamp], func() time.Time { return time.Now().UTC() }},
	} {
		f, err := x.fc.getFunction(s.ctx, s.datumsToConstants(nil))
		c.Assert(err, IsNil)
		resetStmtContext(s.ctx)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		ts := x.now()
		c.Assert(err, IsNil)
		t := v.GetMysqlTime()
		// we canot use a constant value to check timestamp funcs, so here
		// just to check the fractional seconds part and the time delta.
		c.Assert(strings.Contains(t.String(), "."), IsFalse)
		c.Assert(ts.Sub(gotime(t, ts.Location())), LessEqual, time.Second)

		f, err = x.fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(6)))
		c.Assert(err, IsNil)
		resetStmtContext(s.ctx)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		ts = x.now()
		c.Assert(err, IsNil)
		t = v.GetMysqlTime()
		c.Assert(strings.Contains(t.String(), "."), IsTrue)
		c.Assert(ts.Sub(gotime(t, ts.Location())), LessEqual, time.Millisecond)

		resetStmtContext(s.ctx)
		f, err = x.fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(8)))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, NotNil)

		resetStmtContext(s.ctx)
		f, err = x.fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(-2)))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, NotNil)
	}

	// Test that "timestamp" and "time_zone" variable may affect the result of Now() builtin function.
	variable.SetSessionSystemVar(s.ctx.GetSessionVars(), "time_zone", types.NewDatum("+00:00"))
	variable.SetSessionSystemVar(s.ctx.GetSessionVars(), "timestamp", types.NewDatum(1234))
	fc := funcs[ast.Now]
	resetStmtContext(s.ctx)
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(nil))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	result, err := v.ToString()
	c.Assert(err, IsNil)
	c.Assert(result, Equals, "1970-01-01 00:20:34")
	variable.SetSessionSystemVar(s.ctx.GetSessionVars(), "timestamp", types.NewDatum(0))
	variable.SetSessionSystemVar(s.ctx.GetSessionVars(), "time_zone", types.NewDatum("system"))
}

func (s *testEvaluatorSuite) TestIsDuration(c *C) {
	defer testleak.AfterTest(c)
	tbl := []struct {
		Input  string
		expect bool
	}{
		{"110:00:00", true},
		{"aa:bb:cc", false},
		{"1 01:00:00", true},
		{"01:00:00.999999", true},
		{"071231235959.999999", false},
		{"20171231235959.999999", false},
		{"2017-01-01 01:01:01.11", false},
		{"07-12-31 23:59:59.999999", false},
		{"2007-12-31 23:59:59.999999", false},
	}
	for _, t := range tbl {
		result := isDuration(t.Input)
		c.Assert(result, Equals, t.expect)
	}
}

func (s *testEvaluatorSuite) TestAddTimeSig(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input         string
		InputDuration string
		expect        string
	}{
		{"01:00:00.999999", "02:00:00.999998", "03:00:01.999997"},
		{"110:00:00", "1 02:00:00", "136:00:00"},
		{"2017-01-01 01:01:01.11", "01:01:01.11111", "2017-01-01 02:02:02.221110"},
		{"2007-12-31 23:59:59.999999", "1 1:1:1.000002", "2008-01-02 01:01:01.000001"},
		{"2017-12-01 01:01:01.000001", "1 1:1:1.000002", "2017-12-02 02:02:02.000003"},
		{"2017-12-31 23:59:59", "00:00:01", "2018-01-01 00:00:00"},
		{"2017-12-31 23:59:59", "1", "2018-01-01 00:00:00"},
		{"2007-12-31 23:59:59.999999", "2 1:1:1.000002", "2008-01-03 01:01:01.000001"},
		{"2018-08-16 20:21:01", "00:00:00.000001", "2018-08-16 20:21:01.000001"},
	}
	fc := funcs[ast.AddTime]
	for _, t := range tbl {
		tmpInput := types.NewStringDatum(t.Input)
		tmpInputDuration := types.NewStringDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}

	// This is a test for issue 7334
	du := newDateArighmeticalUtil()
	resetStmtContext(s.ctx)
	now, _, err := evalNowWithFsp(s.ctx, 0)
	c.Assert(err, IsNil)
	res, _, err := du.add(s.ctx, now, "1", "MICROSECOND")
	c.Assert(err, IsNil)
	c.Assert(res.Fsp, Equals, 6)

	tbl = []struct {
		Input         string
		InputDuration string
		expect        string
	}{
		{"01:00:00.999999", "02:00:00.999998", "03:00:01.999997"},
		{"23:59:59", "00:00:01", "24:00:00"},
		{"235959", "00:00:01", "24:00:00"},
		{"110:00:00", "1 02:00:00", "136:00:00"},
		{"-110:00:00", "1 02:00:00", "-84:00:00"},
	}
	for _, t := range tbl {
		dur, err := types.ParseDuration(s.ctx.GetSessionVars().StmtCtx, t.Input, types.GetFsp(t.Input))
		c.Assert(err, IsNil)
		tmpInput := types.NewDurationDatum(dur)
		tmpInputDuration := types.NewStringDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}

	tbll := []struct {
		Input         int64
		InputDuration int64
		expect        string
	}{
		{20171010123456, 1, "2017-10-10 12:34:57"},
		{123456, 1, "12:34:57"},
	}
	for _, t := range tbll {
		tmpInput := types.NewIntDatum(t.Input)
		tmpInputDuration := types.NewIntDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}

	tblWarning := []struct {
		Input         interface{}
		InputDuration interface{}
		warning       *terror.Error
	}{
		{"0", "-32073", types.ErrTruncatedWrongVal},
		{"-32073", "0", types.ErrTruncatedWrongVal},
		{types.ZeroDuration, "-32073", types.ErrTruncatedWrongVal},
		{"-32073", types.ZeroDuration, types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeTimestamp), "-32073", types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeDate), "-32073", types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeDatetime), "-32073", types.ErrTruncatedWrongVal},
	}
	for i, t := range tblWarning {
		tmpInput := types.NewDatum(t.Input)
		tmpInputDuration := types.NewDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, "")
		c.Assert(d.IsNull(), Equals, true)
		warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, i+1)
		c.Assert(terror.ErrorEqual(t.warning, warnings[i].Err), IsTrue, Commentf("err %v", warnings[i].Err))
	}
}

func (s *testEvaluatorSuite) TestSubTimeSig(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input         string
		InputDuration string
		expect        string
	}{
		{"01:00:00.999999", "02:00:00.999998", "-00:59:59.999999"},
		{"110:00:00", "1 02:00:00", "84:00:00"},
		{"2017-01-01 01:01:01.11", "01:01:01.11111", "2016-12-31 23:59:59.998890"},
		{"2007-12-31 23:59:59.999999", "1 1:1:1.000002", "2007-12-30 22:58:58.999997"},
	}
	fc := funcs[ast.SubTime]
	for _, t := range tbl {
		tmpInput := types.NewStringDatum(t.Input)
		tmpInputDuration := types.NewStringDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}

	tbl = []struct {
		Input         string
		InputDuration string
		expect        string
	}{
		{"03:00:00.999999", "02:00:00.999998", "01:00:00.000001"},
		{"23:59:59", "00:00:01", "23:59:58"},
		{"235959", "00:00:01", "23:59:58"},
	}
	for _, t := range tbl {
		dur, err := types.ParseDuration(s.ctx.GetSessionVars().StmtCtx, t.Input, types.GetFsp(t.Input))
		c.Assert(err, IsNil)
		tmpInput := types.NewDurationDatum(dur)
		tmpInputDuration := types.NewStringDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}
	tbll := []struct {
		Input         int64
		InputDuration int64
		expect        string
	}{
		{20171010123456, 1, "2017-10-10 12:34:55"},
		{123456, 1, "12:34:55"},
	}
	for _, t := range tbll {
		tmpInput := types.NewIntDatum(t.Input)
		tmpInputDuration := types.NewIntDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, t.expect)
	}

	tblWarning := []struct {
		Input         interface{}
		InputDuration interface{}
		warning       *terror.Error
	}{
		{"0", "-32073", types.ErrTruncatedWrongVal},
		{"-32073", "0", types.ErrTruncatedWrongVal},
		{types.ZeroDuration, "-32073", types.ErrTruncatedWrongVal},
		{"-32073", types.ZeroDuration, types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeTimestamp), "-32073", types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeDate), "-32073", types.ErrTruncatedWrongVal},
		{types.CurrentTime(mysql.TypeDatetime), "-32073", types.ErrTruncatedWrongVal},
	}
	for i, t := range tblWarning {
		tmpInput := types.NewDatum(t.Input)
		tmpInputDuration := types.NewDatum(t.InputDuration)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{tmpInput, tmpInputDuration}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, "")
		c.Assert(d.IsNull(), Equals, true)
		warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
		c.Assert(len(warnings), Equals, i+1)
		c.Assert(terror.ErrorEqual(t.warning, warnings[i].Err), IsTrue, Commentf("err %v", warnings[i].Err))
	}
}

func (s *testEvaluatorSuite) TestSysDate(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Sysdate]

	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.TimeZone = timeutil.SystemLocation()
	timezones := []types.Datum{types.NewDatum(1234), types.NewDatum(0)}
	for _, timezone := range timezones {
		// sysdate() result is not affected by "timestamp" session variable.
		variable.SetSessionSystemVar(ctx.GetSessionVars(), "timestamp", timezone)
		f, err := fc.getFunction(ctx, s.datumsToConstants(nil))
		c.Assert(err, IsNil)
		resetStmtContext(s.ctx)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		last := time.Now()
		c.Assert(err, IsNil)
		n := v.GetMysqlTime()
		c.Assert(n.String(), GreaterEqual, last.Format(types.TimeFormat))
	}

	last := time.Now()
	f, err := fc.getFunction(ctx, s.datumsToConstants(types.MakeDatums(6)))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.TimeFormat))

	f, err = fc.getFunction(ctx, s.datumsToConstants(types.MakeDatums(-2)))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, NotNil)
}

func convertToTimeWithFsp(sc *stmtctx.StatementContext, arg types.Datum, tp byte, fsp int) (d types.Datum, err error) {
	if fsp > types.MaxFsp {
		fsp = types.MaxFsp
	}

	f := types.NewFieldType(tp)
	f.Decimal = fsp

	d, err = arg.ConvertTo(sc, f)
	if err != nil {
		d.SetNull()
		return d, err
	}

	if d.IsNull() {
		return
	}

	if d.Kind() != types.KindMysqlTime {
		d.SetNull()
		return d, errors.Errorf("need time type, but got %T", d.GetValue())
	}
	return
}

func convertToTime(sc *stmtctx.StatementContext, arg types.Datum, tp byte) (d types.Datum, err error) {
	return convertToTimeWithFsp(sc, arg, tp, types.MaxFsp)
}

func builtinDateFormat(ctx sessionctx.Context, args []types.Datum) (d types.Datum, err error) {
	date, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDatetime)
	if err != nil {
		return d, err
	}

	if date.IsNull() {
		return
	}
	t := date.GetMysqlTime()
	str, err := t.DateFormat(args[1].GetString())
	if err != nil {
		return d, err
	}
	d.SetString(str)
	return
}

func (s *testEvaluatorSuite) TestFromUnixTime(c *C) {
	defer testleak.AfterTest(c)()

	tbl := []struct {
		isDecimal      bool
		integralPart   int64
		fractionalPart int64
		decimal        float64
		format         string
		expect         string
	}{
		{false, 1451606400, 0, 0, "", "2016-01-01 00:00:00"},
		{true, 1451606400, 123456000, 1451606400.123456, "", "2016-01-01 00:00:00.123456"},
		{true, 1451606400, 999999000, 1451606400.999999, "", "2016-01-01 00:00:00.999999"},
		{true, 1451606400, 999999900, 1451606400.9999999, "", "2016-01-01 00:00:01.000000"},
		{false, 1451606400, 0, 0, `%Y %D %M %h:%i:%s %x`, "2016-01-01 00:00:00"},
		{true, 1451606400, 123456000, 1451606400.123456, `%Y %D %M %h:%i:%s %x`, "2016-01-01 00:00:00.123456"},
		{true, 1451606400, 999999000, 1451606400.999999, `%Y %D %M %h:%i:%s %x`, "2016-01-01 00:00:00.999999"},
		{true, 1451606400, 999999900, 1451606400.9999999, `%Y %D %M %h:%i:%s %x`, "2016-01-01 00:00:01.000000"},
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	originTZ := sc.TimeZone
	sc.TimeZone = time.UTC
	defer func() {
		sc.TimeZone = originTZ
	}()
	fc := funcs[ast.FromUnixTime]
	for _, t := range tbl {
		var timestamp types.Datum
		if !t.isDecimal {
			timestamp.SetInt64(t.integralPart)
		} else {
			timestamp.SetFloat64(t.decimal)
		}
		// result of from_unixtime() is dependent on specific time zone.
		if len(t.format) == 0 {
			constants := s.datumsToConstants([]types.Datum{timestamp})
			if !t.isDecimal {
				constants[0].GetType().Decimal = 0
			}

			f, err := fc.getFunction(s.ctx, constants)
			c.Assert(err, IsNil)

			v, err := evalBuiltinFunc(f, chunk.Row{})
			c.Assert(err, IsNil)
			ans := v.GetMysqlTime()
			c.Assert(ans.String(), Equals, t.expect, Commentf("%+v", t))
		} else {
			format := types.NewStringDatum(t.format)
			constants := s.datumsToConstants([]types.Datum{timestamp, format})
			if !t.isDecimal {
				constants[0].GetType().Decimal = 0
			}
			f, err := fc.getFunction(s.ctx, constants)
			c.Assert(err, IsNil)
			v, err := evalBuiltinFunc(f, chunk.Row{})
			c.Assert(err, IsNil)
			result, err := builtinDateFormat(s.ctx, []types.Datum{types.NewStringDatum(t.expect), format})
			c.Assert(err, IsNil)
			c.Assert(v.GetString(), Equals, result.GetString(), Commentf("%+v", t))
		}
	}

	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(-12345)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(math.MaxInt32+1)))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestCurrentDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now()
	fc := funcs[ast.CurrentDate]
	f, err := fc.getFunction(mock.NewContext(), s.datumsToConstants(nil))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.DateFormat))
}

func (s *testEvaluatorSuite) TestCurrentTime(c *C) {
	defer testleak.AfterTest(c)()
	tfStr := "15:04:05"

	last := time.Now()
	fc := funcs[ast.CurrentTime]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n := v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(3)))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 12)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(6)))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 15)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	_, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(-1)))
	c.Assert(err, NotNil)

	_, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(7)))
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestUTCTime(c *C) {
	defer testleak.AfterTest(c)()

	last := time.Now().UTC()
	tfStr := "00:00:00"
	fc := funcs[ast.UTCTime]

	tests := []struct {
		param  interface{}
		expect int
	}{{0, 8}, {3, 12}, {6, 15}, {-1, 0}, {7, 0}}

	for _, test := range tests {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(test.param)))
		c.Assert(err, IsNil)
		resetStmtContext(s.ctx)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		if test.expect > 0 {
			c.Assert(err, IsNil)
			n := v.GetMysqlDuration()
			c.Assert(n.String(), HasLen, test.expect)
			c.Assert(n.String(), GreaterEqual, last.Format(tfStr))
		} else {
			c.Assert(err, NotNil)
		}
	}

	f, err := fc.getFunction(s.ctx, make([]Expression, 0))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n := v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))
}

func (s *testEvaluatorSuite) TestUTCDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now().UTC()
	fc := funcs[ast.UTCDate]
	f, err := fc.getFunction(mock.NewContext(), s.datumsToConstants(nil))
	c.Assert(err, IsNil)
	resetStmtContext(mock.NewContext())
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.DateFormat))
}

func (s *testEvaluatorSuite) TestStrToDate(c *C) {
	tests := []struct {
		Date    string
		Format  string
		Success bool
		Expect  time.Time
	}{
		{"10/28/2011 9:46:29 pm", "%m/%d/%Y %l:%i:%s %p", true, time.Date(2011, 10, 28, 21, 46, 29, 0, time.Local)},
		{"10/28/2011 9:46:29 Pm", "%m/%d/%Y %l:%i:%s %p", true, time.Date(2011, 10, 28, 21, 46, 29, 0, time.Local)},
		{"2011/10/28 9:46:29 am", "%Y/%m/%d %l:%i:%s %p", true, time.Date(2011, 10, 28, 9, 46, 29, 0, time.Local)},
		{"20161122165022", `%Y%m%d%H%i%s`, true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"2016 11 22 16 50 22", `%Y%m%d%H%i%s`, true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"16-50-22 2016 11 22", `%H-%i-%s%Y%m%d`, true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"16-50 2016 11 22", `%H-%i-%s%Y%m%d`, false, time.Time{}},
		{"15-01-2001 1:59:58.999", "%d-%m-%Y %I:%i:%s.%f", true, time.Date(2001, 1, 15, 1, 59, 58, 999000000, time.Local)},
		{"15-01-2001 1:59:58.1", "%d-%m-%Y %H:%i:%s.%f", true, time.Date(2001, 1, 15, 1, 59, 58, 100000000, time.Local)},
		{"15-01-2001 1:59:58.", "%d-%m-%Y %H:%i:%s.%f", true, time.Date(2001, 1, 15, 1, 59, 58, 000000000, time.Local)},
		{"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%s.%f", true, time.Date(2001, 1, 15, 1, 9, 8, 999000000, time.Local)},
		{"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%S.%f", true, time.Date(2001, 1, 15, 1, 9, 8, 999000000, time.Local)},
		{"2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", false, time.Time{}},
		{"10:20:10AM", "%H:%i:%S%p", false, time.Time{}},
	}

	fc := funcs[ast.StrToDate]
	for _, test := range tests {
		date := types.NewStringDatum(test.Date)
		format := types.NewStringDatum(test.Format)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{date, format}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if !test.Success {
			c.Assert(err, IsNil)
			c.Assert(result.IsNull(), IsTrue)
			continue
		}
		c.Assert(result.Kind(), Equals, types.KindMysqlTime)
		value := result.GetMysqlTime()
		t1, _ := value.Time.GoTime(time.Local)
		c.Assert(t1, Equals, test.Expect)
	}
}

func (s *testEvaluatorSuite) TestFromDays(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()
	tests := []struct {
		day    int64
		expect string
	}{
		{-140, "0000-00-00"},   // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
		{140, "0000-00-00"},    // mysql FROM_DAYS returns 0000-00-00 for any day <= 365.
		{735000, "2012-05-12"}, // Leap year.
		{735030, "2012-06-11"},
		{735130, "2012-09-19"},
		{734909, "2012-02-11"},
		{734878, "2012-01-11"},
		{734927, "2012-02-29"},
		{734634, "2011-05-12"}, // Non Leap year.
		{734664, "2011-06-11"},
		{734764, "2011-09-19"},
		{734544, "2011-02-11"},
		{734513, "2011-01-11"},
	}

	fc := funcs[ast.FromDays]
	for _, test := range tests {
		t1 := types.NewIntDatum(test.day)

		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{t1}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})

		c.Assert(err, IsNil)
		c.Assert(result.GetMysqlTime().String(), Equals, test.expect)
	}

	stringTests := []struct {
		day    string
		expect string
	}{
		{"z550z", "0000-00-00"},
		{"6500z", "0017-10-18"},
		{"440", "0001-03-16"},
	}

	for _, test := range stringTests {
		t1 := types.NewStringDatum(test.day)

		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{t1}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})

		c.Assert(err, IsNil)
		c.Assert(result.GetMysqlTime().String(), Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestDateDiff(c *C) {
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
	tests := []struct {
		t1     string
		t2     string
		expect int64
	}{
		{"2004-05-21", "2004:01:02", 140},
		{"2004-04-21", "2000:01:02", 1571},
		{"2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002", 1},
		{"1010-11-30 23:59:59", "2010-12-31", -365274},
		{"1010-11-30", "2210-11-01", -438262},
	}

	fc := funcs[ast.DateDiff]
	for _, test := range tests {
		t1 := types.NewStringDatum(test.t1)
		t2 := types.NewStringDatum(test.t2)

		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{t1, t2}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})

		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	// Test invalid time format.
	tests2 := []struct {
		t1 string
		t2 string
	}{
		{"2004-05-21", "abcdefg"},
		{"2007-12-31 23:59:59", "23:59:59"},
		{"2007-00-31 23:59:59", "2016-01-13"},
		{"2007-10-31 23:59:59", "2016-01-00"},
		{"2007-10-31 23:59:59", "99999999-01-00"},
	}

	fc = funcs[ast.DateDiff]
	for _, test := range tests2 {
		t1 := types.NewStringDatum(test.t1)
		t2 := types.NewStringDatum(test.t2)

		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{t1, t2}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.IsNull(), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestTimeDiff(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
	tests := []struct {
		args      []interface{}
		expectStr string
		isNil     bool
		fsp       int
		getErr    bool
	}{
		{[]interface{}{"2000:01:01 00:00:00", "2000:01:01 00:00:00.000001"}, "-00:00:00.000001", false, 6, false},
		{[]interface{}{"2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002"}, "46:58:57.999999", false, 6, false},
		{[]interface{}{"2016-12-00 12:00:00", "2016-12-01 12:00:00"}, "-24:00:00", false, 0, false},
		{[]interface{}{"10:10:10", "10:9:0"}, "00:01:10", false, 0, false},
		{[]interface{}{"2016-12-00 12:00:00", "10:9:0"}, "", true, 0, false},
		{[]interface{}{"2016-12-00 12:00:00", ""}, "", true, 0, false},
	}

	for _, t := range tests {
		f, err := newFunctionForTest(s.ctx, ast.TimeDiff, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		tp := f.GetType()
		c.Assert(tp.Tp, Equals, mysql.TypeDuration)
		c.Assert(tp.Charset, Equals, charset.CharsetBin)
		c.Assert(tp.Collate, Equals, charset.CollationBin)
		c.Assert(tp.Flag, Equals, uint(mysql.BinaryFlag))
		c.Assert(tp.Flen, Equals, mysql.MaxDurationWidthWithFsp)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetMysqlDuration().String(), Equals, t.expectStr)
				c.Assert(d.GetMysqlDuration().Fsp, Equals, t.fsp)
			}
		}
	}
	_, err := funcs[ast.TimeDiff].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestWeek(c *C) {
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
	tests := []struct {
		t      string
		mode   int64
		expect int64
	}{
		{"2008-02-20", 0, 7},
		{"2008-02-20", 1, 8},
		{"2008-12-31", 1, 53},
	}
	fc := funcs[ast.Week]
	for _, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		arg2 := types.NewIntDatum(test.mode)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{arg1, arg2}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestWeekWithoutModeSig(c *C) {
	tests := []struct {
		t      string
		expect int64
	}{
		{"2008-02-20", 7},
		{"2000-12-31", 53},
		{"2000-12-31", 1}, //set default week mode
		{"2005-12-3", 48}, //set default week mode
		{"2008-02-20", 7},
	}

	fc := funcs[ast.Week]
	for i, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{arg1}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
		if i == 1 {
			s.ctx.GetSessionVars().SetSystemVar("default_week_format", "6")
		} else if i == 3 {
			s.ctx.GetSessionVars().SetSystemVar("default_week_format", "")
		}
	}
}
func (s *testEvaluatorSuite) TestYearWeek(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
	tests := []struct {
		t      string
		mode   int64
		expect int64
	}{
		{"1987-01-01", 0, 198652},
		{"2000-01-01", 0, 199952},
	}
	fc := funcs[ast.YearWeek]
	for _, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		arg2 := types.NewIntDatum(test.mode)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{arg1, arg2}))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("2016-00-05")))
	c.Assert(err, IsNil)
	result, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(result.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestTimestampDiff(c *C) {
	tests := []struct {
		unit   string
		t1     string
		t2     string
		expect int64
	}{
		{"MONTH", "2003-02-01", "2003-05-01", 3},
		{"YEAR", "2002-05-01", "2001-01-01", -1},
		{"MINUTE", "2003-02-01", "2003-05-01 12:05:55", 128885},
	}

	fc := funcs[ast.TimestampDiff]
	for _, test := range tests {
		args := []types.Datum{
			types.NewStringDatum(test.unit),
			types.NewStringDatum(test.t1),
			types.NewStringDatum(test.t2),
		}
		resetStmtContext(s.ctx)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, test.expect)
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = true
	sc.IgnoreZeroInDate = true
	resetStmtContext(s.ctx)
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{types.NewStringDatum("DAY"),
		types.NewStringDatum("2017-01-00"),
		types.NewStringDatum("2017-01-01")}))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	resetStmtContext(s.ctx)
	f, err = fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{types.NewStringDatum("DAY"),
		{}, types.NewStringDatum("2017-01-01")}))
	c.Assert(err, IsNil)
	d, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestUnixTimestamp(c *C) {
	// Test UNIX_TIMESTAMP().
	fc := funcs[ast.UnixTimestamp]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64()-time.Now().Unix(), GreaterEqual, int64(-1))
	c.Assert(d.GetInt64()-time.Now().Unix(), LessEqual, int64(1))

	// https://github.com/pingcap/tidb/issues/2496
	// Test UNIX_TIMESTAMP(NOW()).
	resetStmtContext(s.ctx)
	now, isNull, err := evalNowWithFsp(s.ctx, 0)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	n := types.Datum{}
	n.SetMysqlTime(now)
	args := []types.Datum{n}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	resetStmtContext(s.ctx)
	d, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	val, _ := d.GetMysqlDecimal().ToInt()
	c.Assert(val-time.Now().Unix(), GreaterEqual, int64(-1))
	c.Assert(val-time.Now().Unix(), LessEqual, int64(1))

	// https://github.com/pingcap/tidb/issues/2852
	// Test UNIX_TIMESTAMP(NULL).
	args = []types.Datum{types.NewDatum(nil)}
	resetStmtContext(s.ctx)
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	d, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), Equals, true)

	// Set the time_zone variable, because UnixTimestamp() result depends on it.
	s.ctx.GetSessionVars().TimeZone = time.UTC
	s.ctx.GetSessionVars().StmtCtx.IgnoreZeroInDate = true
	tests := []struct {
		inputDecimal int
		input        types.Datum
		expectKind   byte
		expect       string
	}{
		{0, types.NewIntDatum(151113), types.KindInt64, "1447372800"}, // YYMMDD
		// TODO: Uncomment the line below after fixing #4232
		// {5, types.NewFloat64Datum(151113.12345), types.KindMysqlDecimal, "1447372800.00000"},                                           // YYMMDD
		{0, types.NewIntDatum(20151113), types.KindInt64, "1447372800"}, // YYYYMMDD
		// TODO: Uncomment the line below after fixing #4232
		// {5, types.NewFloat64Datum(20151113.12345), types.KindMysqlDecimal, "1447372800.00000"},                                         // YYYYMMDD
		{0, types.NewIntDatum(151113102019), types.KindInt64, "1447410019"},                                                            // YYMMDDHHMMSS
		{0, types.NewFloat64Datum(151113102019), types.KindInt64, "1447410019"},                                                        // YYMMDDHHMMSS
		{2, types.NewFloat64Datum(151113102019.12), types.KindMysqlDecimal, "1447410019.12"},                                           // YYMMDDHHMMSS
		{0, types.NewDecimalDatum(types.NewDecFromStringForTest("151113102019")), types.KindInt64, "1447410019"},                       // YYMMDDHHMMSS
		{2, types.NewDecimalDatum(types.NewDecFromStringForTest("151113102019.12")), types.KindMysqlDecimal, "1447410019.12"},          // YYMMDDHHMMSS
		{7, types.NewDecimalDatum(types.NewDecFromStringForTest("151113102019.1234567")), types.KindMysqlDecimal, "1447410019.123457"}, // YYMMDDHHMMSS
		{0, types.NewIntDatum(20151113102019), types.KindInt64, "1447410019"},                                                          // YYYYMMDDHHMMSS
		{0, types.NewStringDatum("2015-11-13 10:20:19"), types.KindInt64, "1447410019"},
		{0, types.NewStringDatum("2015-11-13 10:20:19.012"), types.KindMysqlDecimal, "1447410019.012"},
		{0, types.NewStringDatum("1970-01-01 00:00:00"), types.KindInt64, "0"},                               // Min timestamp
		{0, types.NewStringDatum("2038-01-19 03:14:07.999999"), types.KindMysqlDecimal, "2147483647.999999"}, // Max timestamp
		{0, types.NewStringDatum("2017-00-02"), types.KindInt64, "0"},                                        // Invalid date
		{0, types.NewStringDatum("1969-12-31 23:59:59.999999"), types.KindMysqlDecimal, "0"},                 // Invalid timestamp
		{0, types.NewStringDatum("2038-01-19 03:14:08"), types.KindInt64, "0"},                               // Invalid timestamp
		// Below tests irregular inputs.
		//{0, types.NewIntDatum(0), types.KindInt64, "0"},
		//{0, types.NewIntDatum(-1), types.KindInt64, "0"},
		//{0, types.NewIntDatum(12345), types.KindInt64, "0"},
	}

	for _, test := range tests {
		expr := s.datumsToConstants([]types.Datum{test.input})
		expr[0].GetType().Decimal = test.inputDecimal
		resetStmtContext(s.ctx)
		f, err := fc.getFunction(s.ctx, expr)
		c.Assert(err, IsNil, Commentf("%+v", test))
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil, Commentf("%+v", test))
		c.Assert(d.Kind(), Equals, test.expectKind, Commentf("%+v", test))
		str, err := d.ToString()
		c.Assert(err, IsNil, Commentf("%+v", test))
		c.Assert(str, Equals, test.expect, Commentf("%+v", test))
	}
}

func (s *testEvaluatorSuite) TestDateArithFuncs(c *C) {
	defer testleak.AfterTest(c)()

	date := []string{"2016-12-31", "2017-01-01"}
	fcAdd := funcs[ast.DateAdd]
	fcSub := funcs[ast.DateSub]

	tests := []struct {
		inputDate    string
		fc           functionClass
		inputDecimal float64
		expect       string
	}{
		{date[0], fcAdd, 1, date[1]},
		{date[1], fcAdd, -1, date[0]},
		{date[1], fcAdd, -0.5, date[0]},
		{date[1], fcAdd, -1.4, date[0]},

		{date[1], fcSub, 1, date[0]},
		{date[0], fcSub, -1, date[1]},
		{date[0], fcSub, -0.5, date[1]},
		{date[0], fcSub, -1.4, date[1]},
	}
	for _, test := range tests {
		args := types.MakeDatums(test.inputDate, test.inputDecimal, "DAY")
		f, err := test.fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetMysqlTime().String(), Equals, test.expect)
	}

	args := types.MakeDatums(date[0], nil, "DAY")
	f, err := fcAdd.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	args = types.MakeDatums(date[1], nil, "DAY")
	f, err = fcSub.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	testMonths := []struct {
		input    string
		months   int
		expected string
	}{
		{"1900-01-31", 1, "1900-02-28"},
		{"2000-01-31", 1, "2000-02-29"},
		{"2016-01-31", 1, "2016-02-29"},
		{"2018-07-31", 1, "2018-08-31"},
		{"2018-08-31", 1, "2018-09-30"},
		{"2018-07-31", 2, "2018-09-30"},
		{"2016-01-31", 27, "2018-04-30"},
		{"2000-02-29", 12, "2001-02-28"},
		{"2000-11-30", 1, "2000-12-30"},
	}

	for _, test := range testMonths {
		args = types.MakeDatums(test.input, test.months, "MONTH")
		f, err = fcAdd.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetMysqlTime().String(), Equals, test.expected)
	}

	testYears := []struct {
		input    string
		year     int
		expected string
	}{
		{"1899-02-28", 1, "1900-02-28"},
		{"1901-02-28", -1, "1900-02-28"},
		{"2000-02-29", 1, "2001-02-28"},
		{"2001-02-28", -1, "2000-02-28"},
		{"2004-02-29", 1, "2005-02-28"},
		{"2005-02-28", -1, "2004-02-28"},
	}

	for _, test := range testYears {
		args = types.MakeDatums(test.input, test.year, "YEAR")
		f, err = fcAdd.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetMysqlTime().String(), Equals, test.expected)
	}

	testOverflowYears := []struct {
		input string
		year  int
	}{
		{"2008-11-23", -1465647104},
		{"2008-11-23", 1465647104},
	}

	for _, test := range testOverflowYears {
		args = types.MakeDatums(test.input, test.year, "YEAR")
		f, err = fcAdd.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.IsNull(), IsTrue)
	}

	for _, test := range testOverflowYears {
		args = types.MakeDatums(test.input, test.year, "YEAR")
		f, err = fcSub.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.IsNull(), IsTrue)
	}

	testDurations := []struct {
		fc       functionClass
		dur      string
		fsp      int
		unit     string
		format   interface{}
		expected string
	}{
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "MICROSECOND",
			format:   "100",
			expected: "00:00:00.000100",
		},
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "MICROSECOND",
			format:   100.0,
			expected: "00:00:00.000100",
		},
		{
			fc:       fcSub,
			dur:      "00:00:01",
			fsp:      0,
			unit:     "MICROSECOND",
			format:   "100",
			expected: "00:00:00.999900",
		},
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   "1",
			expected: "24:00:00",
		},
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "SECOND",
			format:   1,
			expected: "00:00:01",
		},
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   types.NewDecFromInt(1),
			expected: "24:00:00",
		},
		{
			fc:       fcAdd,
			dur:      "00:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   1.0,
			expected: "24:00:00",
		},
		{
			fc:       fcSub,
			dur:      "26:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   "1",
			expected: "02:00:00",
		},
		{
			fc:       fcSub,
			dur:      "26:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   1,
			expected: "02:00:00",
		},
		{
			fc:       fcSub,
			dur:      "26:00:00",
			fsp:      0,
			unit:     "SECOND",
			format:   types.NewDecFromInt(1),
			expected: "25:59:59",
		},
		{
			fc:       fcSub,
			dur:      "27:00:00",
			fsp:      0,
			unit:     "DAY",
			format:   1.0,
			expected: "03:00:00",
		},
	}
	for _, tt := range testDurations {
		dur, _, ok, err := types.StrToDuration(nil, tt.dur, tt.fsp)
		c.Assert(err, IsNil)
		c.Assert(ok, IsTrue)
		args = types.MakeDatums(dur, tt.format, tt.unit)
		f, err = tt.fc.getFunction(s.ctx, s.datumsToConstants(args))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetMysqlDuration().String(), Equals, tt.expected)
	}
}

func (s *testEvaluatorSuite) TestTimestamp(c *C) {
	tests := []struct {
		t      []types.Datum
		expect string
	}{
		// one argument
		{[]types.Datum{types.NewStringDatum("2017-01-18")}, "2017-01-18 00:00:00"},
		{[]types.Datum{types.NewStringDatum("20170118")}, "2017-01-18 00:00:00"},
		{[]types.Datum{types.NewStringDatum("170118")}, "2017-01-18 00:00:00"},
		{[]types.Datum{types.NewStringDatum("20170118123056")}, "2017-01-18 12:30:56"},
		{[]types.Datum{types.NewStringDatum("2017-01-18 12:30:56")}, "2017-01-18 12:30:56"},
		{[]types.Datum{types.NewIntDatum(170118)}, "2017-01-18 00:00:00"},
		{[]types.Datum{types.NewFloat64Datum(20170118)}, "2017-01-18 00:00:00"},
		{[]types.Datum{types.NewStringDatum("20170118123050.999")}, "2017-01-18 12:30:50.999"},
		{[]types.Datum{types.NewStringDatum("20170118123050.1234567")}, "2017-01-18 12:30:50.123457"},
		// TODO: Parse int should use ParseTimeFromNum, rather than convert int to string for parsing.
		// {[]types.Datum{types.NewIntDatum(11111111111)}, "2001-11-11 11:11:11"},
		{[]types.Datum{types.NewStringDatum("11111111111")}, "2011-11-11 11:11:01"},
		{[]types.Datum{types.NewFloat64Datum(20170118.999)}, "2017-01-18 00:00:00.000"},

		// two arguments
		{[]types.Datum{types.NewStringDatum("2017-01-18"), types.NewStringDatum("12:30:59")}, "2017-01-18 12:30:59"},
		{[]types.Datum{types.NewStringDatum("2017-01-18"), types.NewStringDatum("12:30:59")}, "2017-01-18 12:30:59"},
		{[]types.Datum{types.NewStringDatum("2017-01-18 01:01:01"), types.NewStringDatum("12:30:50")}, "2017-01-18 13:31:51"},
		{[]types.Datum{types.NewStringDatum("2017-01-18 01:01:01"), types.NewStringDatum("838:59:59")}, "2017-02-22 00:01:00"},

		{[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("20170118123950.123"))}, "2017-01-18 12:39:50.123"},
		{[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("20170118123950.999"))}, "2017-01-18 12:39:50.999"},
		{[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("20170118123950.999"))}, "2017-01-18 12:39:50.999"},
	}
	fc := funcs[ast.Timestamp]
	for _, test := range tests {
		resetStmtContext(s.ctx)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(test.t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}

	nilDatum := types.NewDatum(nil)
	resetStmtContext(s.ctx)
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{nilDatum}))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestMakeDate(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		args     []interface{}
		expected string
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{71, 1}, "1971-01-01", false, false},
		{[]interface{}{71.1, 1.89}, "1971-01-02", false, false},
		{[]interface{}{99, 1}, "1999-01-01", false, false},
		{[]interface{}{100, 1}, "0100-01-01", false, false},
		{[]interface{}{69, 1}, "2069-01-01", false, false},
		{[]interface{}{70, 1}, "1970-01-01", false, false},
		{[]interface{}{1000, 1}, "1000-01-01", false, false},
		{[]interface{}{-1, 3660}, "", true, false},
		{[]interface{}{10000, 3660}, "", true, false},
		{[]interface{}{2060, 2900025}, "9999-12-31", false, false},
		{[]interface{}{2060, 2900026}, "", true, false},
		{[]interface{}{"71", 1}, "1971-01-01", false, false},
		{[]interface{}{71, "1"}, "1971-01-01", false, false},
		{[]interface{}{"71", "1"}, "1971-01-01", false, false},
		{[]interface{}{nil, 2900025}, "", true, false},
		{[]interface{}{2060, nil}, "", true, false},
		{[]interface{}{nil, nil}, "", true, false},
		{[]interface{}{errors.New("must error"), errors.New("must error")}, "", false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.MakeDate, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		tp := f.GetType()
		c.Assert(tp.Tp, Equals, mysql.TypeDate)
		c.Assert(tp.Charset, Equals, charset.CharsetBin)
		c.Assert(tp.Collate, Equals, charset.CollationBin)
		c.Assert(tp.Flag, Equals, uint(mysql.BinaryFlag))
		c.Assert(tp.Flen, Equals, mysql.MaxDateWidth)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetMysqlTime().String(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.MakeDate].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestMakeTime(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Args []interface{}
		Want interface{}
	}{
		{[]interface{}{12, 15, 30}, "12:15:30"},
		{[]interface{}{25, 15, 30}, "25:15:30"},
		{[]interface{}{-25, 15, 30}, "-25:15:30"},
		{[]interface{}{12, -15, 30}, nil},
		{[]interface{}{12, 15, -30}, nil},

		{[]interface{}{12, 15, "30.10"}, "12:15:30.100000"},
		{[]interface{}{12, 15, "30.20"}, "12:15:30.200000"},
		{[]interface{}{12, 15, 30.3000001}, "12:15:30.300000"},
		{[]interface{}{12, 15, 30.0000005}, "12:15:30.000001"},
		{[]interface{}{"12", "15", 30.1}, "12:15:30.100000"},

		{[]interface{}{0, 58.4, 0}, "00:58:00"},
		{[]interface{}{0, "58.4", 0}, "00:58:00"},
		{[]interface{}{0, 58.5, 1}, "00:59:01"},
		{[]interface{}{0, "58.5", 1}, "00:58:01"},
		{[]interface{}{0, 59.5, 1}, nil},
		{[]interface{}{0, "59.5", 1}, "00:59:01"},
		{[]interface{}{0, 1, 59.1}, "00:01:59.100000"},
		{[]interface{}{0, 1, "59.1"}, "00:01:59.100000"},
		{[]interface{}{0, 1, 59.5}, "00:01:59.500000"},
		{[]interface{}{0, 1, "59.5"}, "00:01:59.500000"},
		{[]interface{}{23.5, 1, 10}, "24:01:10"},
		{[]interface{}{"23.5", 1, 10}, "23:01:10"},

		{[]interface{}{0, 0, 0}, "00:00:00"},

		{[]interface{}{837, 59, 59.1}, "837:59:59.100000"},
		{[]interface{}{838, 59, 59.1}, "838:59:59.000000"},
		{[]interface{}{-838, 59, 59.1}, "-838:59:59.000000"},
		{[]interface{}{1000, 1, 1}, "838:59:59"},
		{[]interface{}{-1000, 1, 1.23}, "-838:59:59.000000"},
		{[]interface{}{1000, 59.1, 1}, "838:59:59"},
		{[]interface{}{1000, 59.5, 1}, nil},
		{[]interface{}{1000, 1, 59.1}, "838:59:59.000000"},
		{[]interface{}{1000, 1, 59.5}, "838:59:59.000000"},

		{[]interface{}{12, 15, 60}, nil},
		{[]interface{}{12, 15, "60"}, nil},
		{[]interface{}{12, 60, 0}, nil},
		{[]interface{}{12, "60", 0}, nil},

		{[]interface{}{12, 15, nil}, nil},
		{[]interface{}{12, nil, 0}, nil},
		{[]interface{}{nil, 15, 0}, nil},
		{[]interface{}{nil, nil, nil}, nil},
	}

	Dtbl := tblToDtbl(tbl)
	maketime := funcs[ast.MakeTime]
	for idx, t := range Dtbl {
		f, err := maketime.getFunction(s.ctx, s.datumsToConstants(t["Args"]))
		c.Assert(err, IsNil)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if t["Want"][0].Kind() == types.KindNull {
			c.Assert(got.Kind(), Equals, types.KindNull, Commentf("[%v] - args:%v", idx, t["Args"]))
		} else {
			want, err := t["Want"][0].ToString()
			c.Assert(err, IsNil)
			c.Assert(got.GetMysqlDuration().String(), Equals, want, Commentf("[%v] - args:%v", idx, t["Args"]))
		}
	}

	// MAKETIME(CAST(-1 AS UNSIGNED),0,0);
	tp1 := &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flag:    mysql.UnsignedFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
		Flen:    mysql.MaxIntWidth,
	}
	f := BuildCastFunction(s.ctx, &Constant{Value: types.NewDatum("-1"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err := f.Eval(chunk.Row{})
	c.Assert(err, IsNil)
	f1, err := maketime.getFunction(s.ctx, s.datumsToConstants([]types.Datum{res, makeDatums(0)[0], makeDatums(0)[0]}))
	c.Assert(err, IsNil)
	got, err := evalBuiltinFunc(f1, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(got.GetMysqlDuration().String(), Equals, "838:59:59")

	tbl = []struct {
		Args []interface{}
		Want interface{}
	}{
		{[]interface{}{"", "", ""}, "00:00:00"},
		{[]interface{}{"h", "m", "s"}, "00:00:00"},
	}
	Dtbl = tblToDtbl(tbl)
	maketime = funcs[ast.MakeTime]
	for idx, t := range Dtbl {
		f, err := maketime.getFunction(s.ctx, s.datumsToConstants(t["Args"]))
		c.Assert(err, IsNil)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, NotNil)
		want, err := t["Want"][0].ToString()
		c.Assert(err, IsNil)
		c.Assert(got.GetMysqlDuration().String(), Equals, want, Commentf("[%v] - args:%v", idx, t["Args"]))
	}
}

func (s *testEvaluatorSuite) TestQuarter(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	tests := []struct {
		t      string
		expect int64
	}{
		// Test case from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
		{"2008-04-01", 2},
		// Test case for boundary values
		{"2008-01-01", 1},
		{"2008-03-31", 1},
		{"2008-06-30", 2},
		{"2008-07-01", 3},
		{"2008-09-30", 3},
		{"2008-10-01", 4},
		{"2008-12-31", 4},
		// Test case for month 0
		{"2008-00-01", 0},
	}
	fc := funcs["quarter"]
	for _, test := range tests {
		arg := types.NewStringDatum(test.t)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{arg}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	// test invalid input
	argInvalid := types.NewStringDatum("2008-13-01")
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argInvalid}))
	c.Assert(err, IsNil)
	result, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(result.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestGetFormat(c *C) {
	tests := []struct {
		unit     string
		location string
		expect   string
	}{
		{"DATE", "USA", `%m.%d.%Y`},
		{"DATE", "JIS", `%Y-%m-%d`},
		{"DATE", "ISO", `%Y-%m-%d`},
		{"DATE", "EUR", `%d.%m.%Y`},
		{"DATE", "INTERNAL", `%Y%m%d`},

		{"DATETIME", "USA", `%Y-%m-%d %H.%i.%s`},
		{"DATETIME", "JIS", `%Y-%m-%d %H:%i:%s`},
		{"DATETIME", "ISO", `%Y-%m-%d %H:%i:%s`},
		{"DATETIME", "EUR", `%Y-%m-%d %H.%i.%s`},
		{"DATETIME", "INTERNAL", `%Y%m%d%H%i%s`},

		{"TIME", "USA", `%h:%i:%s %p`},
		{"TIME", "JIS", `%H:%i:%s`},
		{"TIME", "ISO", `%H:%i:%s`},
		{"TIME", "EUR", `%H.%i.%s`},
		{"TIME", "INTERNAL", `%H%i%s`},
	}

	fc := funcs[ast.GetFormat]
	for _, test := range tests {
		t := []types.Datum{types.NewStringDatum(test.unit), types.NewStringDatum(test.location)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestToSeconds(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	tests := []struct {
		param  interface{}
		expect int64
	}{
		{950501, 62966505600},
		{"2009-11-29", 63426672000},
		{"2009-11-29 13:43:32", 63426721412},
		{"09-11-29 13:43:32", 63426721412},
		{"99-11-29 13:43:32", 63111102212},
	}

	fc := funcs[ast.ToSeconds]
	for _, test := range tests {
		t := []types.Datum{types.NewDatum(test.param)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, test.expect)
	}

	testsNull := []interface{}{
		"0000-00-00",
		"1992-13-00",
		"2007-10-07 23:59:61",
		123456789}

	for _, i := range testsNull {
		t := []types.Datum{types.NewDatum(i)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.IsNull(), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestToDays(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	tests := []struct {
		param  interface{}
		expect int64
	}{
		{950501, 728779},
		{"2007-10-07", 733321},
		{"2008-10-07", 733687},
		{"08-10-07", 733687},
		{"0000-01-01", 1},
		{"2007-10-07 00:00:59", 733321},
	}

	fc := funcs[ast.ToDays]
	for _, test := range tests {
		t := []types.Datum{types.NewDatum(test.param)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, test.expect)
	}

	testsNull := []interface{}{
		"0000-00-00",
		"1992-13-00",
		"2007-10-07 23:59:61",
		123456789}

	for _, i := range testsNull {
		t := []types.Datum{types.NewDatum(i)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.IsNull(), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestTimestampAdd(c *C) {
	tests := []struct {
		unit     string
		interval int64
		date     interface{}
		expect   string
	}{
		{"MINUTE", 1, "2003-01-02", "2003-01-02 00:01:00"},
		{"WEEK", 1, "2003-01-02 23:59:59", "2003-01-09 23:59:59"},
		{"MICROSECOND", 1, 950501, "1995-05-01 00:00:00.000001"},
	}

	fc := funcs[ast.TimestampAdd]
	for _, test := range tests {
		t := []types.Datum{types.NewStringDatum(test.unit), types.NewIntDatum(test.interval), types.NewDatum(test.date)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestPeriodAdd(c *C) {
	tests := []struct {
		Period  int64
		Months  int64
		Success bool
		Expect  int64
	}{
		{201611, 2, true, 201701},
		{201611, 3, true, 201702},
		{201611, -13, true, 201510},
		{1611, 3, true, 201702},
		{7011, 3, true, 197102},
		{12323, 10, false, 0},
		{0, 3, false, 0},
	}

	fc := funcs[ast.PeriodAdd]
	for _, test := range tests {
		period := types.NewIntDatum(test.Period)
		months := types.NewIntDatum(test.Months)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{period, months}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		if !test.Success {
			c.Assert(result.IsNull(), IsTrue)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, types.KindInt64)
		value := result.GetInt64()
		c.Assert(value, Equals, test.Expect)
	}
}

func (s *testEvaluatorSuite) TestTimeFormat(c *C) {
	defer testleak.AfterTest(c)()

	// SELECT TIME_FORMAT(null,'%H %k %h %I %l')
	args := []types.Datum{types.NewDatum(nil), types.NewStringDatum(`%H %k %h %I %l`)}
	fc := funcs[ast.TimeFormat]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), Equals, true)

	tblDate := []struct {
		Input  []string
		Expect interface{}
	}{
		{[]string{"100:00:00", `%H %k %h %I %l`},
			"100 100 04 04 4"},
		{[]string{"23:00:00", `%H %k %h %I %l`},
			"23 23 11 11 11"},
		{[]string{"11:00:00", `%H %k %h %I %l`},
			"11 11 11 11 11"},
		{[]string{"17:42:03.000001", `%r %T %h:%i%p %h:%i:%s %p %H %i %s`},
			"05:42:03 PM 17:42:03 05:42PM 05:42:03 PM 17 42 03"},
		{[]string{"07:42:03.000001", `%f`},
			"000001"},
		{[]string{"1990-05-07 19:30:10", `%H %i %s`},
			"19 30 10"},
	}
	dtblDate := tblToDtbl(tblDate)
	for i, t := range dtblDate {
		fc := funcs[ast.TimeFormat]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Expect"][0], Commentf(`no.%d \nobtain:%v \nexpect:%v\n`, i,
			v.GetValue(), t["Expect"][0].GetValue()))
	}
}

func (s *testEvaluatorSuite) TestTimeToSec(c *C) {
	fc := funcs[ast.TimeToSec]

	// test nil
	nilDatum := types.NewDatum(nil)
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{nilDatum}))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	// TODO: Some test cases are commented out due to #4340, #4341.
	tests := []struct {
		input  types.Datum
		expect int64
	}{
		{types.NewStringDatum("22:23:00"), 80580},
		{types.NewStringDatum("00:39:38"), 2378},
		{types.NewStringDatum("23:00"), 82800},
		{types.NewStringDatum("00:00"), 0},
		{types.NewStringDatum("00:00:00"), 0},
		{types.NewStringDatum("23:59:59"), 86399},
		{types.NewStringDatum("1:0"), 3600},
		{types.NewStringDatum("1:00"), 3600},
		{types.NewStringDatum("1:0:0"), 3600},
		{types.NewStringDatum("-02:00"), -7200},
		{types.NewStringDatum("-02:00:05"), -7205},
		{types.NewStringDatum("020005"), 7205},
		// {types.NewStringDatum("20171222020005"), 7205},
		// {types.NewIntDatum(020005), 7205},
		// {types.NewIntDatum(20171222020005), 7205},
		// {types.NewIntDatum(171222020005), 7205},
	}
	for _, test := range tests {
		expr := s.datumsToConstants([]types.Datum{test.input})
		f, err := fc.getFunction(s.ctx, expr)
		c.Assert(err, IsNil, Commentf("%+v", test))
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil, Commentf("%+v", test))
		c.Assert(result.GetInt64(), Equals, test.expect, Commentf("%+v", test))
	}
}

func (s *testEvaluatorSuite) TestSecToTime(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	fc := funcs[ast.SecToTime]

	// test nil
	nilDatum := types.NewDatum(nil)
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{nilDatum}))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	tests := []struct {
		inputDecimal int
		input        types.Datum
		expect       string
	}{
		{0, types.NewIntDatum(2378), "00:39:38"},
		{0, types.NewIntDatum(3864000), "838:59:59"},
		{0, types.NewIntDatum(-3864000), "-838:59:59"},
		{1, types.NewFloat64Datum(86401.4), "24:00:01.4"},
		{1, types.NewFloat64Datum(-86401.4), "-24:00:01.4"},
		{5, types.NewFloat64Datum(86401.54321), "24:00:01.54321"},
		{-1, types.NewFloat64Datum(86401.54321), "24:00:01.543210"},
		{0, types.NewStringDatum("123.4"), "00:02:03.400000"},
		{0, types.NewStringDatum("123.4567891"), "00:02:03.456789"},
		{0, types.NewStringDatum("123"), "00:02:03.000000"},
		{0, types.NewStringDatum("abc"), "00:00:00.000000"},
	}
	for _, test := range tests {
		expr := s.datumsToConstants([]types.Datum{test.input})
		expr[0].GetType().Decimal = test.inputDecimal
		f, err := fc.getFunction(s.ctx, expr)
		c.Assert(err, IsNil, Commentf("%+v", test))
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil, Commentf("%+v", test))
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect, Commentf("%+v", test))
	}
}

func (s *testEvaluatorSuite) TestConvertTz(c *C) {
	tests := []struct {
		t       interface{}
		fromTz  interface{}
		toTz    interface{}
		Success bool
		expect  string
	}{
		{"2004-01-01 12:00:00.111", "-00:00", "+12:34", true, "2004-01-02 00:34:00.111"},
		{"2004-01-01 12:00:00.11", "+00:00", "+12:34", true, "2004-01-02 00:34:00.11"},
		{"2004-01-01 12:00:00.11111111111", "-00:00", "+12:34", true, "2004-01-02 00:34:00.111111"},
		{"2004-01-01 12:00:00", "GMT", "MET", true, "2004-01-01 13:00:00"},
		{"2004-01-01 12:00:00", "-01:00", "-12:00", true, "2004-01-01 01:00:00"},
		{"2004-01-01 12:00:00", "-00:00", "+13:00", true, "2004-01-02 01:00:00"},
		{"2004-01-01 12:00:00", "-00:00", "-13:00", true, ""},
		{"2004-01-01 12:00:00", "-00:00", "-12:88", true, ""},
		{"2004-01-01 12:00:00", "+10:82", "GMT", true, ""},
		{"2004-01-01 12:00:00", "+00:00", "GMT", true, ""},
		{"2004-01-01 12:00:00", "GMT", "+00:00", true, ""},
		{20040101, "+00:00", "+10:32", true, "2004-01-01 10:32:00"},
		{3.14159, "+00:00", "+10:32", true, ""},
		{"2004-01-01 12:00:00", "", "GMT", true, ""},
		{"2004-01-01 12:00:00", "GMT", "", true, ""},
		{"2004-01-01 12:00:00", "a", "GMT", true, ""},
		{"2004-01-01 12:00:00", "0", "GMT", true, ""},
		{"2004-01-01 12:00:00", "GMT", "a", true, ""},
		{"2004-01-01 12:00:00", "GMT", "0", true, ""},
		{nil, "GMT", "+00:00", true, ""},
		{"2004-01-01 12:00:00", nil, "+00:00", true, ""},
		{"2004-01-01 12:00:00", "GMT", nil, true, ""},
	}
	fc := funcs[ast.ConvertTz]
	for _, test := range tests {
		f, err := fc.getFunction(s.ctx,
			s.datumsToConstants(
				[]types.Datum{
					types.NewDatum(test.t),
					types.NewDatum(test.fromTz),
					types.NewDatum(test.toTz)}))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		if test.Success {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
		}
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect, Commentf("convert_tz(\"%v\", \"%s\", \"%s\")", test.t, test.fromTz, test.toTz))
	}
}

func (s *testEvaluatorSuite) TestPeriodDiff(c *C) {
	tests := []struct {
		Period1 int64
		Period2 int64
		Success bool
		Expect  int64
	}{
		{201611, 201611, true, 0},
		{200802, 200703, true, 11},
		{201701, 201611, true, 2},
		{201702, 201611, true, 3},
		{201510, 201611, true, -13},
		{201702, 1611, true, 3},
		{197102, 7011, true, 3},
	}

	tests2 := []struct {
		Period1 int64
		Period2 int64
	}{
		{0, 999999999},
		{9999999, 0},
		{411, 200413},
		{197000, 207700},
		{12509, 12323},
		{12509, 12323},
	}
	fc := funcs[ast.PeriodDiff]
	for _, test := range tests {
		period1 := types.NewIntDatum(test.Period1)
		period2 := types.NewIntDatum(test.Period2)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{period1, period2}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		if !test.Success {
			c.Assert(result.IsNull(), IsTrue)
			continue
		}
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, types.KindInt64)
		value := result.GetInt64()
		c.Assert(value, Equals, test.Expect)
	}

	for _, test := range tests2 {
		period1 := types.NewIntDatum(test.Period1)
		period2 := types.NewIntDatum(test.Period2)
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{period1, period2}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "[expression:1210]Incorrect arguments to period_diff")
	}

	// nil
	args := []types.Datum{types.NewDatum(nil), types.NewIntDatum(0)}
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = []types.Datum{types.NewIntDatum(0), types.NewDatum(nil)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(args))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestLastDay(c *C) {
	tests := []struct {
		param  interface{}
		expect string
	}{
		{"2003-02-05", "2003-02-28"},
		{"2004-02-05", "2004-02-29"},
		{"2004-01-01 01:01:01", "2004-01-31"},
		{950501, "1995-05-31"},
	}

	fc := funcs[ast.LastDay]
	for _, test := range tests {
		t := []types.Datum{types.NewDatum(test.param)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}

	testsNull := []interface{}{
		"0000-00-00",
		"1992-13-00",
		"2007-10-07 23:59:61",
		"2005-00-00",
		"2005-00-01",
		123456789}

	for _, i := range testsNull {
		t := []types.Datum{types.NewDatum(i)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.IsNull(), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestWithTimeZone(c *C) {
	sv := s.ctx.GetSessionVars()
	originTZ := sv.Location()
	sv.TimeZone, _ = time.LoadLocation("Asia/Tokyo")
	defer func() {
		sv.TimeZone = originTZ
	}()

	timeToGoTime := func(d types.Datum, loc *time.Location) time.Time {
		result, _ := d.GetMysqlTime().Time.GoTime(loc)
		return result
	}
	durationToGoTime := func(d types.Datum, loc *time.Location) time.Time {
		t, _ := d.GetMysqlDuration().ConvertToTime(sv.StmtCtx, mysql.TypeDatetime)
		result, _ := t.Time.GoTime(sv.TimeZone)
		return result
	}

	tests := []struct {
		method        string
		Input         []types.Datum
		convertToTime func(types.Datum, *time.Location) time.Time
	}{
		{ast.Sysdate, makeDatums(2), timeToGoTime},
		{ast.Sysdate, nil, timeToGoTime},
		{ast.Curdate, nil, timeToGoTime},
		{ast.CurrentTime, makeDatums(2), durationToGoTime},
		{ast.CurrentTime, nil, durationToGoTime},
		{ast.Curtime, nil, durationToGoTime},
	}

	for _, t := range tests {
		now := time.Now().In(sv.TimeZone)
		f, err := funcs[t.method].getFunction(s.ctx, s.datumsToConstants(t.Input))
		c.Assert(err, IsNil)
		resetStmtContext(s.ctx)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result := t.convertToTime(d, sv.TimeZone)
		c.Assert(result.Sub(now), LessEqual, 2*time.Second)
	}
}

func (s *testEvaluatorSuite) TestTidbParseTso(c *C) {
	s.ctx.GetSessionVars().TimeZone = time.UTC
	tests := []struct {
		param  interface{}
		expect string
	}{
		{404411537129996288, "2018-11-20 09:53:04.877000"},
		{"404411537129996288", "2018-11-20 09:53:04.877000"},
		{1, "1970-01-01 00:00:00.000000"},
	}

	fc := funcs[ast.TiDBParseTso]
	for _, test := range tests {
		t := []types.Datum{types.NewDatum(test.param)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}

	testsNull := []interface{}{
		0,
		-1,
		"-1"}

	for _, i := range testsNull {
		t := []types.Datum{types.NewDatum(i)}
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t))
		c.Assert(err, IsNil)
		d, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d.IsNull(), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestGetIntervalFromDecimal(c *C) {
	defer testleak.AfterTest(c)()
	du := baseDateArithmitical{}

	tests := []struct {
		param  string
		expect string
		unit   string
	}{
		{"1.100", "1:100", "MINUTE_SECOND"},
		{"1.10000", "1-10000", "YEAR_MONTH"},
		{"1.10000", "1 10000", "DAY_HOUR"},
		{"11000", "0 00:00:11000", "DAY_MICROSECOND"},
		{"11000", "00:00:11000", "HOUR_MICROSECOND"},
		{"11.1000", "00:11:1000", "HOUR_SECOND"},
		{"1000", "00:1000", "MINUTE_MICROSECOND"},
	}

	for _, test := range tests {
		interval, isNull, err := du.getIntervalFromDecimal(s.ctx, s.datumsToConstants([]types.Datum{types.NewDatum("CURRENT DATE"), types.NewDecimalDatum(newMyDecimal(c, test.param))}), chunk.Row{}, test.unit)
		c.Assert(isNull, IsFalse)
		c.Assert(err, IsNil)
		c.Assert(interval, Equals, test.expect)
	}
}
