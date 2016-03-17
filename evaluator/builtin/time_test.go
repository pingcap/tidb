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

package builtin

import (
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

func (s *testBuiltinSuite) TestDate(c *C) {
	tblDate := []struct {
		Input  interface{}
		Expect interface{}
	}{
		{"2011-11-11", "2011-11-11"},
		{nil, nil},
		{"2011-11-11 10:10:10", "2011-11-11"},
	}

	for _, t := range tblDate {
		v, err := builtinDate([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		switch x := v.(type) {
		case nil:
			c.Assert(t.Expect, IsNil)
		case mysql.Time:
			c.Assert(x.String(), Equals, t.Expect)
		default:
			c.Assert(x, DeepEquals, t.Expect)
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

	for _, t := range tbl {
		args := []interface{}{t.Input}
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Year)

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Month)

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfMonth)

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfWeek)

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfYear)

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.WeekDay)

		v, err = builtinDayName(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayName)

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Week)

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.WeekOfYear)

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.YearWeek)
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

	for _, t := range tblNil {
		args := []interface{}{t.Input}
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Year)

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Month)

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfMonth)

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfWeek)

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayOfYear)

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.WeekDay)

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.DayName)

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Week)

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.WeekOfYear)

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.YearWeek)
	}
}

func (s *testBuiltinSuite) TestClock(c *C) {
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

	for _, t := range tbl {
		v, err := builtinHour([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Hour)

		v, err = builtinMinute([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Minute)

		v, err = builtinSecond([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Second)

		v, err = builtinMicroSecond([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.MicroSecond)
	}

	// nil
	v, err := builtinHour([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinMinute([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinSecond([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinMicroSecond([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	// test error
	errTbl := []string{
		"2011-11-11T10:10:10.11",
	}

	for _, t := range errTbl {
		_, err := builtinHour([]interface{}{t}, nil)
		c.Assert(err, NotNil)

		_, err = builtinMinute([]interface{}{t}, nil)
		c.Assert(err, NotNil)

		_, err = builtinSecond([]interface{}{t}, nil)
		c.Assert(err, NotNil)

		_, err = builtinMicroSecond([]interface{}{t}, nil)
		c.Assert(err, NotNil)
	}
}

func (s *testBuiltinSuite) TestNow(c *C) {
	v, err := builtinNow(nil, nil)
	c.Assert(err, IsNil)
	t, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	// we canot use a constant value to check now, so here
	// just to check whether has fractional seconds part.
	c.Assert(strings.Contains(t.String(), "."), IsFalse)

	v, err = builtinNow([]interface{}{6}, nil)
	c.Assert(err, IsNil)
	t, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(strings.Contains(t.String(), "."), IsTrue)

	_, err = builtinNow([]interface{}{8}, nil)
	c.Assert(err, NotNil)

	_, err = builtinNow([]interface{}{-2}, nil)
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestSysDate(c *C) {
	last := time.Now()
	v, err := builtinSysDate(nil, nil)
	c.Assert(err, IsNil)
	n, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.TimeFormat))

	v, err = builtinSysDate([]interface{}{6}, nil)
	c.Assert(err, IsNil)
	n, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.TimeFormat))

	_, err = builtinSysDate([]interface{}{-2}, nil)
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestCurrentDate(c *C) {
	last := time.Now()
	v, err := builtinCurrentDate(nil, nil)
	c.Assert(err, IsNil)
	n, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.DateFormat))
}

func (s *testBuiltinSuite) TestCurrentTime(c *C) {
	tfStr := "15:04:05"

	last := time.Now()
	v, err := builtinCurrentTime(nil, nil)
	c.Assert(err, IsNil)
	n, ok := v.(mysql.Duration)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime([]interface{}{3}, nil)
	c.Assert(err, IsNil)
	n, ok = v.(mysql.Duration)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), HasLen, 12)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime([]interface{}{6}, nil)
	c.Assert(err, IsNil)
	n, ok = v.(mysql.Duration)
	c.Assert(ok, IsTrue)
	c.Assert(n.String(), HasLen, 15)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = builtinCurrentTime([]interface{}{-1}, nil)
	c.Assert(err, NotNil)

	v, err = builtinCurrentTime([]interface{}{7}, nil)
	c.Assert(err, NotNil)
}
