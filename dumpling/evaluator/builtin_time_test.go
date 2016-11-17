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
	"math"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
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
		v, err := builtinDate(t["Input"], nil)
		c.Assert(err, IsNil)
		if v.Kind() != types.KindMysqlTime {
			c.Assert(v, testutil.DatumEquals, t["Expect"][0])
		} else {
			c.Assert(v.GetMysqlTime().String(), Equals, t["Expect"][0].GetString())
		}
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
		{"2000-01-01", 2000, 1, "January", 1, 7, 1, 5, "Saturday", 52, 52, 199952},
		{"2011-11-11", 2011, 11, "November", 11, 6, 315, 4, "Friday", 45, 45, 201145},
		{"0000-01-01", int64(0), 1, "January", 1, 7, 1, 5, "Saturday", 52, 52, math.MaxUint32},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		args := t["Input"]
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		v, err = builtinMonthName(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		v, err = builtinDayName(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["YearWeek"][0])
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
		{"0000-00-00", int64(0), int64(0), nil, int64(0), nil, nil, nil, nil, nil, nil, nil},
	}

	dtblNil := tblToDtbl(tblNil)
	for _, t := range dtblNil {
		args := t["Input"]
		v, err := builtinYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		v, err = builtinMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		v, err = builtinMonthName(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		v, err = builtinDayOfMonth(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		v, err = builtinDayOfWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		v, err = builtinDayOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		v, err = builtinWeekDay(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		v, err = builtinWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		v, err = builtinWeekOfYear(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		v, err = builtinYearWeek(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["YearWeek"][0])
	}
}

func (s *testEvaluatorSuite) TestDateFormat(c *C) {
	defer testleak.AfterTest(c)()

	tblDate := []struct {
		Input  []string
		Expect interface{}
	}{
		{[]string{"2010-01-07 23:12:34.12345",
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%"},
			"Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %"},
		{[]string{"2012-12-21 23:12:34.123456",
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%"},
			"Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %"},
		{[]string{"0000-01-01 00:00:00.123456",
			// Functions week() and yearweek() don't support multi mode,
			// so the result of "%U %u %V %Y" is different from MySQL.
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %x %Y %y %%"},
			"Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 4294967295 0000 00 %"},
		{[]string{"2016-09-3 00:59:59.123456",
			"abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z"},
			"abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z"},
		{[]string{"2012-10-01 00:00:00",
			"%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%"},
			"Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %"},
	}
	dtblDate := tblToDtbl(tblDate)

	for i, t := range dtblDate {
		v, err := builtinDateFormat(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Expect"][0], Commentf("no.%d \nobtain:%v \nexpect:%v\n", i,
			v.GetValue(), t["Expect"][0].GetValue()))
	}

	// error
	ds := types.MakeDatums("0000-01-00 00:00:00.123456",
		"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%")
	_, err := builtinDateFormat(ds, nil)
	// Some like dayofweek() doesn't support the date format like 2000-00-00 returns 0,
	// so it returns an error.
	c.Assert(err, NotNil)
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
		v, err := builtinHour(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Hour"][0])

		v, err = builtinMinute(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Minute"][0])

		v, err = builtinSecond(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Second"][0])

		v, err = builtinMicroSecond(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MicroSecond"][0])

		v, err = builtinTime(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Time"][0])
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

	v, err = builtinTime(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	// test error
	errTbl := []string{
		"2011-11-11T10:10:10.11",
		"2011-11-11 10:10:10.11.12",
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

		_, err = builtinTime(td, nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestNow(c *C) {
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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

func (s *testEvaluatorSuite) TestFromUnixTime(c *C) {
	defer testleak.AfterTest(c)()

	tbl := []struct {
		isDecimal      bool
		integralPart   int64
		fractionalPart int64
		decimal        float64
		format         string
		ansLen         int
	}{
		{false, 1451606400, 0, 0, "", 19},
		{true, 1451606400, 123456000, 1451606400.123456, "", 26},
		{true, 1451606400, 999999000, 1451606400.999999, "", 26},
		{true, 1451606400, 999999900, 1451606400.9999999, "", 19},
		{false, 1451606400, 0, 0, "%Y %D %M %h:%i:%s %x", 19},
		{true, 1451606400, 123456000, 1451606400.123456, "%Y %D %M %h:%i:%s %x", 26},
		{true, 1451606400, 999999000, 1451606400.999999, "%Y %D %M %h:%i:%s %x", 26},
		{true, 1451606400, 999999900, 1451606400.9999999, "%Y %D %M %h:%i:%s %x", 19},
	}

	for _, t := range tbl {
		var timestamp types.Datum
		if !t.isDecimal {
			timestamp.SetInt64(t.integralPart)
		} else {
			timestamp.SetFloat64(t.decimal)
		}
		// result of from_unixtime() is dependent on specific time zone.
		unixTime := time.Unix(t.integralPart, t.fractionalPart).Round(time.Microsecond).String()[:t.ansLen]
		if len(t.format) == 0 {
			v, err := builtinFromUnixTime([]types.Datum{timestamp}, nil)
			c.Assert(err, IsNil)
			ans := v.GetMysqlTime()
			c.Assert(ans.String(), Equals, unixTime)
		} else {
			format := types.NewStringDatum(t.format)
			v, err := builtinFromUnixTime([]types.Datum{timestamp, format}, nil)
			c.Assert(err, IsNil)
			result, err := builtinDateFormat([]types.Datum{types.NewStringDatum(unixTime), format}, nil)
			c.Assert(err, IsNil)
			c.Assert(v.GetString(), Equals, result.GetString())
		}
	}

	v, err := builtinFromUnixTime([]types.Datum{types.NewIntDatum(-12345)}, nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	_, err = builtinFromUnixTime([]types.Datum{types.NewIntDatum(math.MaxInt32 + 1)}, nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestCurrentDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now()
	v, err := builtinCurrentDate(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.DateFormat))
}

func (s *testEvaluatorSuite) TestCurrentTime(c *C) {
	defer testleak.AfterTest(c)()
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

func (s *testEvaluatorSuite) TestUTCDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now().UTC()
	v, err := builtinUTCDate(types.MakeDatums(nil), nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(mysql.DateFormat))
}

func (s *testEvaluatorSuite) TestDateArith(c *C) {
	defer testleak.AfterTest(c)()
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
		ast.SetFlag(expr)
		v, err := Eval(ctx, expr)
		if t.error == true {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if v.IsNull() {
				c.Assert(nil, Equals, t.AddResult)
			} else {
				c.Assert(v.Kind(), Equals, types.KindMysqlTime)
				value := v.GetMysqlTime()
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
			if v.IsNull() {
				c.Assert(nil, Equals, t.AddResult)
			} else {
				c.Assert(v.Kind(), Equals, types.KindMysqlTime)
				value := v.GetMysqlTime()
				c.Assert(value.String(), Equals, t.SubResult)
			}
		}
	}
}
