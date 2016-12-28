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
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) getYearFunction() builtinFunc {
	f := &builtinYear{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getMonthFunction() builtinFunc {
	f := &builtinMonth{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getMonthNameFunction() builtinFunc {
	f := &builtinMonthName{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getDayOfMonthFunction() builtinFunc {
	f := &builtinDayOfMonth{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getDayOfWeekFunction() builtinFunc {
	f := &builtinDayOfWeek{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getDayOfYearFunction() builtinFunc {
	f := &builtinDayOfYear{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getWeekDayFunction() builtinFunc {
	f := &builtinWeekDay{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getDayNameFunction() builtinFunc {
	f := &builtinDayName{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getWeekFunction() builtinFunc {
	f := &builtinWeek{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getWeekOfYearFunction() builtinFunc {
	f := &builtinWeekOfYear{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getYearWeekFunction() builtinFunc {
	f := &builtinYearWeek{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getMinuteFunction() builtinFunc {
	f := &builtinMinute{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getHourFunction() builtinFunc {
	f := &builtinHour{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getSecondFunction() builtinFunc {
	f := &builtinSecond{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getMicroSecondFunction() builtinFunc {
	f := &builtinMicroSecond{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

func (s *testEvaluatorSuite) getTimeFunction() builtinFunc {
	f := &builtinTime{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	return f
}

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
		f := &builtinDate{newBaseBuiltinFunc(nil, true, s.ctx)}
		f.self = f
		v, err := f.constantFold(t["Input"])
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
		{"2000-01-01", 2000, 1, "January", 1, 7, 1, 5, "Saturday", 0, 52, 199952},
		{"2011-11-11", 2011, 11, "November", 11, 6, 315, 4, "Friday", 45, 45, 201145},
		{"0000-01-01", int64(0), 1, "January", 1, 7, 1, 5, "Saturday", 1, 52, 1},
	}

	dtbl := tblToDtbl(tbl)
	for ith, t := range dtbl {
		args := t["Input"]

		f := s.getYearFunction()
		v, err := f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		f = s.getMonthFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		f = s.getMonthNameFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		f = s.getDayOfMonthFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		f = s.getDayOfWeekFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		f = s.getDayOfYearFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		f = s.getWeekDayFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		f = s.getDayNameFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		f = s.getWeekFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0], Commentf("no.%d", ith))

		f = s.getWeekOfYearFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		f = s.getYearWeekFunction()
		v, err = f.constantFold(args)
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
		{"0000-00-00", int64(0), int64(0), nil, int64(0), nil, nil, nil, nil, nil, nil, nil},
	}

	dtblNil := tblToDtbl(tblNil)
	for _, t := range dtblNil {
		args := t["Input"]
		f := s.getYearFunction()
		v, err := f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		f = s.getMonthFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		f = s.getMonthNameFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		f = s.getDayOfMonthFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		f = s.getDayOfWeekFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		f = s.getDayOfYearFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		f = s.getWeekDayFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		f = s.getDayNameFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		f = s.getWeekFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		f = s.getWeekOfYearFunction()
		v, err = f.constantFold(args)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		f = s.getYearWeekFunction()
		v, err = f.constantFold(args)
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
		f := &builtinDateFormat{newBaseBuiltinFunc(nil, true, s.ctx)}
		f.self = f
		v, err := f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Expect"][0], Commentf("no.%d \nobtain:%v \nexpect:%v\n", i,
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
		f := s.getHourFunction()
		v, err := f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Hour"][0])

		f = s.getMinuteFunction()
		v, err = f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Minute"][0])

		f = s.getSecondFunction()
		v, err = f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Second"][0])

		f = s.getMicroSecondFunction()
		v, err = f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MicroSecond"][0])

		f = s.getTimeFunction()
		v, err = f.constantFold(t["Input"])
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Time"][0])
	}

	// nil
	f := s.getHourFunction()
	v, err := f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f = s.getMinuteFunction()
	v, err = f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f = s.getSecondFunction()
	v, err = f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f = s.getMicroSecondFunction()
	v, err = f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f = s.getTimeFunction()
	v, err = f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	// test error
	errTbl := []string{
		"2011-11-11T10:10:10.11",
		"2011-11-11 10:10:10.11.12",
	}

	for _, t := range errTbl {
		td := types.MakeDatums(t)
		f := s.getHourFunction()
		_, err := f.constantFold(types.MakeDatums(td))
		c.Assert(err, NotNil)

		f = s.getMinuteFunction()
		_, err = f.constantFold(types.MakeDatums(td))
		c.Assert(err, NotNil)

		f = s.getSecondFunction()
		_, err = f.constantFold(types.MakeDatums(td))
		c.Assert(err, NotNil)

		f = s.getMicroSecondFunction()
		_, err = f.constantFold(types.MakeDatums(td))
		c.Assert(err, NotNil)

		f = s.getTimeFunction()
		_, err = f.constantFold(types.MakeDatums(td))
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestNow(c *C) {
	defer testleak.AfterTest(c)()
	f := &builtinNow{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	v, err := f.constantFold(nil)
	c.Assert(err, IsNil)
	t := v.GetMysqlTime()
	// we canot use a constant value to check now, so here
	// just to check whether has fractional seconds part.
	c.Assert(strings.Contains(t.String(), "."), IsFalse)

	v, err = f.constantFold(types.MakeDatums(6))
	c.Assert(err, IsNil)
	t = v.GetMysqlTime()
	c.Assert(strings.Contains(t.String(), "."), IsTrue)

	v, err = f.constantFold(types.MakeDatums(8))
	c.Assert(err, NotNil)

	v, err = f.constantFold(types.MakeDatums(-2))
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
	f := &builtinFromUnixTime{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
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
			v, err := f.constantFold([]types.Datum{timestamp})
			c.Assert(err, IsNil)
			ans := v.GetMysqlTime()
			c.Assert(ans.String(), Equals, unixTime)
		} else {
			format := types.NewStringDatum(t.format)
			v, err := f.constantFold([]types.Datum{timestamp, format})
			c.Assert(err, IsNil)
			df := &builtinDateFormat{newBaseBuiltinFunc(nil, true, s.ctx)}
			df.self = df
			result, err := df.constantFold([]types.Datum{types.NewStringDatum(unixTime), format})
			c.Assert(err, IsNil)
			c.Assert(v.GetString(), Equals, result.GetString())
		}
	}

	v, err := f.constantFold([]types.Datum{types.NewIntDatum(-12345)})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	_, err = f.constantFold([]types.Datum{types.NewIntDatum(math.MaxInt32 + 1)})
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestCurrentDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now()
	v, err := (&builtinCurrentDate{newBaseBuiltinFunc(nil, true, s.ctx)}).eval(nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.DateFormat))
}

func (s *testEvaluatorSuite) TestCurrentTime(c *C) {
	defer testleak.AfterTest(c)()
	tfStr := "15:04:05"

	f := &builtinCurrentTime{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	last := time.Now()
	v, err := f.constantFold(types.MakeDatums(nil))
	c.Assert(err, IsNil)
	n := v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = f.constantFold(types.MakeDatums(3))
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 12)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = f.constantFold(types.MakeDatums(6))
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 15)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	v, err = f.constantFold(types.MakeDatums(-1))
	c.Assert(err, NotNil)

	v, err = f.constantFold(types.MakeDatums(7))
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestUTCDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now().UTC()
	v, err := (&builtinUTCDate{newBaseBuiltinFunc(nil, false, s.ctx)}).eval(nil)
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
		{"20161122165022", "%Y%m%d%H%i%s", true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"2016 11 22 16 50 22", "%Y%m%d%H%i%s", true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"16-50-22 2016 11 22", "%H-%i-%s%Y%m%d", true, time.Date(2016, 11, 22, 16, 50, 22, 0, time.Local)},
		{"16-50 2016 11 22", "%H-%i-%s%Y%m%d", false, time.Time{}},
	}

	f := &builtinStrToDate{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	for _, test := range tests {
		date := types.NewStringDatum(test.Date)
		format := types.NewStringDatum(test.Format)
		result, err := f.constantFold([]types.Datum{date, format})
		if !test.Success {
			c.Assert(err, IsNil)
			c.Assert(result.IsNull(), IsTrue)
			continue
		}
		c.Assert(result.Kind(), Equals, types.KindMysqlTime)
		value := result.GetMysqlTime()
		t1, _ := value.Time.GoTime()
		c.Assert(t1, Equals, test.Expect)
	}
}

func (s *testEvaluatorSuite) TestTimeDiff(c *C) {
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
	tests := []struct {
		t1        string
		t2        string
		expectStr string
	}{
		{"2000:01:01 00:00:00", "2000:01:01 00:00:00.000001", "-00:00:00.000001"},
		{"2008-12-31 23:59:59.000001", "2008-12-30 01:01:01.000002", "46:58:57.999999"},
		{"2016-12-00 12:00:00", "2016-12-01 12:00:00", "-24:00:00.000000"},
	}
	f := &builtinTimeDiff{newBaseBuiltinFunc(nil, true, s.ctx)}
	f.self = f
	for _, test := range tests {
		t1 := types.NewStringDatum(test.t1)
		t2 := types.NewStringDatum(test.t2)
		result, err := f.constantFold([]types.Datum{t1, t2})
		c.Assert(err, IsNil)
		c.Assert(result.GetMysqlDuration().String(), Equals, test.expectStr)
	}
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
	for _, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		arg2 := types.NewIntDatum(test.mode)
		f := s.getWeekFunction()
		result, err := f.constantFold([]types.Datum{arg1, arg2})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

}

func (s *testEvaluatorSuite) TestYearWeek(c *C) {
	// Test cases from https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
	tests := []struct {
		t      string
		mode   int64
		expect int64
	}{
		{"1987-01-01", 0, 198652},
		{"2000-01-01", 0, 199952},
	}
	f := s.getYearWeekFunction()
	for _, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		arg2 := types.NewIntDatum(test.mode)
		result, err := f.constantFold([]types.Datum{arg1, arg2})
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	result, err := f.constantFold([]types.Datum{types.NewStringDatum("2016-00-05")})
	c.Assert(err, IsNil)
	c.Assert(result.IsNull(), IsTrue)
}
