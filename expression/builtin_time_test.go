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
	"github.com/pingcap/tidb/ast"
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
		fc := funcs[ast.Date]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		fc = funcs[ast.Month]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		fc = funcs[ast.MonthName]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		fc = funcs[ast.DayOfMonth]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		fc = funcs[ast.DayOfWeek]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		fc = funcs[ast.DayOfYear]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		fc = funcs[ast.Weekday]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		fc = funcs[ast.DayName]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		fc = funcs[ast.Week]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0], Commentf("no.%d", ith))

		fc = funcs[ast.WeekOfYear]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		fc = funcs[ast.YearWeek]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
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
		fc := funcs[ast.Year]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Year"][0])

		fc = funcs[ast.Month]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Month"][0])

		fc = funcs[ast.MonthName]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MonthName"][0])

		fc = funcs[ast.DayOfMonth]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfMonth"][0])

		fc = funcs[ast.DayOfWeek]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfWeek"][0])

		fc = funcs[ast.DayOfYear]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayOfYear"][0])

		fc = funcs[ast.Weekday]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekDay"][0])

		fc = funcs[ast.DayName]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["DayName"][0])

		fc = funcs[ast.Week]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Week"][0])

		fc = funcs[ast.WeekOfYear]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["WeekOfYear"][0])

		fc = funcs[ast.YearWeek]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
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
		fc := funcs[ast.DateFormat]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
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
		fc := funcs[ast.Hour]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Hour"][0])

		fc = funcs[ast.Minute]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Minute"][0])

		fc = funcs[ast.Second]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Second"][0])

		fc = funcs[ast.MicroSecond]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["MicroSecond"][0])

		fc = funcs[ast.Time]
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Time"][0])
	}

	// nil
	fc := funcs[ast.Hour]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Minute]
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Second]
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.MicroSecond]
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	fc = funcs[ast.Time]
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(td), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)

		fc = funcs[ast.Minute]
		f, err = fc.getFunction(datumsToConstants(td), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)

		fc = funcs[ast.Second]
		f, err = fc.getFunction(datumsToConstants(td), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)

		fc = funcs[ast.MicroSecond]
		f, err = fc.getFunction(datumsToConstants(td), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)

		fc = funcs[ast.Time]
		f, err = fc.getFunction(datumsToConstants(td), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)
	}
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
		f, err := x.fc.getFunction(datumsToConstants(nil), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		ts := x.now()
		c.Assert(err, IsNil)
		t := v.GetMysqlTime()
		// we canot use a constant value to check timestamp funcs, so here
		// just to check the fractional seconds part and the time delta.
		c.Assert(strings.Contains(t.String(), "."), IsFalse)
		c.Assert(ts.Sub(gotime(t, ts.Location())), LessEqual, time.Second)

		f, err = x.fc.getFunction(datumsToConstants(types.MakeDatums(6)), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		ts = x.now()
		c.Assert(err, IsNil)
		t = v.GetMysqlTime()
		c.Assert(strings.Contains(t.String(), "."), IsTrue)
		c.Assert(ts.Sub(gotime(t, ts.Location())), LessEqual, time.Millisecond)

		f, err = x.fc.getFunction(datumsToConstants(types.MakeDatums(8)), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)

		f, err = x.fc.getFunction(datumsToConstants(types.MakeDatums(-2)), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestSysDate(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Sysdate]
	f, err := fc.getFunction(datumsToConstants(nil), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	last := time.Now()
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.TimeFormat))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(6)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.TimeFormat))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(-2)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
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
	fc := funcs[ast.FromUnixTime]
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
			f, err := fc.getFunction(datumsToConstants([]types.Datum{timestamp}), s.ctx)
			c.Assert(err, IsNil)
			v, err := f.eval(nil)
			c.Assert(err, IsNil)
			ans := v.GetMysqlTime()
			c.Assert(ans.String(), Equals, unixTime)
		} else {
			format := types.NewStringDatum(t.format)
			f, err := fc.getFunction(datumsToConstants([]types.Datum{timestamp, format}), s.ctx)
			c.Assert(err, IsNil)
			v, err := f.eval(nil)
			c.Assert(err, IsNil)
			result, err := builtinDateFormat([]types.Datum{types.NewStringDatum(unixTime), format}, s.ctx)
			c.Assert(err, IsNil)
			c.Assert(v.GetString(), Equals, result.GetString())
		}
	}

	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(-12345)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(math.MaxInt32+1)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestCurrentDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now()
	fc := funcs[ast.CurrentDate]
	f, err := fc.getFunction(datumsToConstants(nil), mock.NewContext())
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlTime()
	c.Assert(n.String(), GreaterEqual, last.Format(types.DateFormat))
}

func (s *testEvaluatorSuite) TestCurrentTime(c *C) {
	defer testleak.AfterTest(c)()
	tfStr := "15:04:05"

	last := time.Now()
	fc := funcs[ast.CurrentTime]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	n := v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 8)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(3)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 12)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(6)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	n = v.GetMysqlDuration()
	c.Assert(n.String(), HasLen, 15)
	c.Assert(n.String(), GreaterEqual, last.Format(tfStr))

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(-1)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(7)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestUTCDate(c *C) {
	defer testleak.AfterTest(c)()
	last := time.Now().UTC()
	fc := funcs[ast.UTCDate]
	f, err := fc.getFunction(datumsToConstants(nil), mock.NewContext())
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
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

	fc := funcs[ast.StrToDate]
	for _, test := range tests {
		date := types.NewStringDatum(test.Date)
		format := types.NewStringDatum(test.Format)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{date, format}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
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

		f, err := fc.getFunction(datumsToConstants([]types.Datum{t1}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)

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

		f, err := fc.getFunction(datumsToConstants([]types.Datum{t1}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)

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

		f, err := fc.getFunction(datumsToConstants([]types.Datum{t1, t2}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)

		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	// Check if month is 0.
	t1 := types.NewStringDatum("2016-00-01")
	t2 := types.NewStringDatum("2016-01-13")

	f, err := fc.getFunction(datumsToConstants([]types.Datum{t1, t2}), s.ctx)
	c.Assert(err, IsNil)
	result, err := f.eval(nil)

	c.Assert(err, IsNil)
	c.Assert(result.IsNull(), Equals, true)

	f, err = fc.getFunction(datumsToConstants([]types.Datum{{}, types.NewStringDatum("2017-01-01")}), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), IsTrue)
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
	fc := funcs[ast.TimeDiff]
	for _, test := range tests {
		t1 := types.NewStringDatum(test.t1)
		t2 := types.NewStringDatum(test.t2)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{t1, t2}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result.GetMysqlDuration().String(), Equals, test.expectStr)
	}
	f, err := fc.getFunction(datumsToConstants([]types.Datum{{}, types.NewStringDatum("2017-01-01")}), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), IsTrue)
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
		f, err := fc.getFunction(datumsToConstants([]types.Datum{arg1, arg2}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
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
	fc := funcs[ast.YearWeek]
	for _, test := range tests {
		arg1 := types.NewStringDatum(test.t)
		arg2 := types.NewIntDatum(test.mode)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{arg1, arg2}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	f, err := fc.getFunction(datumsToConstants(types.MakeDatums("2016-00-05")), s.ctx)
	c.Assert(err, IsNil)
	result, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(args), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d.GetInt64(), Equals, test.expect)
	}
	s.ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	f, err := fc.getFunction(datumsToConstants([]types.Datum{types.NewStringDatum("DAY"),
		types.NewStringDatum("2017-01-00"),
		types.NewStringDatum("2017-01-01")}), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	f, err = fc.getFunction(datumsToConstants([]types.Datum{types.NewStringDatum("DAY"),
		{}, types.NewStringDatum("2017-01-01")}), s.ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestUnixTimestamp(c *C) {
	fc := funcs[ast.UnixTimestamp]
	f, err := fc.getFunction(nil, s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64()-time.Now().Unix(), GreaterEqual, int64(-1))
	c.Assert(d.GetInt64()-time.Now().Unix(), LessEqual, int64(1))

	// Test case for https://github.com/pingcap/tidb/issues/2496
	// select unix_timestamp(now());
	n, err := builtinNow(nil, s.ctx)
	c.Assert(err, IsNil)
	args := []types.Datum{n}
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetInt64()-time.Now().Unix(), GreaterEqual, int64(-1))
	c.Assert(d.GetInt64()-time.Now().Unix(), LessEqual, int64(1))

	// Test case for https://github.com/pingcap/tidb/issues/2852
	// select UNIX_TIMESTAMP(null);
	args = []types.Datum{types.NewDatum(nil)}
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.IsNull(), Equals, true)

	// Set the time_zone variable, because UnixTimestamp() result depends on it.
	s.ctx.GetSessionVars().TimeZone = time.UTC
	tests := []struct {
		input  types.Datum
		expect string
	}{
		{types.NewIntDatum(20151113102019), "1447410019"},
		{types.NewStringDatum("2015-11-13 10:20:19"), "1447410019"},
		{types.NewStringDatum("2015-11-13 10:20:19.012"), "1447410019.012"},
		{types.NewStringDatum("2017-00-02"), "0"},
	}

	for _, test := range tests {
		f, err := fc.getFunction(datumsToConstants([]types.Datum{test.input}), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		str, err := d.ToString()
		c.Assert(err, IsNil)
		c.Assert(str, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestDateArithFuncs(c *C) {
	defer testleak.AfterTest(c)()

	date := []string{"2016-12-31", "2017-01-01"}
	fcAdd := funcs[ast.DateAdd]
	fcSub := funcs[ast.DateSub]

	args := types.MakeDatums(date[0], 1, "DAY")
	f, err := fcAdd.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime().String(), Equals, date[1])

	args = types.MakeDatums(date[1], 1, "DAY")
	f, err = fcSub.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetMysqlTime().String(), Equals, date[0])

	args = types.MakeDatums(date[0], nil, "DAY")
	f, err = fcAdd.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	args = types.MakeDatums(date[1], nil, "DAY")
	f, err = fcSub.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)
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

		// two arguments
		{[]types.Datum{types.NewStringDatum("2017-01-18"), types.NewStringDatum("12:30:59")}, "2017-01-18 12:30:59"},
		{[]types.Datum{types.NewStringDatum("2017-01-18"), types.NewStringDatum("12:30:59")}, "2017-01-18 12:30:59"},
		{[]types.Datum{types.NewStringDatum("2017-01-18 01:01:01"), types.NewStringDatum("12:30:50")}, "2017-01-18 13:31:51"},
		{[]types.Datum{types.NewStringDatum("2017-01-18 01:01:01"), types.NewStringDatum("838:59:59")}, "2017-02-22 00:01:00"},

		// TODO: the following test cases exists precision problems.
		//{[]types.Datum{types.NewFloat64Datum(20170118123950.123)}, "2017-01-18 12:30:50.123"},
		//{[]types.Datum{types.NewFloat64Datum(20170118123950.999)}, "2017-01-18 12:30:50.999"},
		//{[]types.Datum{types.NewFloat32Datum(float32(20170118123950.999))}, "2017-01-18 12:30:50.699"},

		// TODO: the following test cases will cause time format error.
		//{[]types.Datum{types.NewFloat64Datum(20170118.999)}, "2017-01-18 00:00:00.000"},
		//{[]types.Datum{types.NewStringDatum("11111111111")}, "2011-11-11 11:11:01"},

	}
	fc := funcs[ast.Timestamp]
	for _, test := range tests {
		f, err := fc.getFunction(datumsToConstants(test.t), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		result, _ := d.ToString()
		c.Assert(result, Equals, test.expect)
	}

	nilDatum := types.NewDatum(nil)
	f, err := fc.getFunction(datumsToConstants([]types.Datum{nilDatum}), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)
}
