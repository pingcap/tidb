// Copyright 2016 PingCAP, Inc.
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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

func (s *testTimeSuite) TestTimeFormatMethod(c *C) {
	tblDate := []struct {
		Input  string
		Format string
		Expect string
	}{
		{
			"2010-01-07 23:12:34.12345",
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			`Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %`,
		},
		{
			"2012-12-21 23:12:34.123456",
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			`Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %`,
		},
		{
			"0000-01-01 00:00:00.123456",
			// Functions week() and yearweek() don't support multi mode,
			// so the result of "%U %u %V %Y" is different from MySQL.
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y %y %%`,
			`Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000 00 %`,
		},
		{
			"2016-09-3 00:59:59.123456",
			`abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z`,
			`abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z`,
		},
		{
			"2012-10-01 00:00:00",
			`%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%`,
			`Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %`,
		},
		{
			// For invalid date month or year = 0, MySQL behavior is confusing, %U (which format Week()) is 52, but Week() is 0.
			// It's because in MySQL, Week() checks invalid date before processing, but DateFormat() don't.
			// So there are some difference to MySQL here (%U %u %V %v), TiDB user should not rely on those corner case behavior.
			// %W %w %a is not compatible in this case because Week() use GoTime() currently.
			"0000-01-00 00:00:00.123456",
			`%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%`,
			`Jan January 01 1 0th 00 0 000 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 00 00 52 Sun Sunday 0 4294967295 4294967295 0000 00 %`,
		},
	}
	for i, t := range tblDate {
		tm, err := ParseTime(t.Input, mysql.TypeDatetime, 6)
		c.Assert(err, IsNil, Commentf("parse time fail: %s", t.Input))

		str, err := tm.DateFormat(t.Format)
		c.Assert(err, IsNil, Commentf("time format fail: %d", i))
		c.Assert(str, Equals, t.Expect, Commentf("no.%d \nobtain:%v \nexpect:%v\n", i,
			str, t.Expect))
	}
}

func (s *testTimeSuite) TestStrToDate(c *C) {
	testcases := []struct {
		input  string
		format string
		expect TimeInternal
	}{
		{`01,05,2013`, `%d,%m,%Y`, FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`May 01, 2013`, `%M %d,%Y`, FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`a09:30:17`, `a%h:%i:%s`, FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`09:30:17a`, `%h:%i:%s`, FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`abc`, `abc`, ZeroTime},
		{`09`, `%m`, FromDate(0, 9, 0, 0, 0, 0, 0)},
		{`09`, `%s`, FromDate(0, 0, 0, 0, 0, 9, 0)},
		{`12:43:24 AM`, `%r`, FromDate(0, 0, 0, 12, 43, 24, 0)},
		{`11:43:24 PM`, `%r`, FromDate(0, 0, 0, 23, 43, 24, 0)},
		{`00:12:13`, `%T`, FromDate(0, 0, 0, 0, 12, 13, 0)},
		{`23:59:59`, `%T`, FromDate(0, 0, 0, 23, 59, 59, 0)},
		{`00/00/0000`, `%m/%d/%Y`, ZeroTime},
		{`04/30/2004`, `%m/%d/%Y`, FromDate(2004, 4, 30, 0, 0, 0, 0)},
		{`15:35:00`, `%H:%i:%s`, FromDate(0, 0, 0, 15, 35, 0, 0)},
		{`Jul 17 33`, `%b %k %S`, FromDate(0, 7, 0, 17, 0, 33, 0)},
		{`2016-January:7 432101`, `%Y-%M:%l %f`, FromDate(2016, 1, 0, 7, 0, 0, 432101)},
		{`10:13 PM`, `%l:%i %p`, FromDate(0, 0, 0, 22, 13, 0, 0)},
		{`12:00:00 AM`, `%h:%i:%s %p`, FromDate(0, 0, 0, 0, 0, 0, 0)},
		{`12:00:00 PM`, `%h:%i:%s %p`, FromDate(0, 0, 0, 12, 0, 0, 0)},
	}
	for i, test := range testcases {
		var t Time
		c.Assert(t.StrToDate(test.input, test.format), IsTrue, Commentf("no.%d failed", i))
		c.Assert(t.Time, Equals, test.expect, Commentf("no.%d failed", i))
	}

	errcases := []struct {
		input  string
		format string
	}{
		{`04/31/2004`, `%m/%d/%Y`},
		{`a09:30:17`, `%h:%i:%s`}, // format mismatch
		{`12:43:24 PM`, `%r`},
		{`12:43:24`, `%r`}, // no PM or AM followed
		{`23:60:12`, `%T`}, // invalid minute
		{`18`, `%l`},
		{`00:21:22 AM`, `%h:%i:%s %p`},
	}
	for _, test := range errcases {
		var t Time
		c.Assert(t.StrToDate(test.input, test.format), IsFalse)
	}
}
