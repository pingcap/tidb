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

package types_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testTimeSuite) TestTimeFormatMethod(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
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
			`Jan January 01 1 0th 00 0 000 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 00 00 52 Fri Friday 5 4294967295 4294967295 0000 00 %`,
		},
	}
	for i, t := range tblDate {
		tm, err := types.ParseTime(sc, t.Input, mysql.TypeDatetime, 6)
		c.Assert(err, IsNil, Commentf("parse time fail: %s", t.Input))

		str, err := tm.DateFormat(t.Format)
		c.Assert(err, IsNil, Commentf("time format fail: %d", i))
		c.Assert(str, Equals, t.Expect, Commentf("no.%d \nobtain:%v \nexpect:%v\n", i,
			str, t.Expect))
	}
}

func (s *testTimeSuite) TestStrToDate(c *C) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	tests := []struct {
		input  string
		format string
		expect types.MysqlTime
	}{
		{`01,05,2013`, `%d,%m,%Y`, types.FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`May 01, 2013`, `%M %d,%Y`, types.FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`a09:30:17`, `a%h:%i:%s`, types.FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`09:30:17a`, `%h:%i:%s`, types.FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`abc`, `abc`, types.ZeroTime},
		{`09`, `%m`, types.FromDate(0, 9, 0, 0, 0, 0, 0)},
		{`09`, `%s`, types.FromDate(0, 0, 0, 0, 0, 9, 0)},
		{`12:43:24 AM`, `%r`, types.FromDate(0, 0, 0, 12, 43, 24, 0)},
		{`11:43:24 PM`, `%r`, types.FromDate(0, 0, 0, 23, 43, 24, 0)},
		{`00:12:13`, `%T`, types.FromDate(0, 0, 0, 0, 12, 13, 0)},
		{`23:59:59`, `%T`, types.FromDate(0, 0, 0, 23, 59, 59, 0)},
		{`00/00/0000`, `%m/%d/%Y`, types.ZeroTime},
		{`04/30/2004`, `%m/%d/%Y`, types.FromDate(2004, 4, 30, 0, 0, 0, 0)},
		{`15:35:00`, `%H:%i:%s`, types.FromDate(0, 0, 0, 15, 35, 0, 0)},
		{`Jul 17 33`, `%b %k %S`, types.FromDate(0, 7, 0, 17, 0, 33, 0)},
		{`2016-January:7 432101`, `%Y-%M:%l %f`, types.FromDate(2016, 1, 0, 7, 0, 0, 432101)},
		{`10:13 PM`, `%l:%i %p`, types.FromDate(0, 0, 0, 22, 13, 0, 0)},
		{`12:00:00 AM`, `%h:%i:%s %p`, types.FromDate(0, 0, 0, 0, 0, 0, 0)},
		{`12:00:00 PM`, `%h:%i:%s %p`, types.FromDate(0, 0, 0, 12, 0, 0, 0)},
		{`18/10/22`, `%y/%m/%d`, types.FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`8/10/22`, `%y/%m/%d`, types.FromDate(2008, 10, 22, 0, 0, 0, 0)},
		{`69/10/22`, `%y/%m/%d`, types.FromDate(2069, 10, 22, 0, 0, 0, 0)},
		{`70/10/22`, `%y/%m/%d`, types.FromDate(1970, 10, 22, 0, 0, 0, 0)},
		{`18/10/22`, `%Y/%m/%d`, types.FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`2018/10/22`, `%Y/%m/%d`, types.FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`8/10/22`, `%Y/%m/%d`, types.FromDate(2008, 10, 22, 0, 0, 0, 0)},
		{`69/10/22`, `%Y/%m/%d`, types.FromDate(2069, 10, 22, 0, 0, 0, 0)},
		{`70/10/22`, `%Y/%m/%d`, types.FromDate(1970, 10, 22, 0, 0, 0, 0)},
		{`18/10/22`, `%Y/%m/%d`, types.FromDate(2018, 10, 22, 0, 0, 0, 0)},
		{`100/10/22`, `%Y/%m/%d`, types.FromDate(100, 10, 22, 0, 0, 0, 0)},
	}
	for i, tt := range tests {
		var t types.Time
		c.Assert(t.StrToDate(sc, tt.input, tt.format), IsTrue, Commentf("no.%d failed", i))
		c.Assert(t.Time, Equals, tt.expect, Commentf("no.%d failed", i))
	}

	errTests := []struct {
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
		{`100/10/22`, `%y/%m/%d`},
	}
	for _, tt := range errTests {
		var t types.Time
		c.Assert(t.StrToDate(sc, tt.input, tt.format), IsFalse)
	}
}
