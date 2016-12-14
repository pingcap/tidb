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
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
			"Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %",
		},
		{
			"2012-12-21 23:12:34.123456",
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%",
			"Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
		},
		{
			"0000-01-01 00:00:00.123456",
			// Functions week() and yearweek() don't support multi mode,
			// so the result of "%U %u %V %Y" is different from MySQL.
			"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y %y %%",
			"Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000 00 %",
		},
		{
			"2016-09-3 00:59:59.123456",
			"abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
			"abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
		},
		{
			"2012-10-01 00:00:00",
			"%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%",
			"Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %",
		},
	}
	for i, t := range tblDate {
		tm, err := ParseTime(t.Input, mysql.TypeDatetime, 6)
		c.Assert(err, IsNil, Commentf("parse time fail: %s", t.Input))

		str, err := tm.Format(t.Format)
		c.Assert(err, IsNil, Commentf("time format fail: %d", i))
		c.Assert(str, Equals, t.Expect, Commentf("no.%d \nobtain:%v \nexpect:%v\n", i,
			str, t.Expect))
	}

	// error
	// ds := types.MakeDatums("0000-01-00 00:00:00.123456",
	// 	"%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%")
	// _, err := builtinDateFormat(ds, s.ctx)
	// Some like dayofweek() doesn't support the date format like 2000-00-00 returns 0,
	// so it returns an error.
	// TODO: Fix here.
	// c.Assert(err, NotNil)
}
