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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types_test

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTimeFormatMethod(t *testing.T) {
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
	for i, tt := range tblDate {
		tm, err := types.ParseTime(sc, tt.Input, mysql.TypeDatetime, 6)
		require.NoErrorf(t, err, "Parse time fail: %s", tt.Input)

		str, err := tm.DateFormat(tt.Format)
		require.NoErrorf(t, err, "time format fail: %d", i)
		require.Equalf(t, tt.Expect, str, "no.%d \nobtain:%v \nexpect:%v\n", i, str, tt.Expect)
	}
}

func TestStrToDate(t *testing.T) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	sc.IgnoreZeroInDate = true
	tests := []struct {
		input  string
		format string
		expect types.CoreTime
	}{
		{`01,05,2013`, `%d,%m,%Y`, types.FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`5 12 2021`, `%m%d%Y`, types.FromDate(2021, 5, 12, 0, 0, 0, 0)},
		{`May 01, 2013`, `%M %d,%Y`, types.FromDate(2013, 5, 1, 0, 0, 0, 0)},
		{`a09:30:17`, `a%h:%i:%s`, types.FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`09:30:17a`, `%h:%i:%s`, types.FromDate(0, 0, 0, 9, 30, 17, 0)},
		{`12:43:24`, `%h:%i:%s`, types.FromDate(0, 0, 0, 0, 43, 24, 0)},
		{`abc`, `abc`, types.ZeroCoreTime},
		{`09`, `%m`, types.FromDate(0, 9, 0, 0, 0, 0, 0)},
		{`09`, `%s`, types.FromDate(0, 0, 0, 0, 0, 9, 0)},
		{`12:43:24 AM`, `%r`, types.FromDate(0, 0, 0, 0, 43, 24, 0)},
		{`12:43:24 PM`, `%r`, types.FromDate(0, 0, 0, 12, 43, 24, 0)},
		{`11:43:24 PM`, `%r`, types.FromDate(0, 0, 0, 23, 43, 24, 0)},
		{`00:12:13`, `%T`, types.FromDate(0, 0, 0, 0, 12, 13, 0)},
		{`23:59:59`, `%T`, types.FromDate(0, 0, 0, 23, 59, 59, 0)},
		{`00/00/0000`, `%m/%d/%Y`, types.ZeroCoreTime},
		{`04/30/2004`, `%m/%d/%Y`, types.FromDate(2004, 4, 30, 0, 0, 0, 0)},
		{`15:35:00`, `%H:%i:%s`, types.FromDate(0, 0, 0, 15, 35, 0, 0)},
		{`Jul 17 33`, `%b %k %S`, types.FromDate(0, 7, 0, 17, 0, 33, 0)},
		{`2016-January:7 432101`, `%Y-%M:%l %f`, types.FromDate(2016, 1, 0, 7, 0, 0, 432101)},
		{`10:13 PM`, `%l:%i %p`, types.FromDate(0, 0, 0, 22, 13, 0, 0)},
		{`12:00:00 AM`, `%h:%i:%s %p`, types.FromDate(0, 0, 0, 0, 0, 0, 0)},
		{`12:00:00 PM`, `%h:%i:%s %p`, types.FromDate(0, 0, 0, 12, 0, 0, 0)},
		{`12:00:00 PM`, `%I:%i:%s %p`, types.FromDate(0, 0, 0, 12, 0, 0, 0)},
		{`1:00:00 PM`, `%h:%i:%s %p`, types.FromDate(0, 0, 0, 13, 0, 0, 0)},
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
		{`09/10/1021`, `%d/%m/%y`, types.FromDate(2010, 10, 9, 0, 0, 0, 0)}, // '%y' only accept up to 2 digits for year
		{`09/10/1021`, `%d/%m/%Y`, types.FromDate(1021, 10, 9, 0, 0, 0, 0)}, // '%Y' accept up to 4 digits for year
		{`09/10/10`, `%d/%m/%Y`, types.FromDate(2010, 10, 9, 0, 0, 0, 0)},   // '%Y' will fix the year for only 2 digits
		//'%b'/'%M' should be case insensitive
		{"31/may/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", types.FromDate(2016, 5, 31, 12, 34, 56, 123400)},
		{"30/april/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", types.FromDate(2016, 4, 30, 12, 34, 56, 0)},
		{"31/mAy/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", types.FromDate(2016, 5, 31, 12, 34, 56, 123400)},
		{"30/apRil/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", types.FromDate(2016, 4, 30, 12, 34, 56, 0)},
		// '%r'
		{" 04 :13:56 AM13/05/2019", "%r %d/%c/%Y", types.FromDate(2019, 5, 13, 4, 13, 56, 0)},  //
		{"12: 13:56 AM 13/05/2019", "%r%d/%c/%Y", types.FromDate(2019, 5, 13, 0, 13, 56, 0)},   //
		{"12:13 :56 pm 13/05/2019", "%r %d/%c/%Y", types.FromDate(2019, 5, 13, 12, 13, 56, 0)}, //
		{"12:3: 56pm  13/05/2019", "%r %d/%c/%Y", types.FromDate(2019, 5, 13, 12, 3, 56, 0)},   //
		{"11:13:56", "%r", types.FromDate(0, 0, 0, 11, 13, 56, 0)},                             // EOF before parsing "AM"/"PM"
		{"11:13", "%r", types.FromDate(0, 0, 0, 11, 13, 0, 0)},                                 // EOF after hh:mm
		{"11:", "%r", types.FromDate(0, 0, 0, 11, 0, 0, 0)},                                    // EOF after hh:
		{"11", "%r", types.FromDate(0, 0, 0, 11, 0, 0, 0)},                                     // EOF after hh:
		{"12", "%r", types.FromDate(0, 0, 0, 0, 0, 0, 0)},                                      // EOF after hh:, and hh=12 -> 0
		// '%T'
		{" 4 :13:56 13/05/2019", "%T %d/%c/%Y", types.FromDate(2019, 5, 13, 4, 13, 56, 0)},
		{"23: 13:56  13/05/2019", "%T%d/%c/%Y", types.FromDate(2019, 5, 13, 23, 13, 56, 0)},
		{"12:13 :56 13/05/2019", "%T %d/%c/%Y", types.FromDate(2019, 5, 13, 12, 13, 56, 0)},
		{"19:3: 56  13/05/2019", "%T %d/%c/%Y", types.FromDate(2019, 5, 13, 19, 3, 56, 0)},
		{"21:13", "%T", types.FromDate(0, 0, 0, 21, 13, 0, 0)}, // EOF after hh:mm
		{"21:", "%T", types.FromDate(0, 0, 0, 21, 0, 0, 0)},    // EOF after hh:
		// More patterns than input string
		{" 2/Jun", "%d/%b/%Y", types.FromDate(0, 6, 2, 0, 0, 0, 0)},
		{" liter", "lit era l", types.ZeroCoreTime},
		// Feb 29 in leap-year
		{"29/Feb/2020 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", types.FromDate(2020, 2, 29, 12, 34, 56, 0)},
		// When `AllowInvalidDate` is true, check only that the month is in the range from 1 to 12 and the day is in the range from 1 to 31
		{"31/April/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", types.FromDate(2016, 4, 31, 12, 34, 56, 0)},        // April 31th
		{"29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f", types.FromDate(2021, 2, 29, 12, 34, 56, 0)},          // Feb 29 in non-leap-year
		{"30/Feb/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", types.FromDate(2016, 2, 30, 12, 34, 56, 123400)}, // Feb 30th
	}
	for i, tt := range tests {
		sc.AllowInvalidDate = true
		var time types.Time
		require.Truef(t, time.StrToDate(sc, tt.input, tt.format), "no.%d failed input=%s format=%s", i, tt.input, tt.format)
		require.Equalf(t, tt.expect, time.CoreTime(), "no.%d failed input=%s format=%s", i, tt.input, tt.format)
	}

	errTests := []struct {
		input  string
		format string
	}{
		// invalid days when `AllowInvalidDate` is false
		{`04/31/2004`, `%m/%d/%Y`},                        // not exists in the real world
		{"29/Feb/2021 12:34:56.", "%d/%b/%Y %H:%i:%s.%f"}, // Feb 29 in non-leap-year

		{`512 2021`, `%m%d %Y`}, // MySQL will try to parse '51' for '%m', fail

		{`a09:30:17`, `%h:%i:%s`}, // format mismatch
		{`12:43:24 a`, `%r`},      // followed by incomplete 'AM'/'PM'
		{`23:60:12`, `%T`},        // invalid minute
		{`18`, `%l`},
		{`00:21:22 AM`, `%h:%i:%s %p`},
		{`100/10/22`, `%y/%m/%d`},
		{"2010-11-12 11 am", `%Y-%m-%d %H %p`},
		{"2010-11-12 13 am", `%Y-%m-%d %h %p`},
		{"2010-11-12 0 am", `%Y-%m-%d %h %p`},
		// MySQL accept `SEPTEMB` as `SEPTEMBER`, but we don't want this "feature" in TiDB
		// unless we have to.
		{"15 SEPTEMB 2001", "%d %M %Y"},
		// '%r'
		{"13:13:56 AM13/5/2019", "%r"},  // hh = 13 with am is invalid
		{"00:13:56 AM13/05/2019", "%r"}, // hh = 0 with am is invalid
		{"00:13:56 pM13/05/2019", "%r"}, // hh = 0 with pm is invalid
		{"11:13:56a", "%r"},             // EOF while parsing "AM"/"PM"
	}
	for i, tt := range errTests {
		sc.AllowInvalidDate = false
		var time types.Time
		require.Falsef(t, time.StrToDate(sc, tt.input, tt.format), "no.%d failed input=%s format=%s", i, tt.input, tt.format)
	}
}
