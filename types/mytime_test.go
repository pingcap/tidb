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
	"time"
	"unsafe"

	. "github.com/pingcap/check"
)

type testMyTimeSuite struct{}

var _ = Suite(&testMyTimeSuite{})

func (s *testMyTimeSuite) TestWeekBehaviour(c *C) {
	c.Assert(weekBehaviourMondayFirst, Equals, weekBehaviour(1))
	c.Assert(weekBehaviourYear, Equals, weekBehaviour(2))
	c.Assert(weekBehaviourFirstWeekday, Equals, weekBehaviour(4))

	c.Check(weekBehaviour(1).test(weekBehaviourMondayFirst), IsTrue)
	c.Check(weekBehaviour(2).test(weekBehaviourYear), IsTrue)
	c.Check(weekBehaviour(4).test(weekBehaviourFirstWeekday), IsTrue)
}

func (s *testMyTimeSuite) TestWeek(c *C) {
	tests := []struct {
		Input  MysqlTime
		Mode   int
		Expect int
	}{
		{MysqlTime{year: 2008, month: 2, day: 20, hour: 0, minute: 0, second: 0, microsecond: 0}, 0, 7},
		{MysqlTime{year: 2008, month: 2, day: 20, hour: 0, minute: 0, second: 0, microsecond: 0}, 1, 8},
		{MysqlTime{year: 2008, month: 12, day: 31, hour: 0, minute: 0, second: 0, microsecond: 0}, 1, 53},
	}

	for ith, tt := range tests {
		_, week := calcWeek(&tt.Input, weekMode(tt.Mode))
		c.Check(week, Equals, tt.Expect, Commentf("%d failed.", ith))
	}
}

func (s *testMyTimeSuite) TestCalcDaynr(c *C) {
	c.Assert(calcDaynr(0, 0, 0), Equals, 0)
	c.Assert(calcDaynr(9999, 12, 31), Equals, 3652424)
	c.Assert(calcDaynr(1970, 1, 1), Equals, 719528)
	c.Assert(calcDaynr(2006, 12, 16), Equals, 733026)
	c.Assert(calcDaynr(10, 1, 2), Equals, 3654)
	c.Assert(calcDaynr(2008, 2, 20), Equals, 733457)
}

func (s *testMyTimeSuite) TestCalcTimeDiff(c *C) {
	tests := []struct {
		T1     MysqlTime
		T2     MysqlTime
		Sign   int
		Expect MysqlTime
	}{
		// calcTimeDiff can be used for month = 0.
		{
			MysqlTime{year: 2006, month: 0, day: 1, hour: 12, minute: 23, second: 21, microsecond: 0},
			MysqlTime{year: 2006, month: 0, day: 3, hour: 21, minute: 23, second: 22, microsecond: 0},
			1,
			MysqlTime{year: 0, month: 0, day: 0, hour: 57, minute: 0, second: 1, microsecond: 0},
		},
		{
			MysqlTime{year: 0, month: 0, day: 0, hour: 21, minute: 23, second: 24, microsecond: 0},
			MysqlTime{year: 0, month: 0, day: 0, hour: 11, minute: 23, second: 22, microsecond: 0},
			1,
			MysqlTime{year: 0, month: 0, day: 0, hour: 10, minute: 0, second: 2, microsecond: 0},
		},
		{
			MysqlTime{year: 0, month: 0, day: 0, hour: 1, minute: 2, second: 3, microsecond: 0},
			MysqlTime{year: 0, month: 0, day: 0, hour: 5, minute: 2, second: 0, microsecond: 0},
			-1,
			MysqlTime{year: 0, month: 0, day: 0, hour: 6, minute: 4, second: 3, microsecond: 0},
		},
	}

	for i, tt := range tests {
		seconds, microseconds, _ := calcTimeDiff(tt.T1, tt.T2, tt.Sign)
		var result MysqlTime
		calcTimeFromSec(&result, seconds, microseconds)
		c.Assert(result, Equals, tt.Expect, Commentf("%d failed.", i))
	}
}

func (s *testMyTimeSuite) TestCompareTime(c *C) {
	tests := []struct {
		T1     MysqlTime
		T2     MysqlTime
		Expect int
	}{
		{MysqlTime{year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0, microsecond: 0}, MysqlTime{year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0, microsecond: 0}, 0},
		{MysqlTime{year: 0, month: 0, day: 0, hour: 0, minute: 1, second: 0, microsecond: 0}, MysqlTime{year: 0, month: 0, day: 0, hour: 0, minute: 0, second: 0, microsecond: 0}, 1},
		{MysqlTime{year: 2006, month: 1, day: 2, hour: 3, minute: 4, second: 5, microsecond: 6}, MysqlTime{year: 2016, month: 1, day: 2, hour: 3, minute: 4, second: 5, microsecond: 0}, -1},
		{MysqlTime{year: 0, month: 0, day: 0, hour: 11, minute: 22, second: 33, microsecond: 0}, MysqlTime{year: 0, month: 0, day: 0, hour: 12, minute: 21, second: 33, microsecond: 0}, -1},
		{MysqlTime{year: 9999, month: 12, day: 30, hour: 23, minute: 59, second: 59, microsecond: 999999}, MysqlTime{year: 0, month: 1, day: 2, hour: 3, minute: 4, second: 5, microsecond: 6}, 1},
	}

	for _, tt := range tests {
		c.Assert(compareTime(tt.T1, tt.T2), Equals, tt.Expect)
		c.Assert(compareTime(tt.T2, tt.T1), Equals, -tt.Expect)
	}
}

func (s *testMyTimeSuite) TestGetDateFromDaynr(c *C) {
	tests := []struct {
		daynr uint
		year  uint
		month uint
		day   uint
	}{
		{730669, 2000, 7, 3},
		{720195, 1971, 10, 30},
		{719528, 1970, 01, 01},
		{719892, 1970, 12, 31},
		{730850, 2000, 12, 31},
		{730544, 2000, 2, 29},
		{204960, 561, 2, 28},
		{0, 0, 0, 0},
		{32, 0, 0, 0},
		{366, 1, 1, 1},
		{744729, 2038, 12, 31},
		{3652424, 9999, 12, 31},
	}

	for _, tt := range tests {
		yy, mm, dd := getDateFromDaynr(tt.daynr)
		c.Assert(yy, Equals, tt.year)
		c.Assert(mm, Equals, tt.month)
		c.Assert(dd, Equals, tt.day)
	}
}

func (s *testMyTimeSuite) TestMixDateAndTime(c *C) {
	tests := []struct {
		date   MysqlTime
		time   MysqlTime
		neg    bool
		expect MysqlTime
	}{
		{
			date:   MysqlTime{year: 1896, month: 3, day: 4, hour: 0, minute: 0, second: 0, microsecond: 0},
			time:   MysqlTime{year: 0, month: 0, day: 0, hour: 12, minute: 23, second: 24, microsecond: 5},
			neg:    false,
			expect: MysqlTime{year: 1896, month: 3, day: 4, hour: 12, minute: 23, second: 24, microsecond: 5},
		},
		{
			date:   MysqlTime{year: 1896, month: 3, day: 4, hour: 0, minute: 0, second: 0, microsecond: 0},
			time:   MysqlTime{year: 0, month: 0, day: 0, hour: 24, minute: 23, second: 24, microsecond: 5},
			neg:    false,
			expect: MysqlTime{year: 1896, month: 3, day: 5, hour: 0, minute: 23, second: 24, microsecond: 5},
		},
		{
			date:   MysqlTime{year: 2016, month: 12, day: 31, hour: 0, minute: 0, second: 0, microsecond: 0},
			time:   MysqlTime{year: 0, month: 0, day: 0, hour: 24, minute: 0, second: 0, microsecond: 0},
			neg:    false,
			expect: MysqlTime{year: 2017, month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: 0},
		},
		{
			date:   MysqlTime{year: 2016, month: 12, day: 0, hour: 0, minute: 0, second: 0, microsecond: 0},
			time:   MysqlTime{year: 0, month: 0, day: 0, hour: 24, minute: 0, second: 0, microsecond: 0},
			neg:    false,
			expect: MysqlTime{year: 2016, month: 12, day: 1, hour: 0, minute: 0, second: 0, microsecond: 0},
		},
		{
			date:   MysqlTime{year: 2017, month: 1, day: 12, hour: 3, minute: 23, second: 15, microsecond: 0},
			time:   MysqlTime{year: 0, month: 0, day: 0, hour: 2, minute: 21, second: 10, microsecond: 0},
			neg:    true,
			expect: MysqlTime{year: 2017, month: 1, day: 12, hour: 1, minute: 2, second: 5, microsecond: 0},
		},
	}

	for ith, t := range tests {
		mixDateAndTime(&t.date, &t.time, t.neg)
		c.Assert(compareTime(t.date, t.expect), Equals, 0, Commentf("%d", ith))
	}
}

func (s *testMyTimeSuite) TestIsLeapYear(c *C) {
	tests := []struct {
		T      MysqlTime
		Expect bool
	}{
		{MysqlTime{year: 1960, month: 1, day: 1, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 1963, month: 2, day: 21, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
		{MysqlTime{year: 2008, month: 11, day: 25, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 2017, month: 4, day: 24, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
		{MysqlTime{year: 1988, month: 2, day: 29, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 2000, month: 3, day: 15, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 1992, month: 5, day: 3, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 2024, month: 10, day: 1, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 2016, month: 6, day: 29, hour: 0, minute: 0, second: 0, microsecond: 0}, true},
		{MysqlTime{year: 2015, month: 6, day: 29, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
		{MysqlTime{year: 2014, month: 9, day: 31, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
		{MysqlTime{year: 2001, month: 12, day: 7, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
		{MysqlTime{year: 1989, month: 7, day: 6, hour: 0, minute: 0, second: 0, microsecond: 0}, false},
	}

	for _, tt := range tests {
		c.Assert(tt.T.IsLeapYear(), Equals, tt.Expect)
	}
}
func (s *testMyTimeSuite) TestGetLastDay(c *C) {
	tests := []struct {
		year        int
		month       int
		expectedDay int
	}{
		{2000, 1, 31},
		{2000, 2, 29},
		{2000, 4, 30},
		{1900, 2, 28},
		{1996, 2, 29},
	}

	for _, t := range tests {
		day := GetLastDay(t.year, t.month)
		c.Assert(day, Equals, t.expectedDay)
	}
}

func (s *testMyTimeSuite) TestgetFixDays(c *C) {
	tests := []struct {
		year        int
		month       int
		day         int
		ot          time.Time
		expectedDay int
	}{
		{2000, 1, 0, time.Date(2000, 1, 31, 0, 0, 0, 0, time.UTC), -2},
		{2000, 1, 12, time.Date(2000, 1, 31, 0, 0, 0, 0, time.UTC), 0},
		{2000, 1, 12, time.Date(2000, 1, 0, 0, 0, 0, 0, time.UTC), 0},
		{2000, 2, 24, time.Date(2000, 2, 10, 0, 0, 0, 0, time.UTC), 0},
		{2019, 04, 05, time.Date(2019, 04, 01, 1, 2, 3, 4, time.UTC), 0},
	}

	for _, t := range tests {
		res := getFixDays(t.year, t.month, t.day, t.ot)
		c.Assert(res, Equals, t.expectedDay)
	}
}

func (s *testMyTimeSuite) TestAddDate(c *C) {
	tests := []struct {
		year  int
		month int
		day   int
		ot    time.Time
	}{
		{01, 1, 0, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{02, 1, 12, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{03, 1, 12, time.Date(2000, 1, 01, 0, 0, 0, 0, time.UTC)},
		{04, 2, 24, time.Date(2000, 2, 10, 0, 0, 0, 0, time.UTC)},
		{01, 04, 05, time.Date(2019, 04, 01, 1, 2, 3, 4, time.UTC)},
	}

	for _, t := range tests {
		res := AddDate(int64(t.year), int64(t.month), int64(t.day), t.ot)
		c.Assert(res.Year(), Equals, t.year+t.ot.Year())
	}
}

func (s *testMyTimeSuite) TestWeekday(c *C) {
	tests := []struct {
		Input  MysqlTime
		Expect string
	}{
		{MysqlTime{year: 2019, month: 01, day: 01, hour: 0, minute: 0, second: 0, microsecond: 0}, "Tuesday"},
		{MysqlTime{year: 2019, month: 02, day: 31, hour: 0, minute: 0, second: 0, microsecond: 0}, "Sunday"},
		{MysqlTime{year: 2019, month: 04, day: 31, hour: 0, minute: 0, second: 0, microsecond: 0}, "Wednesday"},
	}

	for _, tt := range tests {
		weekday := tt.Input.Weekday()
		c.Check(weekday.String(), Equals, tt.Expect)
	}
}

func (s *testMyTimeSuite) TestTimeStructSize(c *C) {
	c.Assert(unsafe.Sizeof(MysqlTime{}), Equals, uintptr(0x10))
	c.Assert(unsafe.Sizeof(Time{}), Equals, uintptr(0x14))
}
