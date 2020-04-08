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
	"time"
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
		{MysqlTime{2008, 2, 20, 0, 0, 0, 0}, 0, 7},
		{MysqlTime{2008, 2, 20, 0, 0, 0, 0}, 1, 8},
		{MysqlTime{2008, 12, 31, 0, 0, 0, 0}, 1, 53},
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
			MysqlTime{2006, 0, 1, 12, 23, 21, 0},
			MysqlTime{2006, 0, 3, 21, 23, 22, 0},
			1,
			MysqlTime{0, 0, 0, 57, 0, 1, 0},
		},
		{
			MysqlTime{0, 0, 0, 21, 23, 24, 0},
			MysqlTime{0, 0, 0, 11, 23, 22, 0},
			1,
			MysqlTime{0, 0, 0, 10, 0, 2, 0},
		},
		{
			MysqlTime{0, 0, 0, 1, 2, 3, 0},
			MysqlTime{0, 0, 0, 5, 2, 0, 0},
			-1,
			MysqlTime{0, 0, 0, 6, 4, 3, 0},
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
		{MysqlTime{0, 0, 0, 0, 0, 0, 0}, MysqlTime{0, 0, 0, 0, 0, 0, 0}, 0},
		{MysqlTime{0, 0, 0, 0, 1, 0, 0}, MysqlTime{0, 0, 0, 0, 0, 0, 0}, 1},
		{MysqlTime{2006, 1, 2, 3, 4, 5, 6}, MysqlTime{2016, 1, 2, 3, 4, 5, 0}, -1},
		{MysqlTime{0, 0, 0, 11, 22, 33, 0}, MysqlTime{0, 0, 0, 12, 21, 33, 0}, -1},
		{MysqlTime{9999, 12, 30, 23, 59, 59, 999999}, MysqlTime{0, 1, 2, 3, 4, 5, 6}, 1},
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
			date:   MysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 12, 23, 24, 5},
			neg:    false,
			expect: MysqlTime{1896, 3, 4, 12, 23, 24, 5},
		},
		{
			date:   MysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 23, 24, 5},
			neg:    false,
			expect: MysqlTime{1896, 3, 5, 0, 23, 24, 5},
		},
		{
			date:   MysqlTime{2016, 12, 31, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: MysqlTime{2017, 1, 1, 0, 0, 0, 0},
		},
		{
			date:   MysqlTime{2016, 12, 0, 0, 0, 0, 0},
			time:   MysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: MysqlTime{2016, 12, 1, 0, 0, 0, 0},
		},
		{
			date:   MysqlTime{2017, 1, 12, 3, 23, 15, 0},
			time:   MysqlTime{0, 0, 0, 2, 21, 10, 0},
			neg:    true,
			expect: MysqlTime{2017, 1, 12, 1, 2, 5, 0},
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
		{MysqlTime{1960, 1, 1, 0, 0, 0, 0}, true},
		{MysqlTime{1963, 2, 21, 0, 0, 0, 0}, false},
		{MysqlTime{2008, 11, 25, 0, 0, 0, 0}, true},
		{MysqlTime{2017, 4, 24, 0, 0, 0, 0}, false},
		{MysqlTime{1988, 2, 29, 0, 0, 0, 0}, true},
		{MysqlTime{2000, 3, 15, 0, 0, 0, 0}, true},
		{MysqlTime{1992, 5, 3, 0, 0, 0, 0}, true},
		{MysqlTime{2024, 10, 1, 0, 0, 0, 0}, true},
		{MysqlTime{2016, 6, 29, 0, 0, 0, 0}, true},
		{MysqlTime{2015, 6, 29, 0, 0, 0, 0}, false},
		{MysqlTime{2014, 9, 31, 0, 0, 0, 0}, false},
		{MysqlTime{2001, 12, 7, 0, 0, 0, 0}, false},
		{MysqlTime{1989, 7, 6, 0, 0, 0, 0}, false},
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
		{MysqlTime{2019, 01, 01, 0, 0, 0, 0}, "Tuesday"},
		{MysqlTime{2019, 02, 31, 0, 0, 0, 0}, "Sunday"},
		{MysqlTime{2019, 04, 31, 0, 0, 0, 0}, "Wednesday"},
	}

	for _, tt := range tests {
		weekday := tt.Input.Weekday()
		c.Check(weekday.String(), Equals, tt.Expect)
	}
}
