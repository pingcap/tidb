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
	cases := []struct {
		Input  mysqlTime
		Mode   int
		Expect int
	}{
		{mysqlTime{2008, 2, 20, 0, 0, 0, 0}, 0, 7},
		{mysqlTime{2008, 2, 20, 0, 0, 0, 0}, 1, 8},
		{mysqlTime{2008, 12, 31, 0, 0, 0, 0}, 1, 53},
	}

	for ith, t := range cases {
		_, week := calcWeek(&t.Input, weekMode(t.Mode))
		c.Check(week, Equals, t.Expect, Commentf("%d failed.", ith))
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
	cases := []struct {
		T1     mysqlTime
		T2     mysqlTime
		Sign   int
		Expect mysqlTime
	}{
		// calcTimeDiff can be used for month = 0.
		{
			mysqlTime{2006, 0, 1, 12, 23, 21, 0},
			mysqlTime{2006, 0, 3, 21, 23, 22, 0},
			1,
			mysqlTime{0, 0, 0, 57, 0, 1, 0},
		},
		{
			mysqlTime{0, 0, 0, 21, 23, 24, 0},
			mysqlTime{0, 0, 0, 11, 23, 22, 0},
			1,
			mysqlTime{0, 0, 0, 10, 0, 2, 0},
		},
	}

	for i, t := range cases {
		seconds, microseconds, _ := calcTimeDiff(&t.T1, &t.T2, t.Sign)
		var result mysqlTime
		calcTimeFromSec(&result, seconds, microseconds)
		c.Assert(result, Equals, t.Expect, Commentf("%d failed.", i))
	}
}

func (s *testMyTimeSuite) TestCompareTime(c *C) {
	cases := []struct {
		T1     mysqlTime
		T2     mysqlTime
		Expect int
	}{
		{mysqlTime{0, 0, 0, 0, 0, 0, 0}, mysqlTime{0, 0, 0, 0, 0, 0, 0}, 0},
		{mysqlTime{0, 0, 0, 0, 1, 0, 0}, mysqlTime{0, 0, 0, 0, 0, 0, 0}, 1},
		{mysqlTime{2006, 1, 2, 3, 4, 5, 6}, mysqlTime{2016, 1, 2, 3, 4, 5, 0}, -1},
		{mysqlTime{0, 0, 0, 11, 22, 33, 0}, mysqlTime{0, 0, 0, 12, 21, 33, 0}, -1},
		{mysqlTime{9999, 12, 30, 23, 59, 59, 999999}, mysqlTime{0, 1, 2, 3, 4, 5, 6}, 1},
	}

	for _, t := range cases {
		c.Assert(compareTime(&t.T1, &t.T2), Equals, t.Expect)
		c.Assert(compareTime(&t.T2, &t.T1), Equals, -t.Expect)
	}
}

func (s *testMyTimeSuite) TestGetDateFromDaynr(c *C) {
	cases := []struct {
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

	for _, t := range cases {
		yy, mm, dd := getDateFromDaynr(t.daynr)
		c.Assert(yy, Equals, t.year)
		c.Assert(mm, Equals, t.month)
		c.Assert(dd, Equals, t.day)
	}
}

func (s *testMyTimeSuite) TestMixDateAndTime(c *C) {
	cases := []struct {
		date   mysqlTime
		time   mysqlTime
		neg    bool
		expect mysqlTime
	}{
		{
			date:   mysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   mysqlTime{0, 0, 0, 12, 23, 24, 5},
			neg:    false,
			expect: mysqlTime{1896, 3, 4, 12, 23, 24, 5},
		},
		{
			date:   mysqlTime{1896, 3, 4, 0, 0, 0, 0},
			time:   mysqlTime{0, 0, 0, 24, 23, 24, 5},
			neg:    false,
			expect: mysqlTime{1896, 3, 5, 0, 23, 24, 5},
		},
		{
			date:   mysqlTime{2016, 12, 31, 0, 0, 0, 0},
			time:   mysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: mysqlTime{2017, 1, 1, 0, 0, 0, 0},
		},
		{
			date:   mysqlTime{2016, 12, 0, 0, 0, 0, 0},
			time:   mysqlTime{0, 0, 0, 24, 0, 0, 0},
			neg:    false,
			expect: mysqlTime{2016, 12, 1, 0, 0, 0, 0},
		},
		{
			date:   mysqlTime{2017, 1, 12, 3, 23, 15, 0},
			time:   mysqlTime{0, 0, 0, 2, 21, 10, 0},
			neg:    true,
			expect: mysqlTime{2017, 1, 12, 1, 2, 5, 0},
		},
	}

	for ith, t := range cases {
		mixDateAndTime(&t.date, &t.time, t.neg)
		c.Assert(compareTime(&t.date, &t.expect), Equals, 0, Commentf("%d", ith))
	}
}
