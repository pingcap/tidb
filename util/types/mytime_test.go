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
