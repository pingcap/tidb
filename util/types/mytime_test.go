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

func (s *testMyTimeSuite) TestWeekBehaviour(c *C) {
	c.Assert(weekBehaviourMondayFirst, Equals, weekBehaviour(0))
	c.Assert(weekBehaviourWeekYear, Equals, weekBehaviour(1))
	c.Assert(weekBehaviourWeekFirstWeekday, Equals, weekBehaviour(2))

	c.Check(weekBehaviour(0).test(weekBehaviourMondayFirst), IsTrue)
	c.Check(weekBehaviour(1).test(weekBehaviourWeekYear), IsTrue)
	c.Check(weekBehaviour(2).test(weekBehaviourWeekFirstWeekday), IsTrue)
}

func (s *testMyTimeSuite) TestWeek(c *C) {
	cases := []struct {
		Input  mysqlTime
		Mode   weekBehaviour
		Expect int
	}{
		{mysqlTime{2008, 2, 20, 0, 0, 0, 0}, 0, 7},
		{mysqlTime{2008, 2, 20, 1, 0, 0, 0}, 0, 8},
		{mysqlTime{2008, 12, 53, 1, 0, 0, 0}, 0, 53},
	}

	for ith, t := range cases {
		var year int
		week := calcWeek(&t.Input, t.Mode, &year)
		c.Check(week, Equals, t.Expect, Commentf("%d failed.", ith))
	}
}
