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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

var _ = Suite(&testDateAddSuite{})

type testDateAddSuite struct {
}

func (t *testDateAddSuite) TestDateAdd(c *C) {
	input := "2011-11-11 10:10:10"
	e := &DateAdd{
		Unit:     "DAY",
		Date:     Value{Val: input},
		Interval: Value{Val: "1"},
	}
	c.Assert(e.String(), Equals, `DATE_ADD("2011-11-11 10:10:10", INTERVAL "1" DAY)`)
	c.Assert(e.Clone(), NotNil)
	c.Assert(e.IsStatic(), IsTrue)

	_, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)

	// Test null.
	e = &DateAdd{
		Unit:     "DAY",
		Date:     Value{Val: nil},
		Interval: Value{Val: "1"},
	}

	v, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	e = &DateAdd{
		Unit:     "DAY",
		Date:     Value{Val: input},
		Interval: Value{Val: nil},
	}

	v, err = e.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	// Test eval.
	tbl := []struct {
		Unit     string
		Interval interface{}
		Expect   string
	}{
		{"MICROSECOND", "1000", "2011-11-11 10:10:10.001000"},
		{"MICROSECOND", 1000, "2011-11-11 10:10:10.001000"},
		{"SECOND", "10", "2011-11-11 10:10:20"},
		{"MINUTE", "10", "2011-11-11 10:20:10"},
		{"HOUR", "10", "2011-11-11 20:10:10"},
		{"DAY", "11", "2011-11-22 10:10:10"},
		{"WEEK", "2", "2011-11-25 10:10:10"},
		{"MONTH", "2", "2012-01-11 10:10:10"},
		{"QUARTER", "4", "2012-11-11 10:10:10"},
		{"YEAR", "2", "2013-11-11 10:10:10"},
		{"SECOND_MICROSECOND", "10.00100000", "2011-11-11 10:10:20.100000"},
		{"SECOND_MICROSECOND", "10.0010000000", "2011-11-11 10:10:30"},
		{"SECOND_MICROSECOND", "10.0010000010", "2011-11-11 10:10:30.000010"},
		{"MINUTE_MICROSECOND", "10:10.100", "2011-11-11 10:20:20.100000"},
		{"MINUTE_SECOND", "10:10", "2011-11-11 10:20:20"},
		{"HOUR_MICROSECOND", "10:10:10.100", "2011-11-11 20:20:20.100000"},
		{"HOUR_SECOND", "10:10:10", "2011-11-11 20:20:20"},
		{"HOUR_MINUTE", "10:10", "2011-11-11 20:20:10"},
		{"DAY_MICROSECOND", "11 10:10:10.100", "2011-11-22 20:20:20.100000"},
		{"DAY_SECOND", "11 10:10:10", "2011-11-22 20:20:20"},
		{"DAY_MINUTE", "11 10:10", "2011-11-22 20:20:10"},
		{"DAY_HOUR", "11 10", "2011-11-22 20:10:10"},
		{"YEAR_MONTH", "11-1", "2022-12-11 10:10:10"},
		{"YEAR_MONTH", "11-11", "2023-10-11 10:10:10"},
	}

	for _, t := range tbl {
		e := &DateAdd{
			Unit:     t.Unit,
			Date:     Value{Val: input},
			Interval: Value{Val: t.Interval},
		}

		v, err := e.Eval(nil, nil)
		c.Assert(err, IsNil)

		value, ok := v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.Expect)
	}

	// Test error.
	errInput := "20111111 10:10:10"
	errTbl := []struct {
		Unit     string
		Interval interface{}
	}{
		{"MICROSECOND", "abc1000"},
		{"MICROSECOND", ""},
		{"SECOND_MICROSECOND", "10"},
		{"MINUTE_MICROSECOND", "10.0000"},
		{"MINUTE_MICROSECOND", "10:10:10.0000"},

		// MySQL support, but tidb not.
		{"HOUR_MICROSECOND", "10:10.0000"},
		{"YEAR_MONTH", "10 1"},
	}

	for _, t := range errTbl {
		e := &DateAdd{
			Unit:     t.Unit,
			Date:     Value{Val: input},
			Interval: Value{Val: t.Interval},
		}

		_, err := e.Eval(nil, nil)
		c.Assert(err, NotNil)

		e = &DateAdd{
			Unit:     t.Unit,
			Date:     Value{Val: errInput},
			Interval: Value{Val: t.Interval},
		}

		v, err := e.Eval(nil, nil)
		c.Assert(err, NotNil, Commentf("%s", v))
	}
}
