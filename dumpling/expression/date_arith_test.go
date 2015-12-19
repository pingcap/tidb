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

var _ = Suite(&testDateArithSuite{})

type testDateArithSuite struct {
}

func (t *testDateArithSuite) TestDateArith(c *C) {
	input := "2011-11-11 10:10:10"
	e := &DateArith{
		Op:       DateAdd,
		Unit:     "DAY",
		Date:     Value{Val: input},
		Interval: Value{Val: "1"},
	}
	c.Assert(e.String(), Equals, `DATE_ADD("2011-11-11 10:10:10", INTERVAL "1" DAY)`)
	c.Assert(e.Clone(), NotNil)
	c.Assert(e.IsStatic(), IsTrue)
	_, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)
	e.Op = DateSub
	c.Assert(e.String(), Equals, `DATE_SUB("2011-11-11 10:10:10", INTERVAL "1" DAY)`)

	// Test null.
	nullTbl := []struct {
		Op       DateArithType
		Unit     string
		Date     interface{}
		Interval interface{}
	}{
		{DateAdd, "DAY", nil, "1"},
		{DateAdd, "DAY", input, nil},
	}
	for _, t := range nullTbl {
		e := &DateArith{
			Op:       t.Op,
			Unit:     t.Unit,
			Date:     Value{Val: t.Date},
			Interval: Value{Val: t.Interval},
		}
		v, err := e.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
		e.Op = DateSub
		v, err = e.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
	}

	// Test eval.
	tbl := []struct {
		Unit      string
		Interval  interface{}
		AddExpect string
		SubExpect string
	}{
		{"MICROSECOND", "1000", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"MICROSECOND", 1000, "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"SECOND", "10", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"MINUTE", "10", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"HOUR", "10", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"DAY", "11", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"WEEK", "2", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"MONTH", "2", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"QUARTER", "4", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"YEAR", "2", "2013-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"SECOND_MICROSECOND", "10.00100000", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"SECOND_MICROSECOND", "10.0010000000", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"SECOND_MICROSECOND", "10.0010000010", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"MINUTE_MICROSECOND", "10:10.100", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"MINUTE_SECOND", "10:10", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"HOUR_MICROSECOND", "10:10:10.100", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"HOUR_SECOND", "10:10:10", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"HOUR_MINUTE", "10:10", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"DAY_MICROSECOND", "11 10:10:10.100", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"DAY_SECOND", "11 10:10:10", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"DAY_MINUTE", "11 10:10", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"DAY_HOUR", "11 10", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"YEAR_MONTH", "11-1", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"YEAR_MONTH", "11-11", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
	}
	for _, t := range tbl {
		e := &DateArith{
			Op:       DateAdd,
			Unit:     t.Unit,
			Date:     Value{Val: input},
			Interval: Value{Val: t.Interval},
		}
		v, err := e.Eval(nil, nil)
		c.Assert(err, IsNil)
		value, ok := v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.AddExpect)

		e.Op = DateSub
		v, err = e.Eval(nil, nil)
		c.Assert(err, IsNil)
		value, ok = v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.SubExpect)
	}

	// Test eval for adddate and subdate with days form
	tblDays := []struct {
		Interval  interface{}
		AddExpect string
		SubExpect string
	}{
		{"20", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{19.88, "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"19.88", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"20-11", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"20,11", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"1000", "2014-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"true", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
	}
	for _, t := range tblDays {
		e := &DateArith{
			Op:       DateAdd,
			Unit:     "day",
			Date:     Value{Val: input},
			Interval: Value{Val: t.Interval},
		}
		v, err := e.Eval(nil, nil)
		c.Assert(err, IsNil)
		value, ok := v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.AddExpect)

		e.Op = DateSub
		v, err = e.Eval(nil, nil)
		c.Assert(err, IsNil)
		value, ok = v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.SubExpect)
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
		e := &DateArith{
			Op:       DateAdd,
			Unit:     t.Unit,
			Date:     Value{Val: input},
			Interval: Value{Val: t.Interval},
		}
		_, err := e.Eval(nil, nil)
		c.Assert(err, NotNil)
		e.Date = Value{Val: errInput}
		v, err := e.Eval(nil, nil)
		c.Assert(err, NotNil, Commentf("%s", v))

		e.Op = DateSub
		_, err = e.Eval(nil, nil)
		c.Assert(err, NotNil)
		e.Date = Value{Val: errInput}
		v, err = e.Eval(nil, nil)
		c.Assert(err, NotNil, Commentf("%s", v))
	}
}
