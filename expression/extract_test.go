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
	"strings"

	. "github.com/pingcap/check"
)

var _ = Suite(&testExtractSuite{})

type testExtractSuite struct {
}

func (t *testExtractSuite) TestExtract(c *C) {
	str := "2011-11-11 10:10:10.123456"
	tbl := []struct {
		Unit   string
		Expect int64
	}{
		{"MICROSECOND", 123456},
		{"SECOND", 10},
		{"MINUTE", 10},
		{"HOUR", 10},
		{"DAY", 11},
		{"WEEK", 45},
		{"MONTH", 11},
		{"QUARTER", 4},
		{"YEAR", 2011},
		{"SECOND_MICROSECOND", 10123456},
		{"MINUTE_MICROSECOND", 1010123456},
		{"MINUTE_SECOND", 1010},
		{"HOUR_MICROSECOND", 101010123456},
		{"HOUR_SECOND", 101010},
		{"HOUR_MINUTE", 1010},
		{"DAY_MICROSECOND", 11101010123456},
		{"DAY_SECOND", 11101010},
		{"DAY_MINUTE", 111010},
		{"DAY_HOUR", 1110},
		{"YEAR_MONTH", 201111},
	}

	for _, t := range tbl {
		e := &Extract{
			Unit: t.Unit,
			Date: Value{Val: str},
		}

		v, err := e.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expect)
	}

	// Test nil
	e := &Extract{
		Unit: "SECOND",
		Date: Value{Val: nil},
	}

	v, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	c.Assert(strings.ToUpper(e.String()), Equals, "EXTRACT(SECOND FROM NULL)")
	c.Assert(e.Clone(), NotNil)
}
