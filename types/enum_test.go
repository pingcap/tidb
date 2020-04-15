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

package types

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/util/testleak"
)

var _ = Suite(&testEnumSuite{})

type testEnumSuite struct {
}

func (s *testEnumSuite) TestEnum(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Elems    []string
		Name     string
		Expected int
	}{
		{[]string{"a", "b"}, "a", 1},
		{[]string{"a"}, "b", 0},
		{[]string{"a"}, "1", 1},
	}

	for _, t := range tbl {
		e, err := ParseEnumName(t.Elems, t.Name)
		if t.Expected == 0 {
			c.Assert(err, NotNil)
			c.Assert(e.ToNumber(), Equals, float64(0))
			c.Assert(e.String(), Equals, "")
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(e.String(), Equals, t.Elems[t.Expected-1])
		c.Assert(e.ToNumber(), Equals, float64(t.Expected))
	}

	tblNumber := []struct {
		Elems    []string
		Number   uint64
		Expected int
	}{
		{[]string{"a"}, 1, 1},
		{[]string{"a"}, 0, 0},
	}

	for _, t := range tblNumber {
		e, err := ParseEnumValue(t.Elems, t.Number)
		if t.Expected == 0 {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(e.ToNumber(), Equals, float64(t.Expected))
	}
}
