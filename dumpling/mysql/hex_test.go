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

package mysql

import (
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testHexSuite{})

type testHexSuite struct {
}

func (s *testHexSuite) TestHex(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  string
		Expect int64
		Err    bool
	}{
		{"x'1'", 0, true},
		{"x'01'", 1, false},
		{"X'01'", 1, false},
		{"0x1", 1, false},
		{"0x-1", 0, true},
		{"0X11", 0, true},
		{"x'01+'", 1, true},
		{"0x123", 0x123, false},
		{"0x10", 0x10, false},
		{"", 0, true},
	}

	for _, t := range tbl {
		h, err := ParseHex(t.Input)
		if t.Err {
			c.Assert(err, NotNil)
			continue
		}

		c.Assert(err, IsNil)
		c.Assert(h.ToNumber(), Equals, float64(t.Expect))
		s := h.String()
		n, err := strconv.ParseInt(s, 0, 64)
		c.Assert(err, IsNil)
		c.Assert(n, Equals, t.Expect)
	}

	h, err := ParseHex("0x4D7953514C")
	c.Assert(err, IsNil)
	c.Assert(h.ToString(), Equals, "MySQL")

	h.Value = 1
	c.Assert(h.ToString(), Equals, "\x01")

	/*
	 mysql> select hex("I am a long hex string");
	 +----------------------------------------------+
	 | hex("I am a long hex string")                |
	 +----------------------------------------------+
	 | 4920616D2061206C6F6E672068657820737472696E67 |
	 +----------------------------------------------+
	 1 row in set (0.00 sec)
	*/
	str := "I am a long hex string"
	hexStr := "0x4920616D2061206C6F6E672068657820737472696E67"
	_, err = ParseHex(hexStr)
	c.Assert(err, NotNil)
	v, err := ParseHexStr(hexStr)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, str)
}
