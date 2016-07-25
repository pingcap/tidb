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

package mysql

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testMyDecimalSuite{})

type testMyDecimalSuite struct {
}

func (s *testMyDecimalSuite) TestToInt(c *C) {
	cases := []struct {
		input   string
		output  int64
		errcode int
	}{
		{"18446744073709551615", 9223372036854775807, 2},
		{"-1", -1, 0},
		{"-1.23", -1, 1},
		{"-9223372036854775807", -9223372036854775807, 0},
		{"-9223372036854775808", -9223372036854775808, 0},
		{"9223372036854775808", 9223372036854775807, 2},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.input))
		result, ec := dec.ToInt()
		c.Check(ec, Equals, ca.errcode)
		c.Check(result, Equals, ca.output)
	}
}

func (s *testMyDecimalSuite) TestToUint(c *C) {
	cases := []struct {
		input   string
		output  uint64
		errcode int
	}{
		{"12345", 12345, 0},
		{"0", 0, 0},
		/* ULLONG_MAX = 18446744073709551615ULL */
		{"18446744073709551615", 18446744073709551615, 0},
		{"18446744073709551616", 18446744073709551615, 2},
		{"-1", 0, 2},
		{"1.23", 1, 1},
		{"9999999999999999999999999.000", 18446744073709551615, 2},
	}
	for _, ca := range cases {
		var dec MyDecimal
		dec.FromString([]byte(ca.input))
		result, ec := dec.ToUint()
		c.Check(ec, Equals, ca.errcode)
		c.Check(result, Equals, ca.output)
	}
}
