// Copyright 2019 PingCAP, Inc.
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
	"strconv"

	. "github.com/pingcap/check"
)

var _ = Suite(&FspTest{})

type FspTest struct{}

func (s *FspTest) TestCheckFsp(c *C) {
	c.Parallel()
	obtained, err := CheckFsp(int(UnspecifiedFsp))
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, IsNil)

	obtained, err = CheckFsp(-2019)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, ErrorMatches, "Invalid fsp -2019")

	obtained, err = CheckFsp(int(MinFsp) - 4294967296)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, ErrorMatches, "Invalid fsp "+strconv.Itoa(int(MinFsp)-4294967296))

	// UnspecifiedFsp
	obtained, err = CheckFsp(-1)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, IsNil)

	obtained, err = CheckFsp(int(MaxFsp) + 1)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, ErrorMatches, "Invalid fsp "+strconv.Itoa(int(MaxFsp)+1))

	obtained, err = CheckFsp(int(MaxFsp) + 2019)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, ErrorMatches, "Invalid fsp "+strconv.Itoa(int(MaxFsp)+2019))

	obtained, err = CheckFsp(int(MaxFsp) + 4294967296)
	c.Assert(obtained, Equals, DefaultFsp)
	c.Assert(err, ErrorMatches, "Invalid fsp "+strconv.Itoa(int(MaxFsp)+4294967296))

	obtained, err = CheckFsp(int(MaxFsp+MinFsp) / 2)
	c.Assert(obtained, Equals, (MaxFsp+MinFsp)/2)
	c.Assert(err, IsNil)

	obtained, err = CheckFsp(5)
	c.Assert(obtained, Equals, int8(5))
	c.Assert(err, IsNil)
}

func (s *FspTest) TestParseFrac(c *C) {
	c.Parallel()
	obtained, overflow, err := ParseFrac("", 5)
	c.Assert(obtained, Equals, 0)
	c.Assert(overflow, Equals, false)
	c.Assert(err, IsNil)

	a := 200
	obtained, overflow, err = ParseFrac("999", int8(a))
	c.Assert(obtained, Equals, 0)
	c.Assert(overflow, Equals, false)
	c.Assert(err, ErrorMatches, "Invalid fsp .*")

	obtained, overflow, err = ParseFrac("NotNum", MaxFsp)
	c.Assert(obtained, Equals, 0)
	c.Assert(overflow, Equals, false)
	c.Assert(err, ErrorMatches, "strconv.ParseInt:.*")

	obtained, overflow, err = ParseFrac("1235", 6)
	c.Assert(obtained, Equals, 123500)
	c.Assert(overflow, Equals, false)
	c.Assert(err, IsNil)

	obtained, overflow, err = ParseFrac("123456", 4)
	c.Assert(obtained, Equals, 123500)
	c.Assert(overflow, Equals, false)
	c.Assert(err, IsNil)

	// 1236 round 3 -> 124 -> 124000
	obtained, overflow, err = ParseFrac("1236", 3)
	c.Assert(obtained, Equals, 124000)
	c.Assert(overflow, Equals, false)
	c.Assert(err, IsNil)

	// 03123 round 2 -> 3 -> 30000
	obtained, overflow, err = ParseFrac("0312", 2)
	c.Assert(obtained, Equals, 30000)
	c.Assert(overflow, Equals, false)
	c.Assert(err, IsNil)

	// 999 round 2 -> 100 -> overflow
	obtained, overflow, err = ParseFrac("999", 2)
	c.Assert(obtained, Equals, 0)
	c.Assert(overflow, Equals, true)
	c.Assert(err, IsNil)
}

func (s *FspTest) TestAlignFrac(c *C) {
	c.Parallel()
	obtained := alignFrac("100", 6)
	c.Assert(obtained, Equals, "100000")
	obtained = alignFrac("10000000000", 6)
	c.Assert(obtained, Equals, "10000000000")
	obtained = alignFrac("-100", 6)
	c.Assert(obtained, Equals, "-100000")
	obtained = alignFrac("-10000000000", 6)
	c.Assert(obtained, Equals, "-10000000000")
}
