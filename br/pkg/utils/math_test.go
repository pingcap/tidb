// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	. "github.com/pingcap/check"
)

type testMathSuite struct{}

var _ = Suite(&testMathSuite{})

func (*testMathSuite) TestMinInt(c *C) {
	c.Assert(MinInt(1, 2), Equals, 1)
	c.Assert(MinInt(2, 1), Equals, 1)
	c.Assert(MinInt(4, 2, 1, 3), Equals, 1)
	c.Assert(MinInt(1, 1), Equals, 1)
}

func (*testMathSuite) TestMaxInt(c *C) {
	c.Assert(MaxInt(1, 2), Equals, 2)
	c.Assert(MaxInt(2, 1), Equals, 2)
	c.Assert(MaxInt(4, 2, 1, 3), Equals, 4)
	c.Assert(MaxInt(1, 1), Equals, 1)
}

func (*testMathSuite) TestClampInt(c *C) {
	c.Assert(ClampInt(100, 1, 3), Equals, 3)
	c.Assert(ClampInt(2, 1, 3), Equals, 2)
	c.Assert(ClampInt(0, 1, 3), Equals, 1)
	c.Assert(ClampInt(0, 1, 1), Equals, 1)
	c.Assert(ClampInt(100, 1, 1), Equals, 1)
}

func (*testMathSuite) TestMinInt64(c *C) {
	c.Assert(MinInt(1, 2), Equals, 1)
	c.Assert(MinInt(2, 1), Equals, 1)
	c.Assert(MinInt(4, 2, 1, 3), Equals, 1)
	c.Assert(MinInt(1, 1), Equals, 1)
}

func (*testMathSuite) TestNextPowerOfTwo(c *C) {
	c.Assert(NextPowerOfTwo(1), Equals, int64(1))
	c.Assert(NextPowerOfTwo(3), Equals, int64(4))
	c.Assert(NextPowerOfTwo(255), Equals, int64(256))
	c.Assert(NextPowerOfTwo(1024), Equals, int64(1024))
	c.Assert(NextPowerOfTwo(0xabcd1234), Equals, int64(0x100000000))
}
