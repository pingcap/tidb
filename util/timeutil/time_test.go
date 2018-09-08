package timeutil

import (
	. "github.com/pingcap/check"
)

type testSuite struct{}

func (s *testSuite) TestgetTZNameFromFileName(c *C) {
	tz, err := getTZNameFromFileName("/user/share/zoneinfo/Asia/Shanghai")
	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")
}
