package timeutil

import (
	"os"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTimeSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testTimeSuite struct{}

func (s *testTimeSuite) TestgetTZNameFromFileName(c *C) {
	tz, err := getTZNameFromFileName("/user/share/zoneinfo/Asia/Shanghai")
	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "Asia/Shanghai")
}

func (s *testTimeSuite) TestLocal(c *C) {
	os.Setenv("TZ", "UTC")
	loc := Local()
	c.Assert(loc.String(), Equals, "UTC")
	os.Unsetenv("TZ")
}
