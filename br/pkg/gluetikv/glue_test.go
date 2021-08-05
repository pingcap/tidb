package gluetikv

import (
	"testing"

	. "github.com/pingcap/check"
)

type testGlue struct{}

var _ = Suite(&testGlue{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testGlue) TestGetVersion(c *C) {
	g := Glue{}
	c.Assert(g.GetVersion(), Matches, "BR(.|\n)*Release Version(.|\n)*Git Commit Hash(.|\n)*")
}
