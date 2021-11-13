// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package build

import (
	"strings"
	"testing"

	. "github.com/pingcap/check"
)

type infoSuite struct{}

var _ = Suite(&infoSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (*infoSuite) TestInfo(c *C) {
	info := Info()
	lines := strings.Split(info, "\n")
	c.Assert(lines[0], Matches, "Release Version.*")
	c.Assert(lines[1], Matches, "Git Commit Hash.*")
	c.Assert(lines[2], Matches, "Git Branch.*")
	c.Assert(lines[3], Matches, "Go Version.*")
	c.Assert(lines[4], Matches, "UTC Build Time.*")
}

func (*infoSuite) TestLogInfo(c *C) {
	LogInfo(BR)
	LogInfo(Lightning)
}
