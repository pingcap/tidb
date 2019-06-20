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

package math

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMath{})

type testMath struct{}

func (s *testMath) TestAbs(c *C) {
	c.Assert(Abs(1), Equals, int64(1))
	c.Assert(Abs(0), Equals, int64(0))
	c.Assert(Abs(1000), Equals, int64(1000))
	c.Assert(Abs(-100), Equals, int64(100))
	c.Assert(Abs(-1234), Equals, int64(1234))
}
