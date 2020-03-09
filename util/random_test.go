// Copyright 2020-present PingCAP, Inc.
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

package util

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testRandSuite{})

type testRandSuite struct {
}

func (s *testMiscSuite) TestRand(c *C) {
	x := FastRand32N(1024)
	c.Assert(x < 1024, IsTrue)
	y := FastRand64N(1 << 63)
	c.Assert(y < 1<<63, IsTrue)

	_ = RandomBuf(20)
}
