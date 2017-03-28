// Copyright 2017 PingCAP, Inc.
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

package bytespool

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testBytesPoolSuite{})

type testBytesPoolSuite struct{}

func (s *testBytesPoolSuite) TestBytesPool(c *C) {
	poolTests := []struct {
		size      int
		allocSize int
		freeIdx   int
	}{
		{100, kilo, 0},
		{kilo, kilo, 0},
		{2 * kilo, 2 * kilo, 1},
		{8*kilo + 1, 16 * kilo, 4},
		{128 * mega, 128 * mega, 17},
	}
	bp := NewBytesPool()
	for _, tt := range poolTests {
		origin, data := bp.Alloc(tt.size)
		c.Assert(len(data), Equals, tt.size)
		c.Assert(len(origin), Equals, tt.allocSize)
		idx := bp.Free(origin)
		c.Assert(idx, Equals, tt.freeIdx)
	}
	c.Assert(bp.Free(make([]byte, 100)), Equals, -1)
	c.Assert(bp.Free(make([]byte, kilo+1)), Equals, -1)
}
