// Copyright 2018 PingCAP, Inc.
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

package disjointset

import (
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testDisjointSetSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testDisjointSetSuite struct {
}

func (s *testDisjointSetSuite) TestIntDisjointSet(c *C) {
	set := NewIntSet(10)
	c.Assert(len(set.parent), Equals, 10)
	for i := range set.parent {
		c.Assert(set.parent[i], Equals, i)
	}
	set.Union(0, 1)
	set.Union(1, 3)
	set.Union(4, 2)
	set.Union(2, 6)
	set.Union(3, 5)
	set.Union(7, 8)
	set.Union(9, 6)
	c.Assert(set.FindRoot(0), Equals, set.FindRoot(1))
	c.Assert(set.FindRoot(3), Equals, set.FindRoot(1))
	c.Assert(set.FindRoot(5), Equals, set.FindRoot(1))
	c.Assert(set.FindRoot(2), Equals, set.FindRoot(4))
	c.Assert(set.FindRoot(6), Equals, set.FindRoot(4))
	c.Assert(set.FindRoot(9), Equals, set.FindRoot(2))
	c.Assert(set.FindRoot(7), Equals, set.FindRoot(8))
}
