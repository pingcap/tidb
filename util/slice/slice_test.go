// Copyright 2021 PingCAP, Inc.
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

package slice

import (
	"testing"

	. "github.com/pingcap/check"
)

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSliceSuite{})

type testSliceSuite struct {
}

func (s *testSliceSuite) Test(c *C) {
	tests := []struct {
		a      []int
		anyOf  bool
		noneOf bool
		allOf  bool
	}{
		{[]int{}, false, true, true},
		{[]int{1, 2, 3}, true, false, false},
		{[]int{1, 3}, false, true, false},
		{[]int{2, 2, 4}, true, false, true},
	}

	for _, t := range tests {
		even := func(i int) bool { return t.a[i]%2 == 0 }
		c.Assert(AnyOf(t.a, even), Equals, t.anyOf)
		c.Assert(NoneOf(t.a, even), Equals, t.noneOf)
		c.Assert(AllOf(t.a, even), Equals, t.allOf)
	}
}
