// Copyright 2016 PingCAP, Inc.
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

package heap

import (
	"testing"

	"github.com/pingcap/check"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testHeapSuite{})

type testHeapSuite struct {
}

type IntList []int

func (l IntList) Len() int {
	return len(l)
}

func (l IntList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l IntList) Less(i, j int) bool {
	return l[i] < l[j]
}

func (s *testHeapSuite) TestHeap(c *check.C) {
	heaper := IntList{0, 1}
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{1})
	heaper = append(heaper, 2)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{2, 1})
	heaper = append(heaper, 3)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{3, 1, 2})
	heaper = append(heaper, 4)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{4, 3, 2, 1})
	heaper = append(heaper, 5)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{5, 4, 2, 1, 3})
	heaper = append(heaper, 6)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{6, 4, 5, 1, 3, 2})
	heaper = append(heaper, 7)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{7, 4, 6, 1, 3, 2, 5})
	heaper = append(heaper, 8)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{8, 7, 6, 4, 3, 2, 5, 1})
	heaper = append(heaper, 9)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{9, 8, 6, 7, 3, 2, 5, 1, 4})
	heaper = append(heaper, 10)
	Update(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{10, 9, 6, 7, 8, 2, 5, 1, 4, 3})
	heaper[1] = 2
	Heapify(heaper)
	c.Assert(heaper[1:], check.DeepEquals, IntList{9, 8, 6, 4, 7, 2, 5, 1, 2, 3})
}
