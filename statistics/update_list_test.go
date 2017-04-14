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

package statistics

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testUpdateListSuite{})

type testUpdateListSuite struct {
}

func (s *testUpdateListSuite) TestInsertAndDelete(c *C) {
	h := NewHandle(nil)
	var items []*SessionStatsCollector
	for i := 0; i < 5; i++ {
		items = append(items, h.NewStatsUpdateHandle())
	}
	h.DelStatsUpdateHandle(items[0]) // delete tail
	h.DelStatsUpdateHandle(items[2]) // delete middle
	h.DelStatsUpdateHandle(items[4]) // delete head

	c.Assert(h.updateManager.listHead.next, Equals, items[3])
	c.Assert(items[3].next, Equals, items[1])
	c.Assert(h.updateManager.listHead.next.next.next, IsNil)
	c.Assert(items[1].prev, Equals, items[3])
	c.Assert(items[3].prev, Equals, h.updateManager.listHead)

	// delete rest
	h.DelStatsUpdateHandle(items[1])
	h.DelStatsUpdateHandle(items[3])
	c.Assert(h.updateManager.listHead.next, IsNil)
}
