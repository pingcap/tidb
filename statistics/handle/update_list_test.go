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

package handle

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/statistics"
)

var _ = Suite(&testUpdateListSuite{})

type testUpdateListSuite struct {
}

func (s *testUpdateListSuite) TestInsertAndDelete(c *C) {
	h := Handle{
		listHead: &SessionStatsCollector{mapper: make(tableDeltaMap)},
		feedback: statistics.NewQueryFeedbackMap(),
	}
	var items []*SessionStatsCollector
	for i := 0; i < 5; i++ {
		items = append(items, h.NewSessionStatsCollector())
	}
	items[0].Delete() // delete tail
	items[2].Delete() // delete middle
	items[4].Delete() // delete head
	h.sweepList()

	c.Assert(h.listHead.next, Equals, items[3])
	c.Assert(items[3].next, Equals, items[1])
	c.Assert(items[1].next, IsNil)

	// delete rest
	items[1].Delete()
	items[3].Delete()
	h.sweepList()
	c.Assert(h.listHead.next, IsNil)
}
