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

package domain

import (
	"time"

	. "github.com/pingcap/check"
)

var _ = Suite(&testTopNSlowQuerySuite{})

type testTopNSlowQuerySuite struct{}

func (t *testTopNSlowQuerySuite) TestPush(c *C) {
	slowQuery := newTopNSlowQuery(10, 0)
	// Insert data into the heap.
	slowQuery.Push(&slowQueryInfo{duration: 300 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 400 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 500 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 600 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 700 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 800 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 900 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 1000 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 1100 * time.Millisecond})
	slowQuery.Push(&slowQueryInfo{duration: 1200 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 300*time.Millisecond)
	checkHeap(slowQuery, c)

	// Update all data in the heap.
	slowQuery.Push(&slowQueryInfo{duration: 1300 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 400*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1400 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 500*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1500 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 600*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1500 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 700*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1600 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 800*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1700 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 900*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1800 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1000*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 1900 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1100*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 2000 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1200*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 2100 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1300*time.Millisecond)
	checkHeap(slowQuery, c)

	// Data smaller than heap top will not be inserted.
	slowQuery.Push(&slowQueryInfo{duration: 1200 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1300*time.Millisecond)
	slowQuery.Push(&slowQueryInfo{duration: 666 * time.Millisecond})
	c.Assert(slowQuery.data[0].duration, Equals, 1300*time.Millisecond)
}

func (t *testTopNSlowQuerySuite) TestRefresh(c *C) {
	now := time.Now()
	slowQuery := newTopNSlowQuery(6, 3*time.Second)

	slowQuery.Push(&slowQueryInfo{start: now, duration: 6})
	slowQuery.Push(&slowQueryInfo{start: now.Add(1 * time.Second), duration: 5})
	slowQuery.Push(&slowQueryInfo{start: now.Add(2 * time.Second), duration: 4})
	slowQuery.Push(&slowQueryInfo{start: now.Add(3 * time.Second), duration: 3})
	slowQuery.Push(&slowQueryInfo{start: now.Add(4 * time.Second), duration: 2})
	c.Assert(slowQuery.data[0].duration, Equals, 2*time.Nanosecond)

	slowQuery.Refresh(now.Add(5 * time.Second))
	c.Assert(slowQuery.offset, Equals, 2)
	c.Assert(slowQuery.data[0].duration, Equals, 2*time.Nanosecond)

	slowQuery.Push(&slowQueryInfo{start: now.Add(3 * time.Second), duration: 3})
	slowQuery.Push(&slowQueryInfo{start: now.Add(4 * time.Second), duration: 2})
	slowQuery.Push(&slowQueryInfo{start: now.Add(5 * time.Second), duration: 1})
	slowQuery.Push(&slowQueryInfo{start: now.Add(6 * time.Second), duration: 0})
	c.Assert(slowQuery.offset, Equals, 6)
	c.Assert(slowQuery.data[0].duration, Equals, 0*time.Nanosecond)

	slowQuery.Refresh(now.Add(6 * time.Second))
	c.Assert(slowQuery.offset, Equals, 4)
	c.Assert(slowQuery.data[0].duration, Equals, 0*time.Nanosecond)
}

func checkHeap(q *topNSlowQuery, c *C) {
	for i := 0; i < q.offset; i++ {
		left := 2*i + 1
		right := 2*i + 2
		if left < q.offset {
			c.Assert(q.data[i].duration, LessEqual, q.data[left].duration)
		}
		if right < q.offset {
			c.Assert(q.data[i].duration, LessEqual, q.data[right].duration)
		}
	}
}
