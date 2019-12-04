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
	slowQuery := newTopNSlowQueries(10, 0, 10)
	// Insert data into the heap.
	slowQuery.Append(&SlowQueryInfo{Duration: 300 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 400 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 500 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 600 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 700 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 800 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 900 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 1000 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 1100 * time.Millisecond})
	slowQuery.Append(&SlowQueryInfo{Duration: 1200 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 300*time.Millisecond)
	checkHeap(&slowQuery.user, c)

	// Update all data in the heap.
	slowQuery.Append(&SlowQueryInfo{Duration: 1300 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 400*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1400 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 500*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1500 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 600*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1500 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 700*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1600 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 800*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1700 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 900*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1800 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1000*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 1900 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1100*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 2000 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1200*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 2100 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1300*time.Millisecond)
	checkHeap(&slowQuery.user, c)

	// Data smaller than heap top will not be inserted.
	slowQuery.Append(&SlowQueryInfo{Duration: 1200 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1300*time.Millisecond)
	slowQuery.Append(&SlowQueryInfo{Duration: 666 * time.Millisecond})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 1300*time.Millisecond)
}

func (t *testTopNSlowQuerySuite) TestRemoveExpired(c *C) {
	now := time.Now()
	slowQuery := newTopNSlowQueries(6, 3*time.Second, 10)

	slowQuery.Append(&SlowQueryInfo{Start: now, Duration: 6})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(1 * time.Second), Duration: 5})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(2 * time.Second), Duration: 4})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(3 * time.Second), Duration: 3})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(4 * time.Second), Duration: 2})
	c.Assert(slowQuery.user.data[0].Duration, Equals, 2*time.Nanosecond)

	slowQuery.RemoveExpired(now.Add(5 * time.Second))
	c.Assert(len(slowQuery.user.data), Equals, 2)
	c.Assert(slowQuery.user.data[0].Duration, Equals, 2*time.Nanosecond)

	slowQuery.Append(&SlowQueryInfo{Start: now.Add(3 * time.Second), Duration: 3})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(4 * time.Second), Duration: 2})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(5 * time.Second), Duration: 1})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(6 * time.Second), Duration: 0})
	c.Assert(len(slowQuery.user.data), Equals, 6)
	c.Assert(slowQuery.user.data[0].Duration, Equals, 0*time.Nanosecond)

	slowQuery.RemoveExpired(now.Add(6 * time.Second))
	c.Assert(len(slowQuery.user.data), Equals, 4)
	c.Assert(slowQuery.user.data[0].Duration, Equals, 0*time.Nanosecond)
}

func checkHeap(q *slowQueryHeap, c *C) {
	for i := 0; i < len(q.data); i++ {
		left := 2*i + 1
		right := 2*i + 2
		if left < len(q.data) {
			c.Assert(q.data[i].Duration, LessEqual, q.data[left].Duration)
		}
		if right < len(q.data) {
			c.Assert(q.data[i].Duration, LessEqual, q.data[right].Duration)
		}
	}
}

func (t *testTopNSlowQuerySuite) TestQueue(c *C) {
	q := newTopNSlowQueries(10, time.Minute, 5)
	q.Append(&SlowQueryInfo{SQL: "aaa"})
	q.Append(&SlowQueryInfo{SQL: "bbb"})
	q.Append(&SlowQueryInfo{SQL: "ccc"})

	query := q.recent.Query(1)
	c.Assert(query[0].SQL, Equals, "ccc")
	query = q.recent.Query(2)
	c.Assert(query[0].SQL, Equals, "ccc")
	c.Assert(query[1].SQL, Equals, "bbb")
	query = q.recent.Query(6)
	c.Assert(query[0].SQL, Equals, "ccc")
	c.Assert(query[1].SQL, Equals, "bbb")
	c.Assert(query[2].SQL, Equals, "aaa")

	q.Append(&SlowQueryInfo{SQL: "ddd"})
	q.Append(&SlowQueryInfo{SQL: "eee"})
	q.Append(&SlowQueryInfo{SQL: "fff"})
	q.Append(&SlowQueryInfo{SQL: "ggg"})

	query = q.recent.Query(3)
	c.Assert(query[0].SQL, Equals, "ggg")
	c.Assert(query[1].SQL, Equals, "fff")
	c.Assert(query[2].SQL, Equals, "eee")
	query = q.recent.Query(6)
	c.Assert(query[0].SQL, Equals, "ggg")
	c.Assert(query[1].SQL, Equals, "fff")
	c.Assert(query[2].SQL, Equals, "eee")
	c.Assert(query[3].SQL, Equals, "ddd")
	c.Assert(query[4].SQL, Equals, "ccc")
}
