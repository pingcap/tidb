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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPush(t *testing.T) {
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
	require.Equal(t, 300*time.Millisecond, slowQuery.user.data[0].Duration)
	checkHeap(&slowQuery.user, t)

	// Update all data in the heap.
	slowQuery.Append(&SlowQueryInfo{Duration: 1300 * time.Millisecond})
	require.Equal(t, 400*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1400 * time.Millisecond})
	require.Equal(t, 500*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1500 * time.Millisecond})
	require.Equal(t, 600*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1500 * time.Millisecond})
	require.Equal(t, 700*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1600 * time.Millisecond})
	require.Equal(t, 800*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1700 * time.Millisecond})
	require.Equal(t, 900*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1800 * time.Millisecond})
	require.Equal(t, 1000*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 1900 * time.Millisecond})
	require.Equal(t, 1100*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 2000 * time.Millisecond})
	require.Equal(t, 1200*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 2100 * time.Millisecond})
	require.Equal(t, 1300*time.Millisecond, slowQuery.user.data[0].Duration)
	checkHeap(&slowQuery.user, t)

	// Data smaller than heap top will not be inserted.
	slowQuery.Append(&SlowQueryInfo{Duration: 1200 * time.Millisecond})
	require.Equal(t, 1300*time.Millisecond, slowQuery.user.data[0].Duration)
	slowQuery.Append(&SlowQueryInfo{Duration: 666 * time.Millisecond})
	require.Equal(t, 1300*time.Millisecond, slowQuery.user.data[0].Duration)
}

func TestRemoveExpired(t *testing.T) {
	now := time.Now()
	slowQuery := newTopNSlowQueries(6, 3*time.Second, 10)

	slowQuery.Append(&SlowQueryInfo{Start: now, Duration: 6})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(1 * time.Second), Duration: 5})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(2 * time.Second), Duration: 4})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(3 * time.Second), Duration: 3})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(4 * time.Second), Duration: 2})
	require.Equal(t, 2*time.Nanosecond, slowQuery.user.data[0].Duration)

	slowQuery.RemoveExpired(now.Add(5 * time.Second))
	require.Len(t, slowQuery.user.data, 2)
	require.Equal(t, 2*time.Nanosecond, slowQuery.user.data[0].Duration)

	slowQuery.Append(&SlowQueryInfo{Start: now.Add(3 * time.Second), Duration: 3})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(4 * time.Second), Duration: 2})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(5 * time.Second), Duration: 1})
	slowQuery.Append(&SlowQueryInfo{Start: now.Add(6 * time.Second), Duration: 0})
	require.Len(t, slowQuery.user.data, 6)
	require.Equal(t, 0*time.Nanosecond, slowQuery.user.data[0].Duration)

	slowQuery.RemoveExpired(now.Add(6 * time.Second))
	require.Len(t, slowQuery.user.data, 4)
	require.Equal(t, 0*time.Nanosecond, slowQuery.user.data[0].Duration)
}

func TestQueue(t *testing.T) {
	q := newTopNSlowQueries(10, time.Minute, 5)
	q.Append(&SlowQueryInfo{SQL: "aaa"})
	q.Append(&SlowQueryInfo{SQL: "bbb"})
	q.Append(&SlowQueryInfo{SQL: "ccc"})

	query := q.recent.Query(1)
	require.Equal(t, "ccc", query[0].SQL)

	query = q.recent.Query(2)
	require.Equal(t, "ccc", query[0].SQL)
	require.Equal(t, "bbb", query[1].SQL)

	query = q.recent.Query(6)
	require.Equal(t, "ccc", query[0].SQL)
	require.Equal(t, "bbb", query[1].SQL)
	require.Equal(t, "aaa", query[2].SQL)

	q.Append(&SlowQueryInfo{SQL: "ddd"})
	q.Append(&SlowQueryInfo{SQL: "eee"})
	q.Append(&SlowQueryInfo{SQL: "fff"})
	q.Append(&SlowQueryInfo{SQL: "ggg"})

	query = q.recent.Query(3)
	require.Equal(t, "ggg", query[0].SQL)
	require.Equal(t, "fff", query[1].SQL)
	require.Equal(t, "eee", query[2].SQL)

	query = q.recent.Query(6)
	require.Equal(t, "ggg", query[0].SQL)
	require.Equal(t, "fff", query[1].SQL)
	require.Equal(t, "eee", query[2].SQL)
	require.Equal(t, "ddd", query[3].SQL)
	require.Equal(t, "ccc", query[4].SQL)
}

func checkHeap(q *slowQueryHeap, t *testing.T) {
	for i := 0; i < len(q.data); i++ {
		left := 2*i + 1
		right := 2*i + 2
		if left < len(q.data) {
			require.LessOrEqual(t, q.data[i].Duration, q.data[left].Duration)
		}
		if right < len(q.data) {
			require.LessOrEqual(t, q.data[i].Duration, q.data[right].Duration)
		}
	}
}
