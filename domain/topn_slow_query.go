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
	"container/heap"
	"time"

	"github.com/pingcap/tidb/util/execdetails"
)

// topNSlowQueries maintains a heap to store recent slow queries.
// N = 30, recent = 7 days by default.
type topNSlowQueries struct {
	data   []*slowQueryInfo
	topN   int
	recent time.Duration
	ch     chan *slowQueryInfo
}

func newTopNSlowQueries(topN int, recent time.Duration) *topNSlowQueries {
	return &topNSlowQueries{
		data:   make([]*slowQueryInfo, 0, topN),
		topN:   topN,
		recent: recent,
		ch:     make(chan *slowQueryInfo, 1000),
	}
}

func (q *topNSlowQueries) Close() {
	close(q.ch)
}

func (q *topNSlowQueries) Len() int           { return len(q.data) }
func (q *topNSlowQueries) Less(i, j int) bool { return q.data[i].duration < q.data[j].duration }
func (q *topNSlowQueries) Swap(i, j int)      { q.data[i], q.data[j] = q.data[j], q.data[i] }

func (q *topNSlowQueries) Push(x interface{}) {
	q.data = append(q.data, x.(*slowQueryInfo))
}

func (q *topNSlowQueries) Pop() interface{} {
	old := q.data
	n := len(old)
	x := old[n-1]
	q.data = old[0 : n-1]
	return x
}

func (q *topNSlowQueries) Append(info *slowQueryInfo) {
	// Heap is not full, append to it and sift up.
	if len(q.data) < q.topN {
		heap.Push(q, info)
		return
	}

	// Replace the heap top and sift down.
	if info.duration > q.data[0].duration {
		heap.Pop(q)
		heap.Push(q, info)
	}
}

func (q *topNSlowQueries) Refresh(now time.Time) {
	// Remove outdated slow query element.
	idx := 0
	for i := 0; i < len(q.data); i++ {
		outdateTime := q.data[i].start.Add(q.recent)
		if outdateTime.After(now) {
			q.data[idx] = q.data[i]
			idx++
		}
	}
	if len(q.data) == idx {
		return
	}

	// Rebuild the heap.
	q.data = q.data[:idx]
	heap.Init(q)
}

type slowQueryInfo struct {
	sql      string
	start    time.Time
	duration time.Duration
	detail   execdetails.ExecDetails
	succ     bool
	connID   uint64
	txnTS    uint64
	user     string
	db       string
	tableIDs string
	indexIDs string
}
