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

type slowQueryInfoHeap []*slowQueryInfo

func (h slowQueryInfoHeap) Len() int           { return len(h) }
func (h slowQueryInfoHeap) Less(i, j int) bool { return h[i].duration < h[j].duration }
func (h slowQueryInfoHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *slowQueryInfoHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*slowQueryInfo))
}

func (h *slowQueryInfoHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// topNSlowQuery maintains a heap to store recent slow queries.
// N = 30, recent = 7 days by default.
type topNSlowQuery struct {
	data   slowQueryInfoHeap
	topN   int
	recent time.Duration
	ch     chan *slowQueryInfo
}

func newTopNSlowQuery(topN int, recent time.Duration) *topNSlowQuery {
	return &topNSlowQuery{
		data:   make([]*slowQueryInfo, 0, topN),
		topN:   topN,
		recent: recent,
		ch:     make(chan *slowQueryInfo, 1000),
	}
}

func (q *topNSlowQuery) Close() {
	close(q.ch)
}

func (q *topNSlowQuery) Push(info *slowQueryInfo) {
	// Heap is not full, append to it and sift up.
	if len(q.data) < q.topN {
		heap.Push(&q.data, info)
		return
	}

	// Replace the heap top and sift down.
	if info.duration > q.data[0].duration {
		heap.Pop(&q.data)
		heap.Push(&q.data, info)
	}
}

func (q *topNSlowQuery) Refresh(now time.Time) {
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
	heap.Init(&q.data)
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
