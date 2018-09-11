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

type slowQueryHeap struct {
	data []*slowQueryInfo
}

func (h *slowQueryHeap) Len() int           { return len(h.data) }
func (h *slowQueryHeap) Less(i, j int) bool { return h.data[i].duration < h.data[j].duration }
func (h *slowQueryHeap) Swap(i, j int)      { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *slowQueryHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*slowQueryInfo))
}

func (h *slowQueryHeap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[0 : n-1]
	return x
}

func (h *slowQueryHeap) Refresh(now time.Time, recent time.Duration) {
	// Remove outdated slow query element.
	idx := 0
	for i := 0; i < len(h.data); i++ {
		outdateTime := h.data[i].start.Add(recent)
		if outdateTime.After(now) {
			h.data[idx] = h.data[i]
			idx++
		}
	}
	if len(h.data) == idx {
		return
	}

	// Rebuild the heap.
	h.data = h.data[:idx]
	heap.Init(h)
}

// topNSlowQueries maintains two heaps to store recent slow queries: one for user's and one for internal.
// N = 30, recent = 7 days by default.
type topNSlowQueries struct {
	user     slowQueryHeap
	internal slowQueryHeap
	topN     int
	recent   time.Duration
	ch       chan *slowQueryInfo
}

func newTopNSlowQueries(topN int, recent time.Duration) *topNSlowQueries {
	ret := &topNSlowQueries{
		topN:   topN,
		recent: recent,
		ch:     make(chan *slowQueryInfo, 1000),
	}
	ret.user.data = make([]*slowQueryInfo, 0, topN)
	ret.internal.data = make([]*slowQueryInfo, 0, topN)
	return ret
}

func (q *topNSlowQueries) Append(info *slowQueryInfo) {
	var h *slowQueryHeap
	if info.internal {
		h = &q.internal
	} else {
		h = &q.user
	}

	// Heap is not full.
	if len(h.data) < q.topN {
		heap.Push(h, info)
		return
	}

	// Replace the heap top.
	if info.duration > h.data[0].duration {
		heap.Pop(h)
		heap.Push(h, info)
	}
}

func (q *topNSlowQueries) Refresh(now time.Time) {
	q.user.Refresh(now, q.recent)
	q.internal.Refresh(now, q.recent)
}

func (q *topNSlowQueries) Close() {
	close(q.ch)
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
	internal bool
}
