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
	data []*SlowQueryInfo
}

func (h *slowQueryHeap) Len() int           { return len(h.data) }
func (h *slowQueryHeap) Less(i, j int) bool { return h.data[i].Duration < h.data[j].Duration }
func (h *slowQueryHeap) Swap(i, j int)      { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *slowQueryHeap) Push(x interface{}) {
	h.data = append(h.data, x.(*SlowQueryInfo))
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
		outdateTime := h.data[i].Start.Add(recent)
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
	ch       chan *SlowQueryInfo
}

func newTopNSlowQueries(topN int, recent time.Duration) *topNSlowQueries {
	ret := &topNSlowQueries{
		topN:   topN,
		recent: recent,
		ch:     make(chan *SlowQueryInfo, 1000),
	}
	ret.user.data = make([]*SlowQueryInfo, 0, topN)
	ret.internal.data = make([]*SlowQueryInfo, 0, topN)
	return ret
}

func (q *topNSlowQueries) Append(info *SlowQueryInfo) {
	var h *slowQueryHeap
	if info.Internal {
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
	if info.Duration > h.data[0].Duration {
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

// SlowQueryInfo is a struct to record slow query info.
type SlowQueryInfo struct {
	SQL      string
	Start    time.Time
	Duration time.Duration
	Detail   execdetails.ExecDetails
	Succ     bool
	ConnID   uint64
	TxnTS    uint64
	User     string
	DB       string
	TableIDs string
	IndexIDs string
	Internal bool
}
