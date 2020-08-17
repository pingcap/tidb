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
	"sort"
	"sync"
	"time"

	"github.com/pingcap/parser/ast"
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

func (h *slowQueryHeap) RemoveExpired(now time.Time, period time.Duration) {
	// Remove outdated slow query element.
	idx := 0
	for i := 0; i < len(h.data); i++ {
		outdateTime := h.data[i].Start.Add(period)
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

func (h *slowQueryHeap) Query(count int) []*SlowQueryInfo {
	// The sorted array still maintains the heap property.
	sort.Sort(h)

	// The result should be in decrease order.
	return takeLastN(h.data, count)
}

type slowQueryQueue struct {
	data []*SlowQueryInfo
	size int
}

func (q *slowQueryQueue) Enqueue(info *SlowQueryInfo) {
	if len(q.data) < q.size {
		q.data = append(q.data, info)
		return
	}

	q.data = append(q.data, info)[1:]
}

func (q *slowQueryQueue) Query(count int) []*SlowQueryInfo {
	// Queue is empty.
	if len(q.data) == 0 {
		return nil
	}
	return takeLastN(q.data, count)
}

func takeLastN(data []*SlowQueryInfo, count int) []*SlowQueryInfo {
	if count > len(data) {
		count = len(data)
	}
	ret := make([]*SlowQueryInfo, 0, count)
	for i := len(data) - 1; i >= 0 && len(ret) < count; i-- {
		ret = append(ret, data[i])
	}
	return ret
}

// topNSlowQueries maintains two heaps to store recent slow queries: one for user's and one for internal.
// N = 30, period = 7 days by default.
// It also maintains a recent queue, in a FIFO manner.
type topNSlowQueries struct {
	recent   slowQueryQueue
	user     slowQueryHeap
	internal slowQueryHeap
	topN     int
	period   time.Duration
	ch       chan *SlowQueryInfo
	msgCh    chan *showSlowMessage

	mu struct {
		sync.RWMutex
		closed bool
	}
}

func newTopNSlowQueries(topN int, period time.Duration, queueSize int) *topNSlowQueries {
	ret := &topNSlowQueries{
		topN:   topN,
		period: period,
		ch:     make(chan *SlowQueryInfo, 1000),
		msgCh:  make(chan *showSlowMessage, 10),
	}
	ret.user.data = make([]*SlowQueryInfo, 0, topN)
	ret.internal.data = make([]*SlowQueryInfo, 0, topN)
	ret.recent.size = queueSize
	ret.recent.data = make([]*SlowQueryInfo, 0, queueSize)
	return ret
}

func (q *topNSlowQueries) Append(info *SlowQueryInfo) {
	// Put into the recent queue.
	q.recent.Enqueue(info)

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

func (q *topNSlowQueries) QueryAll() []*SlowQueryInfo {
	return q.recent.data
}

func (q *topNSlowQueries) RemoveExpired(now time.Time) {
	q.user.RemoveExpired(now, q.period)
	q.internal.RemoveExpired(now, q.period)
}

type showSlowMessage struct {
	request *ast.ShowSlow
	result  []*SlowQueryInfo
	sync.WaitGroup
}

func (q *topNSlowQueries) QueryRecent(count int) []*SlowQueryInfo {
	return q.recent.Query(count)
}

func (q *topNSlowQueries) QueryTop(count int, kind ast.ShowSlowKind) []*SlowQueryInfo {
	var ret []*SlowQueryInfo
	switch kind {
	case ast.ShowSlowKindDefault:
		ret = q.user.Query(count)
	case ast.ShowSlowKindInternal:
		ret = q.internal.Query(count)
	case ast.ShowSlowKindAll:
		tmp := make([]*SlowQueryInfo, 0, len(q.user.data)+len(q.internal.data))
		tmp = append(tmp, q.user.data...)
		tmp = append(tmp, q.internal.data...)
		tmp1 := slowQueryHeap{tmp}
		sort.Sort(&tmp1)
		ret = takeLastN(tmp, count)
	}
	return ret
}

func (q *topNSlowQueries) Close() {
	q.mu.Lock()
	q.mu.closed = true
	q.mu.Unlock()

	close(q.ch)
}

// SlowQueryInfo is a struct to record slow query info.
type SlowQueryInfo struct {
	SQL        string
	Start      time.Time
	Duration   time.Duration
	Detail     execdetails.ExecDetails
	ConnID     uint64
	TxnTS      uint64
	User       string
	DB         string
	TableIDs   string
	IndexNames string
	Digest     string
	Internal   bool
	Succ       bool
}
