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

	"github.com/pingcap/tidb/util/execdetails"
)

// topNSlowQuery maintains a heap to store recent slow queries.
// N = 30, recent = 7 days by default.
type topNSlowQuery struct {
	data   []*slowQueryInfo
	offset int
	recent time.Duration
	ch     chan *slowQueryInfo
}

func newTopNSlowQuery(topN int, recent time.Duration) *topNSlowQuery {
	return &topNSlowQuery{
		data:   make([]*slowQueryInfo, topN),
		recent: recent,
		ch:     make(chan *slowQueryInfo, 1000),
	}
}

func (q *topNSlowQuery) Close() {
	close(q.ch)
}

func (q *topNSlowQuery) Push(info *slowQueryInfo) {
	// Heap is not full, append to it and sift up.
	if q.offset < len(q.data) {
		q.data[q.offset] = info
		q.siftUp(q.offset)
		q.offset++
		return
	}

	// Replace the heap top and sift down.
	if info.duration > q.data[0].duration {
		q.data[0] = info
		for i := 0; i < q.offset; {
			left := 2*i + 1
			right := 2 * (i + 1)
			if left >= q.offset {
				break
			}
			smaller := left
			if right < q.offset && q.data[right].duration < q.data[left].duration {
				smaller = right
			}
			if q.data[i].duration <= q.data[smaller].duration {
				break
			}
			q.data[i], q.data[smaller] = q.data[smaller], q.data[i]
			i = smaller
		}
	}
}

func (q *topNSlowQuery) siftUp(end int) {
	for i := end; i > 0; {
		j := (i - 1) / 2
		if q.data[j].duration < q.data[i].duration {
			break
		}
		q.data[i], q.data[j] = q.data[j], q.data[i]
		i = j
	}
}

func (q *topNSlowQuery) Refresh(now time.Time) {
	// Remove outdated slow query element.
	idx := 0
	for i := 0; i < q.offset; i++ {
		outdateTime := q.data[i].start.Add(q.recent)
		if outdateTime.After(now) {
			q.data[idx] = q.data[i]
			idx++
		}
	}
	if q.offset == idx {
		return
	}
	q.offset = idx

	// Rebuild the heap.
	for i := 1; i < q.offset; i++ {
		q.siftUp(i)
	}
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
