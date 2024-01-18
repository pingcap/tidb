// Copyright 2023 PingCAP, Inc.
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

package sortexec

import (
	"container/list"
	"math/rand"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// All elements in publicQueue are row slices that have been sorted
type sortedRowsList struct {
	lock            sync.Mutex
	sortedRowsQueue list.List
}

func (p *sortedRowsList) add(rows sortedRows) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.sortedRowsQueue.PushBack(rows)
}

func (p *sortedRowsList) fetchTwoSortedRows() (res1 sortedRows, res2 sortedRows) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.sortedRowsQueue.Len() > 1 {
		res1 := popFromList(&p.sortedRowsQueue)
		res2 := popFromList(&p.sortedRowsQueue)
		return res1, res2
	}
	return nil, nil
}

func (p *sortedRowsList) clear() {
	p.lock.Lock()
	defer p.lock.Unlock()
	elem := p.sortedRowsQueue.Front()
	for elem != nil {
		p.sortedRowsQueue.Remove(elem)
		elem = p.sortedRowsQueue.Front()
	}
}

func (p *sortedRowsList) fetchSortedRowsNoLock() sortedRows {
	return popFromList(&p.sortedRowsQueue)
}

func (p *sortedRowsList) getSortedRowsNumNoLock() int {
	return p.sortedRowsQueue.Len()
}

type rowWithPartition struct {
	row         chunk.Row
	partitionID int
}

type multiWayMerge struct {
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements        []rowWithPartition
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	ret := h.lessRowFunction(rowI, rowJ)
	return ret < 0
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (*multiWayMerge) Push(interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func processPanicAndLog(processError func(error), r interface{}) {
	err := util.GetRecoverError(r)
	processError(err)
	logutil.BgLogger().Error("parallel sort panicked", zap.Error(err), zap.Stack("stack"))
}

// The type of Element.Value should always be `sortedRows`.
func popFromList(l *list.List) sortedRows {
	elem := l.Front()
	if elem == nil {
		return nil
	}
	res, _ := elem.Value.(sortedRows) // Should always success
	l.Remove(elem)
	return res
}

func injectParallelSortRandomFail() {
	failpoint.Inject("ParallelSortRandomFail", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < 5 {
				panic("panic is triggered by random fail")
			}
		}
	})
}
