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
	"sync"

	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type publicMergeSpace struct {
	lock        *sync.Mutex
	publicQueue list.List // Type is sortedRows
}

func (p *publicMergeSpace) length() int {
	return p.publicQueue.Len()
}

// If there is sortedRows in the queue, fetch them, or we should put the rows into queue.
func (p *publicMergeSpace) fetchOrPutSortedRows(rows sortedRows) sortedRows {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.publicQueue.Len() > 0 {
		return popFromList(&p.publicQueue)
	} else {
		p.publicQueue.PushBack(rows)
		return nil
	}
}

type partitionPointer struct {
	chk         *chunk.Chunk
	row         chunk.Row
	partitionID int
	consumed    int
}

type multiWayMerge struct {
	lessRowFunction     func(rowI chunk.Row, rowJ chunk.Row) bool
	compressRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements            []partitionPointer
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	return h.lessRowFunction(rowI, rowJ)
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

func processErrorAndLog(processError func(error), r interface{}) {
	err := util.GetRecoverError(r)
	processError(err)
	logutil.BgLogger().Error("parallel sort panicked", zap.Error(err), zap.Stack("stack"))
}

// The type of Element.Value should always be `sortedRows`.
func popFromList(l *list.List) sortedRows {
	elem := l.Front()
	res, _ := elem.Value.(sortedRows) // Should always success
	l.Remove(elem)
	return res
}
