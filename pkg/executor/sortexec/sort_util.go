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
	"container/heap"
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var errSpillEmptyChunk = errors.New("can not spill empty chunk to disk")
var errFailToAddChunk = errors.New("fail to add chunk")

// It should be const, but we need to modify it for test.
var spillChunkSize = 1024

// signalCheckpointForSort indicates the times of row comparation that a signal detection will be triggered.
const signalCheckpointForSort uint = 10240

const (
	notSpilled = iota
	needSpill
	inSpilling
	spillTriggered
)

type rowWithPartition struct {
	row         chunk.Row
	partitionID int
}

type multiWayMergeImpl struct {
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) int
	elements        []rowWithPartition
}

func (h *multiWayMergeImpl) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	ret := h.lessRowFunction(rowI, rowJ)
	return ret < 0
}

func (h *multiWayMergeImpl) Len() int {
	return len(h.elements)
}

func (*multiWayMergeImpl) Push(interface{}) {
	// Should never be called.
}

func (h *multiWayMergeImpl) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMergeImpl) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

type multiWayMerger struct {
	sortedRowsIters []*chunk.Iterator4Slice
	multiWayMerge   *multiWayMergeImpl
}

func newMultiWayMerger(
	sortedRowsIters []*chunk.Iterator4Slice,
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) int) *multiWayMerger {
	return &multiWayMerger{
		sortedRowsIters: sortedRowsIters,
		multiWayMerge: &multiWayMergeImpl{
			lessRowFunction: lessRowFunction,
			elements:        make([]rowWithPartition, len(sortedRowsIters)),
		},
	}
}

func (m *multiWayMerger) init() {
	for i := range m.sortedRowsIters {
		row := m.sortedRowsIters[i].Begin()
		if row.IsEmpty() {
			continue
		}
		m.multiWayMerge.elements = append(m.multiWayMerge.elements, rowWithPartition{row: row, partitionID: i})
	}
	heap.Init(m.multiWayMerge)
}

func (m *multiWayMerger) next() chunk.Row {
	if m.multiWayMerge.Len() > 0 {
		elem := m.multiWayMerge.elements[0]
		newRow := m.sortedRowsIters[elem.partitionID].Next()
		if newRow.IsEmpty() {
			heap.Remove(m.multiWayMerge, 0)
			return elem.row
		}
		m.multiWayMerge.elements[0].row = newRow
		heap.Fix(m.multiWayMerge, 0)
		return elem.row
	}
	return chunk.Row{}
}

func processPanicAndLog(processError func(error), r interface{}) {
	err := util.GetRecoverError(r)
	processError(err)
	logutil.BgLogger().Error("parallel sort panicked", zap.Error(err), zap.Stack("stack"))
}

// chunkWithMemoryUsage contains chunk and memory usage.
// However, some of memory usage may also come from other place,
// not only the chunk's memory usage.
type chunkWithMemoryUsage struct {
	Chk         *chunk.Chunk
	MemoryUsage int64
}

func injectParallelSortRandomFail() {
	failpoint.Inject("ParallelSortRandomFail", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < 3 {
				panic("panic is triggered by random fail")
			}
		}
	})
}
