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

func (*multiWayMergeImpl) Push(any) {
	// Should never be called.
}

func (h *multiWayMergeImpl) Pop() any {
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
			elements:        make([]rowWithPartition, 0, len(sortedRowsIters)),
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

func processPanicAndLog(errOutputChan chan<- rowWithError, r interface{}) {
	err := util.GetRecoverError(r)
	errOutputChan <- rowWithError{err: err}
	logutil.BgLogger().Error("executor panicked", zap.Error(err), zap.Stack("stack"))
}

// chunkWithMemoryUsage contains chunk and memory usage.
// However, some of memory usage may also come from other place,
// not only the chunk's memory usage.
type chunkWithMemoryUsage struct {
	Chk         *chunk.Chunk
	MemoryUsage int64
}

type rowWithError struct {
	row chunk.Row
	err error
}

func injectParallelSortRandomFail(triggerFactor int32) {
	failpoint.Inject("ParallelSortRandomFail", func(val failpoint.Value) {
		if val.(bool) {
			randNum := rand.Int31n(10000)
			if randNum < triggerFactor {
				panic("panic is triggered by random fail")
			}
		}
	})
}

// It's used only when spill is triggered
type dataCursor struct {
	chkID     int
	chunkIter *chunk.Iterator4Chunk
}

// NewDataCursor creates a new dataCursor
func NewDataCursor() *dataCursor {
	return &dataCursor{
		chkID:     -1,
		chunkIter: chunk.NewIterator4Chunk(nil),
	}
}

func (d *dataCursor) getChkID() int {
	return d.chkID
}

func (d *dataCursor) begin() chunk.Row {
	return d.chunkIter.Begin()
}

func (d *dataCursor) next() chunk.Row {
	return d.chunkIter.Next()
}

func (d *dataCursor) setChunk(chk *chunk.Chunk, chkID int) {
	d.chkID = chkID
	d.chunkIter.ResetChunk(chk)
}

func reloadCursor(cursor *dataCursor, inDisk *chunk.DataInDiskByChunks) (bool, error) {
	spilledChkNum := inDisk.NumChunks()
	restoredChkID := cursor.getChkID() + 1
	if restoredChkID >= spilledChkNum {
		// All data has been consumed
		return false, nil
	}

	chk, err := inDisk.GetChunk(restoredChkID)
	if err != nil {
		return false, err
	}
	cursor.setChunk(chk, restoredChkID)
	return true, nil
}
