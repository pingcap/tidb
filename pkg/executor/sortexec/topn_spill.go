// Copyright 2024 PingCAP, Inc.
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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
)

type topNSpillHelper struct {
	cond             *sync.Cond
	spillStatus      int
	sortedRowsInDisk []*chunk.DataInDiskByChunks
	topNExec         *TopNExec // TODO maybe this topNExec can be removed

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	fieldTypes         []*types.FieldType
	tmpSpillChunksChan chan *chunk.Chunk

	// We record max value row in each spill, so that workers
	// could filter some useless rows.
	// Though the field type is chunk.Chunk, it contains only one row.
	// If we set this type as chunk.Row, all rows in the chunk referred
	// by this row will not release their memory.
	maxValueRow chunk.Chunk // TODO initialize this chunk
	workers     []*topNWorker

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64
}

// TODO complete this function
func newTopNSpillerHelper() *topNSpillHelper {
	return nil
}

func (t *topNSpillHelper) isNotSpilledNoLock() bool {
	return t.spillStatus == notSpilled
}

func (t *topNSpillHelper) isInSpillingNoLock() bool {
	return t.spillStatus == inSpilling
}

func (t *topNSpillHelper) isSpillNeeded() bool {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return t.spillStatus == needSpill
}

func (t *topNSpillHelper) addInDisk(inDisk *chunk.DataInDiskByChunks) {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	t.sortedRowsInDisk = append(t.sortedRowsInDisk, inDisk)
}

func (t *topNSpillHelper) isSpillTriggered() bool {
	t.cond.L.Lock()
	defer t.cond.L.Unlock()
	return len(t.sortedRowsInDisk) > 0
}

func (t *topNSpillHelper) spillTmpSpillChunk(inDisk *chunk.DataInDiskByChunks, tmpSpillChunk *chunk.Chunk) error {
	err := inDisk.Add(tmpSpillChunk)
	if err != nil {
		return err
	}
	tmpSpillChunk.Reset()
	return nil
}

func (t *topNSpillHelper) spill() error {
	return nil // TODO
}

func (t *topNSpillHelper) spillHeap(chkHeap *topNChunkHeap) error {
	slices.SortFunc(chkHeap.rowPtrs, t.topNExec.keyColumnsCompare)

	// TODO update maxValueRow

	tmpSpillChunk := <-t.tmpSpillChunksChan
	tmpSpillChunk.Reset()

	inDisk := chunk.NewDataInDiskByChunks(t.fieldTypes)
	inDisk.GetDiskTracker().AttachTo(t.topNExec.diskTracker)

	rowPtrNum := len(chkHeap.rowPtrs)
	for ; chkHeap.idx < rowPtrNum; chkHeap.idx++ {
		if tmpSpillChunk.IsFull() {
			err := t.spillTmpSpillChunk(inDisk, tmpSpillChunk)
			if err != nil {
				return err
			}
		}
		tmpSpillChunk.AppendRow(chkHeap.rowChunks.GetRow(chkHeap.rowPtrs[chkHeap.idx]))
	}

	// Spill remaining rows in tmpSpillChunk
	if tmpSpillChunk.NumRows() > 0 {
		err := t.spillTmpSpillChunk(inDisk, tmpSpillChunk)
		if err != nil {
			return err
		}
	}

	t.addInDisk(inDisk)

	t.tmpSpillChunksChan <- tmpSpillChunk
	chkHeap.clear()
	return nil
}

type topNSpillAction struct {
	memory.BaseOOMAction
	helper *topNSpillHelper
}

// GetPriority get the priority of the Action.
func (*topNSpillAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (s *topNSpillAction) Action(t *memory.Tracker) {
	fallBack := s.executeAction(t)
	if fallBack != nil {
		fallBack.Action(t)
	}
}

func (s *topNSpillAction) executeAction(t *memory.Tracker) memory.ActionOnExceed {
	// TODO when we should call fall back?
	return s.GetFallback()
}
