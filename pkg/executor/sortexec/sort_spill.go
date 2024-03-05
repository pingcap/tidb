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
	"sync"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

// sortPartitionSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, sortPartitionSpillDiskAction.Action is
// triggered.
type sortPartitionSpillDiskAction struct {
	memory.BaseOOMAction
	once      sync.Once
	partition *sortPartition
}

// GetPriority get the priority of the Action.
func (*sortPartitionSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (s *sortPartitionSpillDiskAction) Action(t *memory.Tracker) {
	fallBack := s.executeAction(t)
	if fallBack != nil {
		fallBack.Action(t)
	}
}

func (s *sortPartitionSpillDiskAction) executeAction(t *memory.Tracker) memory.ActionOnExceed {
	s.partition.cond.L.Lock()
	defer s.partition.cond.L.Unlock()

	for s.partition.getIsSpillingNoLock() {
		s.partition.cond.Wait()
	}

	if !s.partition.isSpillTriggeredNoLock() && s.partition.hasEnoughDataToSpill() {
		s.once.Do(func() {
			go func() {
				logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
					zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
				err := s.partition.spillToDisk()
				if err != nil {
					s.partition.setError(err)
				}
			}()
		})

		// Ideally, all goroutines entering this action should wait for the finish of spill once
		// spill is triggered(we consider spill is triggered when the spill goroutine gets the syncLock).
		// However, out of some reasons, we have to directly return the goroutine before the finish of
		// sort operation which is executed in `s.partition.spillToDisk()` as sort will retrigger the action
		// and lead to dead lock.
		return nil
	}

	if !t.CheckExceed() {
		return nil
	}

	return s.GetFallback()
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
	d.chunkIter = chunk.NewIterator4Chunk(chk)
}
