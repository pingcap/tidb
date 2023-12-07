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
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type spillHelper struct {
	syncLock   sync.Mutex
	spillError error
}

// sortPartitionSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, sortPartitionSpillDiskAction.Action is
// triggered.
type sortPartitionSpillDiskAction struct {
	memory.BaseOOMAction
	partition  *sortPartition
	isSpilling atomic.Bool
	helper     *spillHelper
}

// GetPriority get the priority of the Action.
func (*sortPartitionSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (s *sortPartitionSpillDiskAction) Action(t *memory.Tracker) {
	// TODO wait until spill done if spill is in execution.
	// TODO set fallback, we should kill sql if memory is out of quota after spill is triggered.
	if s.isSpilling.CompareAndSwap(false, true) {
		go func() {
			s.helper.syncLock.Lock()
			defer s.helper.syncLock.Unlock()
			if !s.partition.isSpillTriggeredNoLock() && s.partition.hasEnoughDataToSpill() {
				logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
					zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
				err := s.partition.spillToDisk()
				if err != nil {
					s.helper.spillError = err
				}
			}

			s.isSpilling.CompareAndSwap(true, false)
		}()
	}
}

// It's used only when spill is triggered
type dataCursor struct {
	chkID     int
	rowID     int
	chkRowNum int
	chk       *chunk.Chunk
}

func NewDataCursor() *dataCursor {
	return &dataCursor{
		chkID:     0,
		rowID:     0,
		chkRowNum: 0,
		chk:       nil,
	}
}

func (d *dataCursor) getChkID() int {
	return d.chkID
}

func (d *dataCursor) advanceRow() {
	d.rowID++
}

func (d *dataCursor) getSpilledRow() *chunk.Row {
	if d.rowID >= d.chkRowNum {
		return nil
	}
	row := d.chk.GetRow(d.rowID)
	return &row
}

func (d *dataCursor) setChunk(chk *chunk.Chunk, chkID int) {
	d.chkID = chkID
	d.rowID = 0
	d.chkRowNum = chk.NumRows()
	d.chk = chk
}
