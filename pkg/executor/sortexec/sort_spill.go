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
	partition     *sortPartition
	isSpillNeeded bool
}

// GetPriority get the priority of the Action.
func (*sortPartitionSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (s *sortPartitionSpillDiskAction) isSpillTriggered() bool {
	return s.partition.inDisk != nil
}

func (s *sortPartitionSpillDiskAction) needSpill() bool {
	return s.isSpillNeeded
}

func (s *sortPartitionSpillDiskAction) Action(t *memory.Tracker) {
	// Currently, `Action` is always triggered by only one goroutine, so no lock is needed here so far.
	if !s.isSpillNeeded && s.partition.hasEnoughDataToSpill() {
		logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
			zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
		s.isSpillNeeded = true
		// Only set spill flag, the spill action will be executed in other place.
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
