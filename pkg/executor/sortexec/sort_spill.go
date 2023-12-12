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

type spillHelper struct {
	syncLock   sync.Mutex
	spillError error

	lock       *sync.Mutex
	cond       sync.Cond
	isSpilling bool
}

func newSpillHelper() *spillHelper {
	lock := new(sync.Mutex)
	return &spillHelper{
		spillError: nil,
		lock:       lock,
		cond:       *sync.NewCond(lock),
		isSpilling: false,
	}
}

func (s *spillHelper) setErrorNoLock(err error) {
	s.spillError = err
}

func (s *spillHelper) checkError() error {
	s.syncLock.Lock()
	defer s.syncLock.Unlock()
	return s.spillError
}

func (s *spillHelper) checkErrorNoLock() error {
	return s.spillError
}

// sortPartitionSpillDiskAction implements memory.ActionOnExceed for chunk.List. If
// the memory quota of a query is exceeded, sortPartitionSpillDiskAction.Action is
// triggered.
type sortPartitionSpillDiskAction struct {
	memory.BaseOOMAction
	once      sync.Once
	partition *sortPartition
	helper    *spillHelper
}

// GetPriority get the priority of the Action.
func (*sortPartitionSpillDiskAction) GetPriority() int64 {
	return memory.DefSpillPriority
}

func (s *sortPartitionSpillDiskAction) Action(t *memory.Tracker) {
	s.helper.syncLock.Lock()
	defer s.helper.syncLock.Unlock()

	needCheckExceed := true
	if !s.partition.isSpillTriggered() && s.partition.hasEnoughDataToSpill() {
		s.once.Do(func() {
			needCheckExceed = false
			go func() {
				s.helper.syncLock.Lock()
				defer s.helper.syncLock.Unlock()
				logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
					zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))
				s.helper.isSpilling = true
				err := s.partition.spillToDisk()
				if err != nil {
					s.helper.setErrorNoLock(err)
				}
				s.helper.isSpilling = false
				s.helper.cond.Broadcast()
			}()
		})
	}

	s.helper.lock.Lock()
	for s.helper.isSpilling {
		s.helper.cond.Wait()
	}
	s.helper.lock.Unlock()

	if !needCheckExceed || !t.CheckExceed() {
		return
	}

	if fallback := s.GetFallback(); fallback != nil {
		fallback.Action(t)
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
