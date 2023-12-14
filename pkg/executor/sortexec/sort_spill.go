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
	syncLock sync.Mutex

	lock            *sync.Mutex
	cond            sync.Cond
	spillError      error
	isSpilling      bool
	isSpillTrigered bool
}

func newSpillHelper() *spillHelper {
	lock := new(sync.Mutex)
	return &spillHelper{
		lock:            lock,
		cond:            *sync.NewCond(lock),
		spillError:      nil,
		isSpilling:      false,
		isSpillTrigered: false,
	}
}

func (s *spillHelper) reset() {
	s.isSpilling = false
	s.isSpillTrigered = false
}

func (s *spillHelper) setIsSpilling(isSpilling bool) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if isSpilling {
		s.setSpillTriggeredNoLock()
	}
	s.isSpilling = isSpilling
}

func (s *spillHelper) setSpillTriggeredNoLock() {
	s.isSpillTrigered = true
}

func (s *spillHelper) isSpillTriggeredNoLock() bool {
	return s.isSpillTrigered
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
	fallBack := s.executeAction(t)
	if fallBack != nil {
		fallBack.Action(t)
	}
}

func (s *sortPartitionSpillDiskAction) executeAction(t *memory.Tracker) memory.ActionOnExceed {
	s.helper.lock.Lock()
	defer s.helper.lock.Unlock()

	if !s.helper.isSpillTriggeredNoLock() && s.partition.hasEnoughDataToSpill() {
		s.once.Do(func() {
			go func() {
				s.helper.syncLock.Lock()
				defer s.helper.syncLock.Unlock()
				logutil.BgLogger().Info("memory exceeds quota, spill to disk now.",
					zap.Int64("consumed", t.BytesConsumed()), zap.Int64("quota", t.GetBytesLimit()))

				err := s.partition.spillToDisk()
				if err != nil {
					s.helper.setErrorNoLock(err)
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

	for s.helper.isSpilling {
		s.helper.cond.Wait()
	}

	if !t.CheckExceed() {
		return nil
	}

	return s.GetFallback()
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
