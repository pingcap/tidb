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

func processPanicAndLog(errOutputChan chan<- rowWithError, r any) {
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
