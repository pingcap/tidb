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

package join

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type hashJoinSpillHelper struct {
	cond         *sync.Cond
	spillStatus  int
	hashJoinExec *HashJoinV2Exec

	buildRowsInDisk []*chunk.DataInDiskByChunks
	probeRowsInDisk []*chunk.DataInDiskByChunks

	fieldTypes    []*types.FieldType
	tmpSpillChunk *chunk.Chunk

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64
}

func newHashJoinSpillHelper(hashJoinExec *HashJoinV2Exec) *hashJoinSpillHelper {
	helper := &hashJoinSpillHelper{hashJoinExec: hashJoinExec}
	helper.fieldTypes = append(helper.fieldTypes, types.NewFieldType(mysql.TypeLonglong))
	helper.fieldTypes = append(helper.fieldTypes, types.NewFieldType(mysql.TypeBit))
	return helper
}

func (h *hashJoinSpillHelper) close() {
	for _, inDisk := range h.buildRowsInDisk {
		if inDisk != nil {
			inDisk.Close()
		}
	}
	for _, inDisk := range h.probeRowsInDisk {
		if inDisk != nil {
			inDisk.Close()
		}
	}

	if h.tmpSpillChunk != nil {
		h.tmpSpillChunk.Destroy(spillChunkSize, h.fieldTypes)
	}
}

func (h *hashJoinSpillHelper) setNotSpilled() {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	h.spillStatus = notSpilled
}

func (h *hashJoinSpillHelper) setInSpilling() {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	h.spillStatus = inSpilling
}

func (h *hashJoinSpillHelper) setNeedSpillNoLock() {
	h.spillStatus = needSpill
}

func (h *hashJoinSpillHelper) isNotSpilledNoLock() bool {
	return h.spillStatus == notSpilled
}

func (h *hashJoinSpillHelper) isInSpillingNoLock() bool {
	return h.spillStatus == inSpilling
}

func (h *hashJoinSpillHelper) isInSpilling() bool {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	return h.spillStatus == inSpilling
}

func (h *hashJoinSpillHelper) isSpillTriggered() bool {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	return len(h.buildRowsInDisk) > 0
}

func (h *hashJoinSpillHelper) choosePartitionsToSpill() ([]int, int64) {
	partitionNum := h.hashJoinExec.PartitionNumber
	partitionsMemoryUsage := make([]int64, partitionNum)
	for i := 0; i < partitionNum; i++ {
		partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsage(i)
	}

	spilledPartitions := make([]int, 0)

	// TODO choose some partitions to spill
	// TODO some partitions may have been spilled before, we must spill them again
	// TODO When not all partitions are spilled, calculate how many memory will be released by chosen workers, if released memory is not enough, choose more partitions
	return spilledPartitions, 0 // TODO
}

func (h *hashJoinSpillHelper) spillBuildSideOnePartition(partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.fieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.hashJoinExec.diskTracker)
		h.buildRowsInDisk[partID] = inDisk
	}

	for _, seg := range segments {
		
	}

	return nil
}

func (h *hashJoinSpillHelper) initIfNeed() {
	if h.buildRowsInDisk == nil {
		// It's the first time that spill is triggered
		h.tmpSpillChunk = chunk.NewChunkFromPoolWithCapacity(h.fieldTypes, spillChunkSize)

		partitionNum := h.hashJoinExec.PartitionNumber
		h.buildRowsInDisk = make([]*chunk.DataInDiskByChunks, partitionNum)
		h.probeRowsInDisk = make([]*chunk.DataInDiskByChunks, partitionNum)
	}
}

func (h *hashJoinSpillHelper) spillInBuildStage() error {
	h.setInSpilling()
	defer h.cond.Broadcast()
	defer h.setNotSpilled()

	err := checkSQLKiller(&h.hashJoinExec.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuildSpill")
	if err != nil {
		return err
	}

	h.initIfNeed()

	partitionsNeedSpill, totalReleasedMemory := h.choosePartitionsToSpill()

	workerNum := len(h.hashJoinExec.BuildWorkers)
	errChannel := make(chan error, workerNum)

	waiter := &sync.WaitGroup{}
	waiter.Add(len(partitionsNeedSpill))

	logutil.BgLogger().Info(spillInfo, zap.Int64("consumed", h.bytesConsumed.Load()), zap.Int64("quota", h.bytesLimit.Load()))
	for i := 0; i < len(partitionsNeedSpill); i++ {
		go func(partID int) {
			defer func() {
				if err := recover(); err != nil {
					errChannel <- util.GetRecoverError(err)
				}
				defer waiter.Done()
			}()

			spilledSegments := make([]*rowTableSegment, 0)

			// finalize current segment of partition `partID` of each worker
			for i := 0; i < workerNum; i++ {
				worker := h.hashJoinExec.BuildWorkers[i]
				builder := worker.builder
				if builder == nil {
					// TODO check that test could reach to here
					continue
				}
				startPosInRawData := builder.startPosInRawData[partID]
				if len(startPosInRawData) > 0 {
					worker.HashJoinCtx.hashTableContext.finalizeCurrentSeg(i, partID, worker.builder, false)
				}
				spilledSegments = append(spilledSegments, worker.getSegments(partID)...)
				worker.clearSegments(partID)
			}

			// TODO spill

		}(partitionsNeedSpill[i])
	}

	waiter.Wait()
	close(errChannel)
	for err := range errChannel {
		return err
	}
	h.hashJoinExec.hashTableContext.memoryTracker.Consume(-totalReleasedMemory)
	return nil
}
