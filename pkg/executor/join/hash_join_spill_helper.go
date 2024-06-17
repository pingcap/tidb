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
	"slices"
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

// TODO write a specific test for this function
// TODO refine this function
func (h *hashJoinSpillHelper) choosePartitionsToSpill() ([]int, int64) {
	partitionNum := h.hashJoinExec.PartitionNumber
	partitionsMemoryUsage := make([]int64, partitionNum)
	for i := 0; i < partitionNum; i++ {
		partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsage(i)
	}

	spilledPartitions := h.hashJoinExec.HashJoinCtxV2.hashTableContext.getSpilledPartitions()

	releasedMemoryUsage := int64(0)
	for _, partID := range spilledPartitions {
		releasedMemoryUsage += partitionsMemoryUsage[partID]
	}

	bytesLimit := h.hashJoinExec.memTracker.GetBytesLimit()
	bytesConsumed := h.hashJoinExec.memTracker.BytesConsumed()
	bytesConsumedAfterReleased := bytesConsumed - releasedMemoryUsage

	// Check if it's enough to spill existing spilled partitions
	if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*0.8 {
		return spilledPartitions, releasedMemoryUsage
	}

	unspilledPartitions := h.hashJoinExec.HashJoinCtxV2.hashTableContext.getUnspilledPartitions()

	type partIDAndMem struct {
		partID      int
		memoryUsage int64
	}

	unspilledPartitionsAndMemory := make([]partIDAndMem, 0, len(unspilledPartitions))
	for _, partID := range unspilledPartitions {
		unspilledPartitionsAndMemory = append(unspilledPartitionsAndMemory, partIDAndMem{partID: partID, memoryUsage: partitionsMemoryUsage[partID]})
	}

	// Sort partitions by memory usage in descend
	slices.SortFunc(unspilledPartitionsAndMemory, func(i, j partIDAndMem) int {
		if i.memoryUsage > j.memoryUsage {
			return -1
		}
		if i.memoryUsage < j.memoryUsage {
			return 1
		}
		return 0
	})

	// Pick half of unspilled partitions to spill
	spilledPartitionNum := len(unspilledPartitionsAndMemory) / 2
	for i := 0; i < spilledPartitionNum; i++ {
		spilledPartitions = append(spilledPartitions, unspilledPartitionsAndMemory[i].partID)
		releasedMemoryUsage += unspilledPartitionsAndMemory[i].memoryUsage
	}

	unspilledPartitionsAndMemory = unspilledPartitionsAndMemory[spilledPartitionNum:]

	bytesConsumedAfterReleased = bytesConsumed - releasedMemoryUsage
	if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*0.8 {
		return spilledPartitions, releasedMemoryUsage
	}

	// Choose more partitions to spill
	for _, item := range unspilledPartitionsAndMemory {
		spilledPartitions = append(spilledPartitions, item.partID)
		releasedMemoryUsage += item.memoryUsage
		if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*0.8 {
			return spilledPartitions, releasedMemoryUsage
		}
	}

	return spilledPartitions, releasedMemoryUsage
}

func (h *hashJoinSpillHelper) spillBuildSideOnePartition(partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.fieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.hashJoinExec.diskTracker)
		h.buildRowsInDisk[partID] = inDisk
	}

	// Get row bytes from segment and spill them
	for _, seg := range segments {
		rows := seg.getRowsBytes()
		for i, row := range rows {
			if h.tmpSpillChunk.IsFull() {
				h.buildRowsInDisk[partID].Add(h.tmpSpillChunk)
				h.tmpSpillChunk.Reset()
			}

			h.tmpSpillChunk.AppendInt64(0, int64(seg.hashValues[i]))
			h.tmpSpillChunk.AppendBytes(1, row)
		}
	}

	// Spill remaining rows in tmpSpillChunk
	if h.tmpSpillChunk.NumRows() > 0 {
		h.buildRowsInDisk[partID].Add(h.tmpSpillChunk)
		h.tmpSpillChunk.Reset()
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

			err := h.spillBuildSideOnePartition(partID, spilledSegments)
			if err != nil {
				errChannel <- util.GetRecoverError(err)
			}
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
