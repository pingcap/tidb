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
	"bytes"
	"hash"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

type hashJoinSpillHelper struct {
	cond         *sync.Cond
	spillStatus  int
	hashJoinExec *HashJoinV2Exec

	buildRowsInDisk [][]*chunk.DataInDiskByChunks
	probeRowsInDisk [][]*chunk.DataInDiskByChunks

	buildSpillChkFieldTypes []*types.FieldType
	probeFieldTypes         []*types.FieldType
	tmpSpillBuildSideChunks []*chunk.Chunk
	tmpSpillProbeSideChunks []*chunk.Chunk

	// When respilling a row, we need to recalculate the row's hash value.
	// These are auxiliary utility for rehash.
	hash      hash.Hash64
	rehashBuf *bytes.Buffer

	stack restoreStack

	// inDisk that has been popped from stack should be saved here
	// so that we can ensure all inDisk will be closed or disk
	// resource will be leaked.
	discardedInDisk []*chunk.DataInDiskByChunks

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64

	// The hash value in restored probe row needs to be updated before we respill this row,
	// and other columns in the row can be directly repilled.
	// This variable describes which columns can be directed respilled.
	probeSpilledRowIdx []int

	spilledPartitions []bool
	spillTriggered    bool

	triggeredInBuildStageForTest bool
	spillRoundForTest            int
}

func newHashJoinSpillHelper(hashJoinExec *HashJoinV2Exec, partitionNum int, probeFieldTypes []*types.FieldType) *hashJoinSpillHelper {
	helper := &hashJoinSpillHelper{hashJoinExec: hashJoinExec}
	helper.cond = sync.NewCond(new(sync.Mutex))
	helper.buildSpillChkFieldTypes = make([]*types.FieldType, 0, 3)
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeLonglong)) // hash value
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit))      // valid join key
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit))      // row data
	helper.probeFieldTypes = make([]*types.FieldType, 0, 3)
	helper.probeFieldTypes = append(helper.probeFieldTypes, types.NewFieldType(mysql.TypeLonglong)) // hash value
	helper.probeFieldTypes = append(helper.probeFieldTypes, types.NewFieldType(mysql.TypeBit))      // serialized key
	helper.probeFieldTypes = append(helper.probeFieldTypes, probeFieldTypes...)                     // row data
	helper.spilledPartitions = make([]bool, partitionNum)
	helper.hash = fnv.New64()
	helper.rehashBuf = new(bytes.Buffer)

	helper.probeSpilledRowIdx = make([]int, 0, len(helper.probeFieldTypes)-1)
	for i := 1; i < len(helper.probeFieldTypes); i++ {
		helper.probeSpilledRowIdx = append(helper.probeSpilledRowIdx, i)
	}

	// hashJoinExec may be nil in test
	if hashJoinExec != nil {
		helper.memTracker = hashJoinExec.memTracker
		helper.diskTracker = hashJoinExec.diskTracker
	}
	return helper
}

func (h *hashJoinSpillHelper) close() {
	for _, chk := range h.tmpSpillBuildSideChunks {
		chk.Destroy(spillChunkSize, h.buildSpillChkFieldTypes)
	}

	for _, chk := range h.tmpSpillProbeSideChunks {
		chk.Destroy(spillChunkSize, h.probeFieldTypes)
	}

	for _, inDisks := range h.buildRowsInDisk {
		for _, inDisk := range inDisks {
			if inDisk != nil {
				inDisk.Close()
			}
		}
	}
	for _, inDisks := range h.probeRowsInDisk {
		for _, inDisk := range inDisks {
			if inDisk != nil {
				inDisk.Close()
			}
		}
	}

	partition := h.stack.pop()
	for partition != nil {
		for _, inDisk := range partition.buildSideChunks {
			inDisk.Close()
		}
		for _, inDisk := range partition.probeSideChunks {
			inDisk.Close()
		}
		partition = h.stack.pop()
	}

	for _, inDisk := range h.discardedInDisk {
		inDisk.Close()
	}
}

func (h *hashJoinSpillHelper) getSpilledPartitions() []int {
	spilledPartitions := make([]int, 0)
	for i, spilled := range h.spilledPartitions {
		if spilled {
			spilledPartitions = append(spilledPartitions, i)
		}
	}
	return spilledPartitions
}

func (h *hashJoinSpillHelper) getUnspilledPartitions() []int {
	unspilledPartitions := make([]int, 0)
	for i, spilled := range h.spilledPartitions {
		if !spilled {
			unspilledPartitions = append(unspilledPartitions, i)
		}
	}
	return unspilledPartitions
}

func (h *hashJoinSpillHelper) setPartitionSpilled(partIDs []int) {
	for _, partID := range partIDs {
		h.spilledPartitions[partID] = true
	}
	h.spillTriggered = true
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

func (h *hashJoinSpillHelper) isSpillNeeded() bool {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	return h.spillStatus == needSpill
}

func (h *hashJoinSpillHelper) isSpillTriggeredNoLock() bool {
	return h.spillTriggered
}

func (h *hashJoinSpillHelper) isPartitionSpilled(partID int) bool {
	return h.spilledPartitions[partID]
}

// func (h *hashJoinSpillHelper)

func (h *hashJoinSpillHelper) discardInDisks(inDisks [][]*chunk.DataInDiskByChunks) error {
	hasNilInDisk := false
	for _, disks := range inDisks {
		for _, disk := range disks {
			if disk == nil {
				hasNilInDisk = true
			}
			h.discardedInDisk = append(h.discardedInDisk, disk)
		}
	}

	if hasNilInDisk {
		return errors.NewNoStackError("Receive nil DataInDiskByChunks")
	}
	return nil
}

// TODO write a specific test for this function
// TODO refine this function
func (h *hashJoinSpillHelper) choosePartitionsToSpill() ([]int, int64) {
	partitionNum := h.hashJoinExec.PartitionNumber
	partitionsMemoryUsage := make([]int64, partitionNum)
	for i := 0; i < partitionNum; i++ {
		partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsage(i)
	}

	spilledPartitions := h.getSpilledPartitions()

	releasedMemoryUsage := int64(0)
	for _, partID := range spilledPartitions {
		releasedMemoryUsage += partitionsMemoryUsage[partID]
	}

	bytesLimit := h.memTracker.GetBytesLimit()
	bytesConsumed := h.memTracker.BytesConsumed()
	bytesConsumedAfterReleased := bytesConsumed - releasedMemoryUsage

	// Check if it's enough to spill existing spilled partitions
	if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*0.8 {
		return spilledPartitions, releasedMemoryUsage
	}

	unspilledPartitions := h.getUnspilledPartitions()

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

func (h *hashJoinSpillHelper) generateSpilledValidJoinKey(seg *rowTableSegment, validJoinKeys []byte) []byte {
	rowLen := len(seg.rowStartOffset)
	validJoinKeys = validJoinKeys[:rowLen]
	for i := 0; i < rowLen; i++ {
		validJoinKeys[i] = byte(0)
	}
	for _, pos := range seg.validJoinKeyPos {
		validJoinKeys[pos] = byte(1)
	}
	return validJoinKeys
}

func (h *hashJoinSpillHelper) spillSegmentsToDisk(workerID int, partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[workerID] == nil {
		h.buildRowsInDisk[workerID] = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
		h.probeRowsInDisk[workerID] = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
	}
	
	if h.buildRowsInDisk[workerID][partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.buildRowsInDisk[workerID][partID] = inDisk

		inDisk = chunk.NewDataInDiskByChunks(h.probeFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.probeRowsInDisk[workerID][partID] = inDisk
	}

	return h.spillSegmentsToDiskImpl(workerID, h.buildRowsInDisk[workerID][partID], segments)
}

func (h *hashJoinSpillHelper) spillSegmentsToDiskImpl(workerID int, disk *chunk.DataInDiskByChunks, segments []*rowTableSegment) error {
	// TODO we can validJoinKeys buffer into hashJoinSpillHelper so that we can avoid repeated allocation
	validJoinKeys := make([]byte, 0, maxRowTableSegmentSize)
	h.tmpSpillBuildSideChunks[workerID].Reset()

	// Get row bytes from segment and spill them
	for _, seg := range segments {
		validJoinKeys = h.generateSpilledValidJoinKey(seg, validJoinKeys)
		rows := seg.getRowsBytesForSpill()

		for i, row := range rows {
			if h.tmpSpillBuildSideChunks[workerID].IsFull() {
				err := disk.Add(h.tmpSpillBuildSideChunks[workerID])
				if err != nil {
					return err
				}
				h.tmpSpillBuildSideChunks[workerID].Reset()
			}

			h.tmpSpillBuildSideChunks[workerID].AppendInt64(0, int64(seg.hashValues[i]))
			h.tmpSpillBuildSideChunks[workerID].AppendBytes(1, validJoinKeys[i:i+1])
			h.tmpSpillBuildSideChunks[workerID].AppendBytes(2, row)
		}
	}

	// Spill remaining rows in tmpSpillChunk[0]
	if h.tmpSpillBuildSideChunks[workerID].NumRows() > 0 {
		err := disk.Add(h.tmpSpillBuildSideChunks[workerID])
		if err != nil {
			return err
		}
		h.tmpSpillBuildSideChunks[workerID].Reset()
	}
	return nil
}

func (h *hashJoinSpillHelper) spillProbeChk(workerID int, partID int, chk *chunk.Chunk) error {
	return h.probeRowsInDisk[workerID][partID].Add(chk)
}

func (h *hashJoinSpillHelper) init() {
	if h.buildRowsInDisk == nil {
		// It's the first time that spill is triggered
		h.initTmpSpillBuildSideChunks()

		h.buildRowsInDisk = make([][]*chunk.DataInDiskByChunks, h.hashJoinExec.Concurrency)
		h.probeRowsInDisk = make([][]*chunk.DataInDiskByChunks, h.hashJoinExec.Concurrency)
	}
}

func (h *hashJoinSpillHelper) spillBuildRows() error {
	h.setInSpilling()
	defer h.cond.Broadcast()
	defer h.setNotSpilled()

	err := checkSQLKiller(&h.hashJoinExec.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuildSpill")
	if err != nil {
		return err
	}

	h.triggeredInBuildStageForTest = true

	h.init()

	partitionsNeedSpill, totalReleasedMemory := h.choosePartitionsToSpill()

	workerNum := len(h.hashJoinExec.BuildWorkers)
	errChannel := make(chan error, workerNum)

	waiter := &sync.WaitGroup{}
	waiter.Add(len(partitionsNeedSpill))

	h.setPartitionSpilled(partitionsNeedSpill)

	logutil.BgLogger().Info(spillInfo, zap.Int64("consumed", h.bytesConsumed.Load()), zap.Int64("quota", h.bytesLimit.Load()))
	for i := 0; i < workerNum; i++ {
		go func(workerID int) {
			defer func() {
				if err := recover(); err != nil {
					errChannel <- util.GetRecoverError(err)
				}
				defer waiter.Done()
			}()

			for _, partID := range partitionsNeedSpill {
				// finalize current segment of every partition in the worker
				worker := h.hashJoinExec.BuildWorkers[workerID]
				builder := worker.builder

				startPosInRawData := builder.startPosInRawData[partID]
				if len(startPosInRawData) > 0 {
					worker.HashJoinCtx.hashTableContext.finalizeCurrentSeg(workerID, partID, worker.builder, false)
				}
				spilledSegments := worker.getSegments(partID)
				worker.clearSegments(partID)

				err := h.spillSegmentsToDisk(workerID, partID, spilledSegments)
				if err != nil {
					errChannel <- util.GetRecoverError(err)
				}
			}

		}(i)
	}

	waiter.Wait()
	close(errChannel)
	for err := range errChannel {
		return err
	}
	h.hashJoinExec.hashTableContext.memoryTracker.Consume(-totalReleasedMemory)
	return nil
}

func (h *hashJoinSpillHelper) reset() {
	for i := range h.buildRowsInDisk {
		h.buildRowsInDisk[i] = nil
		h.probeRowsInDisk[i] = nil
	}

	for i := range h.spilledPartitions {
		h.spilledPartitions[i] = false
	}

	h.spillTriggered = false
}

func (h *hashJoinSpillHelper) prepareForRestoring(lastRound int) {
	if h.buildRowsInDisk == nil {
		return
	}

	partNum := h.hashJoinExec.PartitionNumber
	concurrency := int(h.hashJoinExec.Concurrency)

	for i := 0; i < partNum; i++ {
		buildInDisks := make([]*chunk.DataInDiskByChunks, 0)
		probeInDisks := make([]*chunk.DataInDiskByChunks, 0)
		for j := 0; j < concurrency; j++ {
			if h.buildRowsInDisk[j][i] != nil {
				buildInDisks = append(buildInDisks, h.buildRowsInDisk[j][i])
				probeInDisks = append(probeInDisks, h.probeRowsInDisk[j][i])
			}
		}

		rd := &restorePartition{
			buildSideChunks: buildInDisks,
			probeSideChunks: probeInDisks,
			round:           lastRound + 1,
		}
		h.stack.push(rd)
	}

	// Reset something as spill may still be triggered during restoring
	h.reset()
}

func (h *hashJoinSpillHelper) initTmpSpillBuildSideChunks() {
	if len(h.tmpSpillBuildSideChunks) < h.hashJoinExec.PartitionNumber {
		for i := len(h.tmpSpillBuildSideChunks); i < h.hashJoinExec.PartitionNumber; i++ {
			h.tmpSpillBuildSideChunks = append(h.tmpSpillBuildSideChunks, chunk.NewChunkFromPoolWithCapacity(h.buildSpillChkFieldTypes, spillChunkSize))
		}
	}
}

func (h *hashJoinSpillHelper) initTmpSpillProbeSideChunks() {
	if len(h.tmpSpillProbeSideChunks) == 0 {
		for i := 0; i < h.hashJoinExec.PartitionNumber; i++ {
			h.tmpSpillProbeSideChunks = append(h.tmpSpillProbeSideChunks, chunk.NewChunkFromPoolWithCapacity(h.probeFieldTypes, spillChunkSize))
		}
	}
}

func (h *hashJoinSpillHelper) isSpillTriggeredInBuildStageForTest() bool {
	return h.triggeredInBuildStageForTest
}

func (h *hashJoinSpillHelper) isRespillTriggeredForTest() bool {
	return h.spillRoundForTest > 1
}

// Data in this structure are in same partition
type restorePartition struct {
	buildSideChunks []*chunk.DataInDiskByChunks
	probeSideChunks []*chunk.DataInDiskByChunks

	round int
}

type restoreStack struct {
	elems []*restorePartition
}

func (r *restoreStack) pop() *restorePartition {
	len := len(r.elems)
	if len == 0 {
		return nil
	}
	ret := r.elems[len-1]
	r.elems = r.elems[:len-1]
	return ret
}

func (r *restoreStack) push(elem *restorePartition) {
	r.elems = append(r.elems, elem)
}
