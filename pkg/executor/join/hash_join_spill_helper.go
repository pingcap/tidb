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
	"encoding/binary"
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

	buildRowsInDisk []*chunk.DataInDiskByChunks
	probeRowsInDisk []*chunk.DataInDiskByChunks

	// Avoid data race, because multi threads may use probeRowsInDisk[idx] at the same time
	probeInDiskLocks []sync.Mutex

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

	triggeredInBuildStageForTest bool
	triggeredInProbeStageForTest bool
	spillRoundForTest            int
}

func newHashJoinSpillHelper(hashJoinExec *HashJoinV2Exec, probeFieldTypes []*types.FieldType) *hashJoinSpillHelper {
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

	partition := h.stack.pop()
	for partition != nil {
		partition.buildSideChunks.Close()
		partition.probeSideChunks.Close()
		partition = h.stack.pop()
	}

	for _, inDisk := range h.discardedInDisk {
		inDisk.Close()
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

func (h *hashJoinSpillHelper) isSpillNeeded() bool {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	return h.spillStatus == needSpill
}

func (h *hashJoinSpillHelper) isSpillTriggered() bool {
	h.cond.L.Lock()
	defer h.cond.L.Unlock()
	return len(h.buildRowsInDisk) > 0
}

func (h *hashJoinSpillHelper) isPartitionSpilled(partID int) bool {
	return len(h.buildRowsInDisk) > 0 && h.buildRowsInDisk[partID] != nil
}

func (h *hashJoinSpillHelper) discardInDisks(inDisks []*chunk.DataInDiskByChunks) error {
	hasNilInDisk := false
	for _, inDisk := range inDisks {
		if inDisk == nil {
			hasNilInDisk = true
		}
		h.discardedInDisk = append(h.discardedInDisk, inDisk)
	}

	if hasNilInDisk {
		return errors.NewNoStackError("Receive nil DataInDiskByChunks")
	}
	return nil
}

func (h *hashJoinSpillHelper) rehash(hashValue uint64) uint64 {
	h.rehashBuf.Reset()
	err := binary.Write(h.rehashBuf, binary.LittleEndian, hashValue)
	if err != nil {
		panic("conversion error")
	}

	h.hash.Reset()
	h.hash.Write(h.rehashBuf.Bytes())
	return h.hash.Sum64()
}

// TODO write a specific test for this function
// TODO refine this function
func (h *hashJoinSpillHelper) choosePartitionsToSpill(isInBuildStage bool) ([]int, int64) {
	partitionNum := h.hashJoinExec.PartitionNumber
	partitionsMemoryUsage := make([]int64, partitionNum)
	for i := 0; i < partitionNum; i++ {
		if isInBuildStage {
			partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsageInBuildStage(i)
		} else {
			partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsageInProbeStage(i)
		}
	}

	spilledPartitions := h.hashJoinExec.HashJoinCtxV2.hashTableContext.getSpilledPartitions()

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

func (h *hashJoinSpillHelper) spillBuildSideOnePartition(workerID int, partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.buildRowsInDisk[partID] = inDisk

		inDisk = chunk.NewDataInDiskByChunks(h.probeFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.probeRowsInDisk[partID] = inDisk

		h.hashJoinExec.hashTableContext.setPartitionSpilled(partID)
	}

	return h.spillSegmentsToDisk(workerID, h.buildRowsInDisk[partID], segments)
}

func (h *hashJoinSpillHelper) spillSegmentsToDisk(workerID int, disk *chunk.DataInDiskByChunks, segments []*rowTableSegment) error {
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

func (h *hashJoinSpillHelper) spillProbeChk(partID int, chk *chunk.Chunk) error {
	h.probeInDiskLocks[partID].Lock()
	defer h.probeInDiskLocks[partID].Unlock()
	return h.probeRowsInDisk[partID].Add(chk)
}

func (h *hashJoinSpillHelper) init() {
	if h.buildRowsInDisk == nil {
		// It's the first time that spill is triggered
		h.initTmpSpillBuildSideChunks()

		h.probeInDiskLocks = make([]sync.Mutex, h.hashJoinExec.PartitionNumber)
		h.buildRowsInDisk = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
		h.probeRowsInDisk = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
	}
}

func (h *hashJoinSpillHelper) spillBuildRows(isInBuildStage bool) error {
	h.setInSpilling()
	defer h.cond.Broadcast()
	defer h.setNotSpilled()

	err := checkSQLKiller(&h.hashJoinExec.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuildSpill")
	if err != nil {
		return err
	}

	if isInBuildStage {
		h.triggeredInBuildStageForTest = true
	} else {
		h.triggeredInProbeStageForTest = true
	}

	h.init()

	partitionsNeedSpill, totalReleasedMemory := h.choosePartitionsToSpill(isInBuildStage)

	workerNum := len(h.hashJoinExec.BuildWorkers)
	errChannel := make(chan error, workerNum)

	waiter := &sync.WaitGroup{}
	waiter.Add(len(partitionsNeedSpill))

	logutil.BgLogger().Info(spillInfo, zap.Int64("consumed", h.bytesConsumed.Load()), zap.Int64("quota", h.bytesLimit.Load()))
	for i := 0; i < len(partitionsNeedSpill); i++ {
		go func(workerID int, partID int) {
			defer func() {
				if err := recover(); err != nil {
					errChannel <- util.GetRecoverError(err)
				}
				defer waiter.Done()
			}()

			spilledSegments := make([]*rowTableSegment, 0)

			if isInBuildStage {
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
			} else {
				spilledSegments = h.hashJoinExec.hashTableContext.getSegments(-1, partID)
				h.hashJoinExec.hashTableContext.clearSegments(-1, partID)
			}

			err := h.spillBuildSideOnePartition(workerID, partID, spilledSegments)
			if err != nil {
				errChannel <- util.GetRecoverError(err)
			}
		}(i, partitionsNeedSpill[i])
	}

	waiter.Wait()
	close(errChannel)
	for err := range errChannel {
		return err
	}
	h.hashJoinExec.hashTableContext.memoryTracker.Consume(-totalReleasedMemory)
	return nil
}

func (h *hashJoinSpillHelper) prepareForRestoring() error {
	if len(h.buildRowsInDisk) != len(h.probeRowsInDisk) {
		return errors.NewNoStackError("length of buildRowsInDisk and probeRowsInDisk are different")
	}

	for i, buildInDisk := range h.buildRowsInDisk {
		if buildInDisk == nil || h.probeRowsInDisk[i] == nil {
			if buildInDisk == nil && h.probeRowsInDisk[i] == nil {
				continue
			}
			panic("buildInDisk and probeRowsInDisk should both be nil")
		}
		rd := &restorePartition{
			buildSideChunks: buildInDisk,
			probeSideChunks: h.probeRowsInDisk[i],
			round:           1,
		}
		h.stack.push(rd)
	}
	return nil
}

// Append restored data to segment
func (h *hashJoinSpillHelper) appendSegment(seg *rowTableSegment, row chunk.Row) {
	hashValue := row.GetInt64(0)
	validJoinKey := row.GetBytes(1)
	rawData := row.GetBytes(2)

	if int(validJoinKey[0]) != 0 {
		// TODO we need to test the restore of validJoinKeyPos
		seg.validJoinKeyPos = append(seg.validJoinKeyPos, seg.getRowNum())
	}
	seg.hashValues = append(seg.hashValues, uint64(hashValue))

	rawDataLen := len(seg.rawData)
	seg.rawData = append(seg.rawData, rawData...)
	seg.rowStartOffset = append(seg.rowStartOffset, uint64(seg.rawData[rawDataLen]))
}

func (h *hashJoinSpillHelper) appendChunkToSegments(chunk *chunk.Chunk, segments []*rowTableSegment) {
	var seg *rowTableSegment
	segLen := len(segments)
	if segLen > 0 && !segments[segLen-1].finalized {
		seg = segments[segLen-1]
	} else {
		seg = newRowTableSegment()
	}

	newSegCreated := false
	defer func() {
		if newSegCreated {
			segments = append(segments, seg)
		}
	}()

	rowNum := chunk.NumRows()
	for i := 0; i < rowNum; i++ {
		if seg.getRowNum() >= int(maxRowTableSegmentSize) {
			// rowLocations's initialization has been done, so it's ok to set `finalized` to true
			seg.finalized = true
			segments = append(segments, seg)
			h.memTracker.Consume(seg.totalUsedBytes())
			seg = newRowTableSegment()
			newSegCreated = true
		}

		row := chunk.GetRow(i)
		h.appendSegment(seg, row)
	}
}

func (h *hashJoinSpillHelper) respillForBuildData(segments []*rowTableSegment, buildInDisk *chunk.DataInDiskByChunks, chunkIdx int) ([]*chunk.DataInDiskByChunks, error) {
	segNum := len(segments)
	if segNum > 0 && !segments[segNum-1].finalized {
		h.memTracker.Consume(segments[segNum-1].totalUsedBytes())
	}

	newBuildInDisk := h.createNewDiskForBuild()
	validJoinKeys := make([]byte, 0, maxRowTableSegmentSize)

	if len(h.tmpSpillBuildSideChunks) < h.hashJoinExec.PartitionNumber {
		panic("length of tmpSpillBuildSideChunks should not be smaller than partition number")
	}

	for _, chk := range h.tmpSpillBuildSideChunks {
		chk.Reset()
	}

	// Spill data in segments
	for _, seg := range segments {
		validJoinKeys = h.generateSpilledValidJoinKey(seg, validJoinKeys)
		rows := seg.getRowsBytesForSpill()

		for i, row := range rows {
			newHashValue := h.rehash(seg.hashValues[i])
			partID := newHashValue % uint64(h.hashJoinExec.PartitionNumber)

			if h.tmpSpillBuildSideChunks[partID].IsFull() {
				err := newBuildInDisk[partID].Add(h.tmpSpillBuildSideChunks[partID])
				if err != nil {
					return nil, err
				}
				h.tmpSpillBuildSideChunks[partID].Reset()
			}

			h.tmpSpillBuildSideChunks[partID].AppendInt64(0, int64(newHashValue))
			h.tmpSpillBuildSideChunks[partID].AppendBytes(1, validJoinKeys[i:i+1])
			h.tmpSpillBuildSideChunks[partID].AppendBytes(2, row)
		}
	}

	// respill build side in disk data
	chunkNum := buildInDisk.NumChunks()
	for i := chunkIdx; i < chunkNum; i++ {
		// TODO reuse chunk
		chunk, err := buildInDisk.GetChunk(i)
		if err != nil {
			return nil, err
		}

		rowNum := chunk.NumRows()
		for j := 0; j < rowNum; j++ {
			row := chunk.GetRow(j)
			newHashValue := h.rehash(uint64(row.GetInt64(0)))
			partID := newHashValue % uint64(h.hashJoinExec.PartitionNumber)

			if h.tmpSpillBuildSideChunks[partID].IsFull() {
				err := newBuildInDisk[partID].Add(h.tmpSpillBuildSideChunks[partID])
				if err != nil {
					return nil, err
				}
				h.tmpSpillBuildSideChunks[partID].Reset()
			}

			h.tmpSpillBuildSideChunks[partID].AppendInt64(0, int64(newHashValue))
			h.tmpSpillBuildSideChunks[partID].AppendBytes(1, row.GetBytes(1))
			h.tmpSpillBuildSideChunks[partID].AppendBytes(2, row.GetBytes(2))
		}
	}

	// Spill remaining rows in tmpSpillBuildSideChunks
	for i := 0; i < h.hashJoinExec.PartitionNumber; i++ {
		if h.tmpSpillBuildSideChunks[i].NumRows() > 0 {
			err := newBuildInDisk[i].Add(h.tmpSpillBuildSideChunks[i])
			if err != nil {
				return nil, err
			}
			h.tmpSpillBuildSideChunks[i].Reset()
		}
	}

	return newBuildInDisk, nil
}

func (h *hashJoinSpillHelper) respillForProbeData(probeInDisk *chunk.DataInDiskByChunks) ([]*chunk.DataInDiskByChunks, error) {
	newProbeInDisk := h.createNewDiskForProbe()
	chunkNum := probeInDisk.NumChunks()
	h.initTmpSpillProbeSideChunks()
	for i := 0; i < chunkNum; i++ {
		// TODO reuse chunk
		chunk, err := probeInDisk.GetChunk(i)
		if err != nil {
			return nil, err
		}

		rowNum := chunk.NumRows()
		for j := 0; j < rowNum; j++ {
			row := chunk.GetRow(j)
			newHashValue := h.rehash(uint64(row.GetInt64(0)))
			partID := newHashValue % uint64(h.hashJoinExec.PartitionNumber)

			if h.tmpSpillProbeSideChunks[partID].IsFull() {
				err := newProbeInDisk[partID].Add(h.tmpSpillProbeSideChunks[partID])
				if err != nil {
					return nil, err
				}
				h.tmpSpillProbeSideChunks[partID].Reset()
			}

			h.tmpSpillProbeSideChunks[partID].AppendInt64(0, int64(newHashValue))
			h.tmpSpillProbeSideChunks[partID].AppendPartialRowByColIdxs(1, row, h.probeSpilledRowIdx)
		}
	}

	// Spill remaining rows in tmpSpillProbeSideChunks
	for i := 0; i < h.hashJoinExec.PartitionNumber; i++ {
		if h.tmpSpillProbeSideChunks[i].NumRows() > 0 {
			err := newProbeInDisk[i].Add(h.tmpSpillProbeSideChunks[i])
			if err != nil {
				return nil, err
			}
			h.tmpSpillProbeSideChunks[i].Reset()
		}
	}

	return newProbeInDisk, nil
}

func (h *hashJoinSpillHelper) createNewDiskForBuild() []*chunk.DataInDiskByChunks {
	newBuildInDisk := make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
	for i := range newBuildInDisk {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		newBuildInDisk[i] = inDisk
	}
	return newBuildInDisk
}

func (h *hashJoinSpillHelper) createNewDiskForProbe() []*chunk.DataInDiskByChunks {
	newProbeInDisk := make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
	for i := range newProbeInDisk {
		inDisk := chunk.NewDataInDiskByChunks(h.probeFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		newProbeInDisk[i] = inDisk
	}
	return newProbeInDisk
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

func (h *hashJoinSpillHelper) buildHashTable(partition *restorePartition) (*hashTableV2, bool, error) {
	rowTb := &rowTable{}
	segments := make([]*rowTableSegment, 0, 10)

	spillTriggered := false
	buildInDisk := partition.buildSideChunks
	chunkNum := buildInDisk.NumChunks()
	idx := 0
	for ; idx < chunkNum; idx++ {
		// TODO maybe we can reuse a chunk
		chunk, err := buildInDisk.GetChunk(idx)
		if err != nil {
			return nil, spillTriggered, err
		}

		// TODO test re-spill case
		h.appendChunkToSegments(chunk, segments)
		if h.isSpillNeeded() {
			idx++
			break
		}
	}

	if len(segments) > 0 && !segments[len(segments)-1].finalized {
		segments[len(segments)-1].finalized = true
		h.memTracker.Consume(segments[len(segments)-1].totalUsedBytes())
	}

	rowTb.segments = segments
	subTb := newSubTable(rowTb, h.memTracker)

	if h.isSpillNeeded() {
		spillTriggered = true
		if partition.round >= h.hashJoinExec.maxSpillRound {
			return nil, spillTriggered, errors.Errorf("Exceed max hash join spill round. max round: %d", h.hashJoinExec.maxSpillRound)
		}

		newBuildInDisks, err := h.respillForBuildData(subTb.rowData.getSegments(), buildInDisk, idx)
		if err != nil {
			return nil, spillTriggered, err
		}

		newProbeInDisks, err := h.respillForProbeData(partition.probeSideChunks)
		if err != nil {
			return nil, spillTriggered, err
		}

		if len(newBuildInDisks) != len(newProbeInDisks) {
			panic("Hash join's respill occurs error")
		}

		newRound := partition.round + 1
		for i := range newBuildInDisks {
			h.stack.push(&restorePartition{
				buildSideChunks: newBuildInDisks[i],
				probeSideChunks: newProbeInDisks[i],
				round:           newRound,
			})
		}
		h.spillRoundForTest = max(h.spillRoundForTest, newRound)

		return nil, spillTriggered, nil
	} else {
		subTb.build(0, len(subTb.rowData.segments))
	}

	hashTable := &hashTableV2{}
	hashTable.tables = append(hashTable.tables, subTb)
	hashTable.partitionNumber = 1

	return hashTable, spillTriggered, nil
}

func (h *hashJoinSpillHelper) isSpillTriggeredInBuildStageForTest() bool {
	return h.triggeredInBuildStageForTest
}

func (h *hashJoinSpillHelper) isSpillTriggeredInProbeStageForTest() bool {
	return h.triggeredInProbeStageForTest
}

func (h *hashJoinSpillHelper) isSpillTriggeredBothInBuildAndProbeStageForTest() bool {
	return h.triggeredInBuildStageForTest && h.triggeredInProbeStageForTest
}

func (h *hashJoinSpillHelper) isRespillTriggeredForTest() bool {
	return h.spillRoundForTest > 1
}

// Data in this structure are in same partition
type restorePartition struct {
	buildSideChunks *chunk.DataInDiskByChunks
	probeSideChunks *chunk.DataInDiskByChunks

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
