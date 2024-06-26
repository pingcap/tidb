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
	"unsafe"

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

	probeInDiskLocks []sync.Mutex

	buildSpillChkFieldTypes []*types.FieldType
	probeFieldTypes         []*types.FieldType
	tmpSpillChunks          []*chunk.Chunk

	// TODO initialize it
	joinChkResourceCh chan *chunk.Chunk

	stack restoreStack

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64
}

func newHashJoinSpillHelper(hashJoinExec *HashJoinV2Exec, probeFieldTypes []*types.FieldType) *hashJoinSpillHelper {
	helper := &hashJoinSpillHelper{hashJoinExec: hashJoinExec}
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeLonglong))
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit))
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit))
	helper.probeFieldTypes = append(helper.probeFieldTypes, types.NewFieldType(mysql.TypeLonglong))
	helper.probeFieldTypes = append(helper.probeFieldTypes, types.NewFieldType(mysql.TypeBit))
	helper.probeFieldTypes = append(helper.probeFieldTypes, probeFieldTypes...)
	helper.memTracker = hashJoinExec.memTracker
	helper.diskTracker = hashJoinExec.diskTracker
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

	if h.tmpSpillChunks != nil {
		h.tmpSpillChunks.Destroy(spillChunkSize, h.buildSpillChkFieldTypes)
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

func (h *hashJoinSpillHelper) isPartitionSpilled(partID int) bool {
	return len(h.buildRowsInDisk) > 0 && h.buildRowsInDisk[partID] != nil
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
	rowLen := len(seg.rowLocations)
	validJoinKeys = validJoinKeys[:rowLen]
	for i := 0; i < rowLen; i++ {
		validJoinKeys[i] = byte(0)
	}
	for _, pos := range seg.validJoinKeyPos {
		validJoinKeys[pos] = byte(1)
	}
	return validJoinKeys
}

func (h *hashJoinSpillHelper) spillBuildSideOnePartition(partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.buildRowsInDisk[partID] = inDisk

		inDisk = chunk.NewDataInDiskByChunks(h.probeFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.probeRowsInDisk[partID] = inDisk

		h.hashJoinExec.hashTableContext.setPartitionSpilled(partID)
	}

	return h.spillSegmentsToDisk(h.buildRowsInDisk[partID], segments)
}

func (h *hashJoinSpillHelper) spillSegmentsToDisk(disk *chunk.DataInDiskByChunks, segments []*rowTableSegment) error {
	validJoinKeys := make([]byte, 0, maxRowTableSegmentSize)
	h.tmpSpillChunks[0].Reset()

	// Get row bytes from segment and spill them
	for _, seg := range segments {
		validJoinKeys = h.generateSpilledValidJoinKey(seg, validJoinKeys)
		rows := seg.getRowsBytes()

		for i, row := range rows {
			if h.tmpSpillChunks[0].IsFull() {
				err := disk.Add(h.tmpSpillChunks[0])
				if err != nil {
					return err
				}
				h.tmpSpillChunks[0].Reset()
			}

			h.tmpSpillChunks[0].AppendInt64(0, int64(seg.hashValues[i]))
			h.tmpSpillChunks[0].AppendBytes(1, validJoinKeys[i:i+1])
			h.tmpSpillChunks[0].AppendBytes(2, row)
		}
	}

	// Spill remaining rows in tmpSpillChunk[0]
	if h.tmpSpillChunks[0].NumRows() > 0 {
		err := disk.Add(h.tmpSpillChunks[0])
		if err != nil {
			return err
		}
		h.tmpSpillChunks[0].Reset()
	}
	return nil
}

func (h *hashJoinSpillHelper) spillProbeChk(partID int, chk *chunk.Chunk) error {
	h.probeInDiskLocks[partID].Lock()
	defer h.probeInDiskLocks[partID].Unlock()
	return h.probeRowsInDisk[partID].Add(chk)
}

func (h *hashJoinSpillHelper) initIfNeed() {
	if h.buildRowsInDisk == nil {
		// It's the first time that spill is triggered
		h.tmpSpillChunks = append(h.tmpSpillChunks, chunk.NewChunkFromPoolWithCapacity(h.buildSpillChkFieldTypes, spillChunkSize))

		partitionNum := h.hashJoinExec.PartitionNumber
		h.buildRowsInDisk = make([]*chunk.DataInDiskByChunks, partitionNum)
		h.probeRowsInDisk = make([]*chunk.DataInDiskByChunks, partitionNum)
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

	h.initIfNeed()

	partitionsNeedSpill, totalReleasedMemory := h.choosePartitionsToSpill(isInBuildStage)

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

func (h *hashJoinSpillHelper) prepareForRestoring() {
	for i, buildInDisk := range h.buildRowsInDisk {
		rd := &restorePartition{
			buildSideChunks: buildInDisk,
			probeSideChunks: h.probeRowsInDisk[i],
			round:           1,
		}
		h.stack.push(rd)
	}
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
	seg.rowLocations = append(seg.rowLocations, unsafe.Pointer(&seg.rawData[rawDataLen]))
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
		if seg.getRowNum() >= maxRowTableSegmentSize {
			// rowLocations's initialization has been done, so it's ok to set `finalized` to true
			seg.finalized = true
			segments = append(segments, seg)
			h.memTracker.Consume(seg.totalUsedBytes())
			// TODO check spill and spill if necessary
			seg = newRowTableSegment()
			newSegCreated = true
		}

		row := chunk.GetRow(i)
		h.appendSegment(seg, row)
	}
}

func (h *hashJoinSpillHelper) respillForBuildData(round int, segments []*rowTableSegment, buildInDisk *chunk.DataInDiskByChunks, chunkIdx int) ([]*chunk.DataInDiskByChunks, error) {
	if round >= h.hashJoinExec.maxSpillRound {
		return nil, errors.Errorf("Exceed max hash join spill round. max round: %d", h.hashJoinExec.maxSpillRound)
	}

	segNum := len(segments)
	if segNum > 0 && !segments[segNum-1].finalized {
		h.memTracker.Consume(segments[segNum-1].totalUsedBytes())
	}

	newBuildInDisk := make([]*chunk.DataInDiskByChunks, h.hashJoinExec.PartitionNumber)
	for i := range newBuildInDisk {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		newBuildInDisk[i] = inDisk
	}

	if len(h.tmpSpillChunks) < h.hashJoinExec.PartitionNumber {
		for i := len(h.tmpSpillChunks); i < h.hashJoinExec.PartitionNumber; i++ {
			h.tmpSpillChunks = append(h.tmpSpillChunks, chunk.NewChunkFromPoolWithCapacity(h.buildSpillChkFieldTypes, spillChunkSize))
		}
	}

	validJoinKeys := make([]byte, 0, maxRowTableSegmentSize)

	// Spill data in segments
	for _, seg := range segments {
		validJoinKeys = h.generateSpilledValidJoinKey(seg, validJoinKeys)
		rows := seg.getRowsBytes()

		for i, row := range rows {
			oldHashValue := seg.hashValues[i]
			var newHashValue int
			var partID int
			// TODO get new hash value and partition id

			if h.tmpSpillChunks[partID].IsFull() {
				err := newBuildInDisk[partID].Add(h.tmpSpillChunks[partID])
				if err != nil {
					return nil, err
				}
				h.tmpSpillChunks[partID].Reset()
			}

			h.tmpSpillChunks[partID].AppendInt64(0, int64(newHashValue))
			h.tmpSpillChunks[partID].AppendBytes(1, validJoinKeys[i:i+1])
			h.tmpSpillChunks[partID].AppendBytes(2, row)
		}
	}

	// respill data in disk
	chunkNum := buildInDisk.NumChunks()
	for i := chunkNum; i < chunkNum; i++ {
		// TODO implement it
	}

	// Spill remaining rows in tmpSpillChunks
	for i := 0; i < h.hashJoinExec.PartitionNumber; i++ {
		if h.tmpSpillChunks[i].NumRows() > 0 {
			err := newBuildInDisk[i].Add(h.tmpSpillChunks[i])
			if err != nil {
				return nil, err
			}
			h.tmpSpillChunks[i].Reset()
		}
	}

	return newBuildInDisk, nil
}

func (h *hashJoinSpillHelper) buildHashTable(partition *restorePartition) (*hashTableV2, bool, error) {
	rowTb := &rowTable{} // TODO initialize metaData in rowTable
	segments := make([]*rowTableSegment, 0, 10)

	spillTriggered := false
	buildInDisk := partition.buildSideChunks
	chunkNum := buildInDisk.NumChunks()
	for i := 0; i < chunkNum; i++ {
		// TODO maybe we can reuse a chunk
		chunk, err := buildInDisk.GetChunk(i)
		if err != nil {
			return nil, spillTriggered, err
		}

		// TODO test re-spill case
		h.appendChunkToSegments(chunk, segments)
		if h.isSpillNeeded() {

			spillTriggered = true
			return nil, spillTriggered, nil
		}
	}

	if len(segments) > 0 {
		segments[len(segments)-1].finalized = true
	}

	rowTb.segments = segments
	subTb := newSubTable(rowTb, h.memTracker)

	if h.isSpillNeeded() {
		// TODO execute spill
		spillTriggered = true
	} else {
		// If we need to spill, it's unnecessary to build the table
		subTb.build(0, len(subTb.rowData.segments))
	}

	hashTable := &hashTableV2{}
	hashTable.tables = append(hashTable.tables, subTb)
	hashTable.partitionNumber = 1

	return hashTable, spillTriggered, nil
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
