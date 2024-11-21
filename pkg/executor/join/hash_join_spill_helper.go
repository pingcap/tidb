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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"go.uber.org/zap"
)

const exceedMaxSpillRoundErrInfo = "Exceed max spill round"
const memFactorAfterSpill = 0.5

type hashJoinSpillHelper struct {
	cond         *sync.Cond
	spillStatus  int
	hashJoinExec *HashJoinV2Exec

	buildRowsInDisk [][]*chunk.DataInDiskByChunks
	probeRowsInDisk [][]*chunk.DataInDiskByChunks

	buildSpillChkFieldTypes []*types.FieldType
	probeSpillFieldTypes    []*types.FieldType
	tmpSpillBuildSideChunks []*chunk.Chunk

	// When respilling a row, we need to recalculate the row's hash value.
	// These are auxiliary utility for rehash.
	hash      hash.Hash64
	rehashBuf *bytes.Buffer

	stack restoreStack

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	bytesConsumed atomic.Int64
	bytesLimit    atomic.Int64

	// The hash value in restored probe row needs to be updated before we respill this row,
	// and other columns in the row can be directly repilled.
	// This variable describes which columns can be directed respilled.
	probeSpilledRowIdx []int

	spilledPartitions []bool

	validJoinKeysBuffer [][]byte

	// This variable will be set to false before restoring
	spillTriggered bool

	canSpillFlag atomic.Bool

	spillTriggeredForTest                        bool
	spillRoundForTest                            int
	spillTriggedInBuildingStageForTest           bool
	spillTriggeredBeforeBuildingHashTableForTest bool
	allPartitionsSpilledForTest                  bool
	skipProbeInRestoreForTest                    bool
}

func newHashJoinSpillHelper(hashJoinExec *HashJoinV2Exec, partitionNum int, probeFieldTypes []*types.FieldType) *hashJoinSpillHelper {
	helper := &hashJoinSpillHelper{hashJoinExec: hashJoinExec}
	helper.cond = sync.NewCond(new(sync.Mutex))
	helper.buildSpillChkFieldTypes = make([]*types.FieldType, 0, 3)

	hashValueField := types.NewFieldType(mysql.TypeLonglong)
	hashValueField.AddFlag(mysql.UnsignedFlag)
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, hashValueField)                    // hash value
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit)) // valid join key
	helper.buildSpillChkFieldTypes = append(helper.buildSpillChkFieldTypes, types.NewFieldType(mysql.TypeBit)) // row data
	helper.probeSpillFieldTypes = getProbeSpillChunkFieldTypes(probeFieldTypes)
	helper.spilledPartitions = make([]bool, partitionNum)
	helper.hash = fnv.New64()
	helper.rehashBuf = new(bytes.Buffer)

	// hashJoinExec may be nill in ut
	if hashJoinExec != nil {
		helper.validJoinKeysBuffer = make([][]byte, hashJoinExec.Concurrency)
	}

	helper.probeSpilledRowIdx = make([]int, 0, len(helper.probeSpillFieldTypes)-1)
	for i := 1; i < len(helper.probeSpillFieldTypes); i++ {
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
}

func (h *hashJoinSpillHelper) areAllPartitionsSpilled() bool {
	for _, spilled := range h.spilledPartitions {
		if !spilled {
			return false
		}
	}
	return true
}

// After merging row tables, hash join can not spill any more.
// Set flag so that we can trigger other executor's spill when
// hash join can not spill.
func (h *hashJoinSpillHelper) setCanSpillFlag(canSpill bool) {
	h.canSpillFlag.Store(canSpill)
}

func (h *hashJoinSpillHelper) canSpill() bool {
	return h.canSpillFlag.Load()
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

func (h *hashJoinSpillHelper) isSpillTriggered() bool {
	return h.spillTriggered
}

func (h *hashJoinSpillHelper) isPartitionSpilled(partID int) bool {
	return h.spilledPartitions[partID]
}

func (h *hashJoinSpillHelper) choosePartitionsToSpill(hashTableMemUsage []int64) ([]int, int64) {
	partitionNum := h.hashJoinExec.partitionNumber
	partitionsMemoryUsage := make([]int64, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		partitionsMemoryUsage[i] = h.hashJoinExec.hashTableContext.getPartitionMemoryUsage(i)
		if hashTableMemUsage != nil {
			partitionsMemoryUsage[i] += hashTableMemUsage[i]
		}
	}

	spilledPartitions := h.getSpilledPartitions()

	releasedMemoryUsage := int64(0)
	for _, partID := range spilledPartitions {
		releasedMemoryUsage += partitionsMemoryUsage[partID]
	}

	// We need to get the latest memory consumption
	bytesConsumed := h.memTracker.BytesConsumed()

	bytesLimit := h.bytesLimit.Load()
	bytesConsumedAfterReleased := bytesConsumed - releasedMemoryUsage

	// Check if it's enough to spill existing spilled partitions
	if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*memFactorAfterSpill {
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
	sort.SliceStable(unspilledPartitionsAndMemory, func(i, j int) bool {
		return unspilledPartitionsAndMemory[i].memoryUsage > unspilledPartitionsAndMemory[j].memoryUsage
	})

	// Choose more partitions to spill
	for _, item := range unspilledPartitionsAndMemory {
		spilledPartitions = append(spilledPartitions, item.partID)
		releasedMemoryUsage += item.memoryUsage
		bytesConsumedAfterReleased -= item.memoryUsage
		if float64(bytesConsumedAfterReleased) <= float64(bytesLimit)*memFactorAfterSpill {
			return spilledPartitions, releasedMemoryUsage
		}
	}

	return spilledPartitions, releasedMemoryUsage
}

func (*hashJoinSpillHelper) generateSpilledValidJoinKey(seg *rowTableSegment, validJoinKeys []byte) []byte {
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

func (h *hashJoinSpillHelper) spillBuildSegmentToDisk(workerID int, partID int, segments []*rowTableSegment) error {
	if h.buildRowsInDisk[workerID] == nil {
		h.buildRowsInDisk[workerID] = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.partitionNumber)
		h.probeRowsInDisk[workerID] = make([]*chunk.DataInDiskByChunks, h.hashJoinExec.partitionNumber)
	}

	if h.buildRowsInDisk[workerID][partID] == nil {
		inDisk := chunk.NewDataInDiskByChunks(h.buildSpillChkFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.buildRowsInDisk[workerID][partID] = inDisk

		inDisk = chunk.NewDataInDiskByChunks(h.probeSpillFieldTypes)
		inDisk.GetDiskTracker().AttachTo(h.diskTracker)
		h.probeRowsInDisk[workerID][partID] = inDisk
	}

	return h.spillSegmentsToDiskImpl(workerID, h.buildRowsInDisk[workerID][partID], segments)
}

func (h *hashJoinSpillHelper) spillSegmentsToDiskImpl(workerID int, disk *chunk.DataInDiskByChunks, segments []*rowTableSegment) error {
	if cap(h.validJoinKeysBuffer[workerID]) == 0 {
		h.validJoinKeysBuffer[workerID] = make([]byte, 0, maxRowTableSegmentSize)
	}
	h.validJoinKeysBuffer[workerID] = h.validJoinKeysBuffer[workerID][:0]
	h.tmpSpillBuildSideChunks[workerID].Reset()

	// Get row bytes from segment and spill them
	for _, seg := range segments {
		h.validJoinKeysBuffer[workerID] = h.generateSpilledValidJoinKey(seg, h.validJoinKeysBuffer[workerID])

		rowNum := seg.getRowNum()
		for i := 0; i < rowNum; i++ {
			row := seg.getRowBytes(i)
			if h.tmpSpillBuildSideChunks[workerID].IsFull() {
				err := disk.Add(h.tmpSpillBuildSideChunks[workerID])
				if err != nil {
					return err
				}
				h.tmpSpillBuildSideChunks[workerID].Reset()

				err = triggerIntest(2)
				if err != nil {
					return err
				}
			}

			h.tmpSpillBuildSideChunks[workerID].AppendUint64(0, seg.hashValues[i])
			h.tmpSpillBuildSideChunks[workerID].AppendBytes(1, h.validJoinKeysBuffer[workerID][i:i+1])
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

		for _, worker := range h.hashJoinExec.BuildWorkers {
			if worker.restoredChkBuf == nil {
				worker.restoredChkBuf = chunk.NewEmptyChunk(h.buildSpillChkFieldTypes)
			}
		}

		for _, worker := range h.hashJoinExec.ProbeWorkers {
			if worker.restoredChkBuf == nil {
				worker.restoredChkBuf = chunk.NewEmptyChunk(h.probeSpillFieldTypes)
			}
		}
	}
}

func (h *hashJoinSpillHelper) spillRowTableImpl(partitionsNeedSpill []int, totalReleasedMemory int64) error {
	workerNum := len(h.hashJoinExec.BuildWorkers)
	errChannel := make(chan error, workerNum)

	wg := &util.WaitGroupWrapper{}

	h.setPartitionSpilled(partitionsNeedSpill)

	if intest.InTest {
		if len(partitionsNeedSpill) == int(h.hashJoinExec.partitionNumber) {
			h.allPartitionsSpilledForTest = true
		}
		h.spillTriggeredForTest = true
	}

	logutil.BgLogger().Info(spillInfo, zap.Int64("consumed", h.bytesConsumed.Load()), zap.Int64("quota", h.bytesLimit.Load()))
	for i := 0; i < workerNum; i++ {
		workerID := i
		wg.RunWithRecover(
			func() {
				for _, partID := range partitionsNeedSpill {
					// finalize current segment of every partition in the worker
					worker := h.hashJoinExec.BuildWorkers[workerID]
					builder := worker.builder

					if builder.rowNumberInCurrentRowTableSeg[partID] > 0 {
						worker.HashJoinCtx.hashTableContext.finalizeCurrentSeg(workerID, partID, worker.builder, false)
					}
					spilledSegments := worker.getSegmentsInRowTable(partID)
					worker.clearSegmentsInRowTable(partID)

					err := h.spillBuildSegmentToDisk(workerID, partID, spilledSegments)
					if err != nil {
						errChannel <- util.GetRecoverError(err)
					}
				}
			},
			func(r any) {
				if r != nil {
					errChannel <- util.GetRecoverError(r)
				}
			},
		)
	}

	wg.Wait()
	close(errChannel)
	for err := range errChannel {
		return err
	}
	h.hashJoinExec.hashTableContext.memoryTracker.Consume(-totalReleasedMemory)

	err := triggerIntest(10)
	if err != nil {
		return err
	}

	return nil
}

func (h *hashJoinSpillHelper) spillRemainingRows() error {
	h.setInSpilling()
	defer h.cond.Broadcast()
	defer h.setNotSpilled()

	err := checkSQLKiller(&h.hashJoinExec.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuildSpill")
	if err != nil {
		return err
	}

	h.init()
	spilledPartitions := h.getSpilledPartitions()
	totalReleasedMemoryUsage := int64(0)
	for _, partID := range spilledPartitions {
		totalReleasedMemoryUsage += h.hashJoinExec.hashTableContext.getPartitionMemoryUsage(partID)
	}

	h.bytesConsumed.Store(h.memTracker.BytesConsumed())
	return h.spillRowTableImpl(spilledPartitions, totalReleasedMemoryUsage)
}

func (h *hashJoinSpillHelper) spillRowTable(hashTableMemUsage []int64) error {
	h.setInSpilling()
	defer h.cond.Broadcast()
	defer h.setNotSpilled()

	err := checkSQLKiller(&h.hashJoinExec.HashJoinCtxV2.SessCtx.GetSessionVars().SQLKiller, "killedDuringBuildSpill")
	if err != nil {
		return err
	}

	h.init()

	partitionsNeedSpill, totalReleasedMemory := h.choosePartitionsToSpill(hashTableMemUsage)
	return h.spillRowTableImpl(partitionsNeedSpill, totalReleasedMemory)
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

func (h *hashJoinSpillHelper) prepareForRestoring(lastRound int) error {
	err := triggerIntest(10)
	if err != nil {
		return err
	}

	if lastRound+1 > h.hashJoinExec.maxSpillRound {
		return errors.NewNoStackError(exceedMaxSpillRoundErrInfo)
	}

	if h.buildRowsInDisk == nil {
		return nil
	}

	partNum := h.hashJoinExec.partitionNumber
	concurrency := int(h.hashJoinExec.Concurrency)

	for i := 0; i < int(partNum); i++ {
		if h.spilledPartitions[i] {
			buildInDisks := make([]*chunk.DataInDiskByChunks, 0)
			probeInDisks := make([]*chunk.DataInDiskByChunks, 0)
			for j := 0; j < concurrency; j++ {
				if h.buildRowsInDisk[j] != nil && h.buildRowsInDisk[j][i] != nil {
					buildInDisks = append(buildInDisks, h.buildRowsInDisk[j][i])
					probeInDisks = append(probeInDisks, h.probeRowsInDisk[j][i])
				}
			}

			if len(buildInDisks) == 0 {
				continue
			}

			rd := &restorePartition{
				buildSideChunks: buildInDisks,
				probeSideChunks: probeInDisks,
				round:           lastRound + 1,
			}
			h.stack.push(rd)
		}
	}

	// Reset something as spill may still be triggered during restoring
	h.reset()
	return nil
}

func (h *hashJoinSpillHelper) initTmpSpillBuildSideChunks() {
	if len(h.tmpSpillBuildSideChunks) < int(h.hashJoinExec.Concurrency) {
		for i := len(h.tmpSpillBuildSideChunks); i < int(h.hashJoinExec.Concurrency); i++ {
			h.tmpSpillBuildSideChunks = append(h.tmpSpillBuildSideChunks, chunk.NewChunkWithCapacity(h.buildSpillChkFieldTypes, spillChunkSize))
		}
	}
}

func (h *hashJoinSpillHelper) isProbeSkippedInRestoreForTest() bool {
	return h.skipProbeInRestoreForTest
}

func (h *hashJoinSpillHelper) isRespillTriggeredForTest() bool {
	return h.spillRoundForTest > 1
}

func (h *hashJoinSpillHelper) isSpillTriggeredForTest() bool {
	return h.spillTriggeredForTest
}

func (h *hashJoinSpillHelper) isSpillTriggedInBuildingStageForTest() bool {
	return h.spillTriggedInBuildingStageForTest
}

func (h *hashJoinSpillHelper) areAllPartitionsSpilledForTest() bool {
	return h.allPartitionsSpilledForTest
}

func (h *hashJoinSpillHelper) isSpillTriggeredBeforeBuildingHashTableForTest() bool {
	return h.spillTriggeredBeforeBuildingHashTableForTest
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
	length := len(r.elems)
	if length == 0 {
		return nil
	}
	ret := r.elems[length-1]
	r.elems = r.elems[:length-1]
	return ret
}

func (r *restoreStack) push(elem *restorePartition) {
	r.elems = append(r.elems, elem)
}
