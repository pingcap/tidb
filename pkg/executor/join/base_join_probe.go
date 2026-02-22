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
	"fmt"
	"hash"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

type keyMode int

const (
	// OneInt64 mean the key contains only one Int64
	OneInt64 keyMode = iota
	// FixedSerializedKey mean the key has fixed length
	FixedSerializedKey
	// VariableSerializedKey mean the key has variable length
	VariableSerializedKey
)

const batchBuildRowSize = 32

func (hCtx *HashJoinCtxV2) hasOtherCondition() bool {
	return hCtx.OtherCondition != nil
}

// ProbeV2 is the interface used to do probe in hash join v2
type ProbeV2 interface {
	// SetChunkForProbe will do some pre-work when start probing a chunk
	SetChunkForProbe(chunk *chunk.Chunk) error
	// SetRestoredChunkForProbe will do some pre-work for a chunk resoted from disk
	SetRestoredChunkForProbe(chunk *chunk.Chunk) error
	// Probe is to probe current chunk, the result chunk is set in result.chk, and Probe need to make sure result.chk.NumRows() <= result.chk.RequiredRows()
	Probe(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (ok bool, result *hashjoinWorkerResult)
	// IsCurrentChunkProbeDone returns true if current probe chunk is all probed
	IsCurrentChunkProbeDone() bool
	// SpillRemainingProbeChunks spills remaining probe chunks
	SpillRemainingProbeChunks() error
	// ScanRowTable is called after all the probe chunks are probed. It is used in some special joins, like left outer join with left side to build, after all
	// the probe side chunks are handled, it needs to scan the row table to return the un-matched rows
	ScanRowTable(joinResult *hashjoinWorkerResult, sqlKiller *sqlkiller.SQLKiller) (result *hashjoinWorkerResult)
	// IsScanRowTableDone returns true after scan row table is done
	IsScanRowTableDone() bool
	// NeedScanRowTable returns true if current join need to scan row table after all the probe side chunks are handled
	NeedScanRowTable() bool
	// InitForScanRowTable do some pre-work before ScanRowTable, it must be called before ScanRowTable
	InitForScanRowTable()
	// Return probe collsion
	GetProbeCollision() uint64
	// Reset probe collsion
	ResetProbeCollision()
	// Reset some probe variables
	ResetProbe()
}

type offsetAndLength struct {
	offset int
	length int
}

type matchedRowInfo struct {
	// probeRowIndex mean the probe side index of current matched row
	probeRowIndex int
	// buildRowStart mean the build row start of the current matched row
	buildRowStart uintptr
	// buildRowOffset mean the current offset of current BuildRow, used to construct column data from BuildRow
	buildRowOffset int
}

type posAndHashValue struct {
	hashValue uint64
	pos       int
}

type baseJoinProbe struct {
	ctx    *HashJoinCtxV2
	workID uint

	currentChunk *chunk.Chunk
	// if currentChunk.Sel() == nil, then construct a fake selRows
	selRows  []int
	usedRows []int
	// matchedRowsHeaders, serializedKeys is indexed by logical row index
	matchedRowsHeaders   []taggedPtr // the start address of each matched rows
	matchedRowsHashValue []uint64    // the hash value of each matched rows
	serializedKeys       [][]byte    // used for save serialized keys
	// filterVector and nullKeyVector is indexed by physical row index because the return vector of VectorizedFilter is based on physical row index
	filterVector                  []bool              // if there is filter before probe, filterVector saves the filter result
	nullKeyVector                 []bool              // nullKeyVector[i] = true if any of the key is null
	hashValues                    [][]posAndHashValue // the start address of each matched rows
	currentProbeRow               int
	matchedRowsForCurrentProbeRow int
	chunkRows                     int
	cachedBuildRows               []matchedRowInfo
	nextCachedBuildRowIndex       int

	keyIndex         []int
	keyTypes         []*types.FieldType
	hasNullableKey   bool
	maxChunkSize     int
	rightAsBuildSide bool
	// lUsed/rUsed show which columns are used by father for left child and right child.
	// NOTE:
	// 1. lUsed/rUsed should never be nil.
	// 2. no columns are used if lUsed/rUsed is not nil but the size of lUsed/rUsed is 0.
	lUsed, rUsed                                 []int
	lUsedInOtherCondition, rUsedInOtherCondition []int
	// used when construct column from probe side
	offsetAndLengthArray []offsetAndLength
	// these 3 variables are used for join that has other condition, should be inited when the join has other condition
	tmpChk        *chunk.Chunk
	rowIndexInfos []matchedRowInfo
	selected      []bool

	// This marks which columns are probe columns, and it is used only in spill
	usedColIdx  []int
	spillTmpChk []*chunk.Chunk

	hash      hash.Hash64
	rehashBuf []byte

	spilledIdx []int

	probeCollision uint64

	serializedKeysLens   []int
	serializedKeysBuffer []byte
}

func (j *baseJoinProbe) GetProbeCollision() uint64 {
	return j.probeCollision
}

func (j *baseJoinProbe) ResetProbeCollision() {
	j.probeCollision = 0
}

func (j *baseJoinProbe) IsCurrentChunkProbeDone() bool {
	return j.currentChunk == nil || j.currentProbeRow >= j.chunkRows
}

func (j *baseJoinProbe) finishCurrentLookupLoop(joinedChk *chunk.Chunk) {
	if j.nextCachedBuildRowIndex > 0 {
		j.batchConstructBuildRows(joinedChk, 0, j.ctx.hasOtherCondition())
	}
	j.finishLookupCurrentProbeRow()
	j.appendProbeRowToChunk(joinedChk, j.currentChunk)
}

func (j *baseJoinProbe) SetChunkForProbe(chk *chunk.Chunk) (err error) {
	defer func() {
		if j.ctx.spillHelper.areAllPartitionsSpilled() {
			// We will not call `Probe` function when all partitions are spilled.
			// So it's necessary to manually set `currentProbeRow` to avoid check fail.
			j.currentProbeRow = j.chunkRows
		}
	}()

	if j.currentChunk != nil {
		if j.currentProbeRow < j.chunkRows {
			return errors.New("Previous chunk is not probed yet")
		}
	}

	j.currentChunk = chk
	logicalRows := chk.NumRows()
	// if chk.sel != nil, then physicalRows is different from logicalRows
	physicalRows := chk.Column(0).Rows()
	j.usedRows = chk.Sel()
	if j.usedRows == nil {
		if logicalRows <= fakeSelLength {
			j.selRows = fakeSel[:logicalRows]
		} else {
			j.selRows = make([]int, logicalRows)
			for i := range logicalRows {
				j.selRows[i] = i
			}
		}
		j.usedRows = j.selRows
	}
	j.chunkRows = logicalRows
	if cap(j.matchedRowsHeaders) >= logicalRows {
		j.matchedRowsHeaders = j.matchedRowsHeaders[:logicalRows]
	} else {
		j.matchedRowsHeaders = make([]taggedPtr, logicalRows)
	}
	if cap(j.matchedRowsHashValue) >= logicalRows {
		j.matchedRowsHashValue = j.matchedRowsHashValue[:logicalRows]
	} else {
		j.matchedRowsHashValue = make([]uint64, logicalRows)
	}
	for i := range int(j.ctx.partitionNumber) {
		j.hashValues[i] = j.hashValues[i][:0]
	}
	if j.ctx.ProbeFilter != nil {
		if cap(j.filterVector) >= physicalRows {
			j.filterVector = j.filterVector[:physicalRows]
		} else {
			j.filterVector = make([]bool, physicalRows)
		}
	}
	if j.hasNullableKey {
		if cap(j.nullKeyVector) >= physicalRows {
			j.nullKeyVector = j.nullKeyVector[:physicalRows]
		} else {
			j.nullKeyVector = make([]bool, physicalRows)
		}
		for i := range physicalRows {
			j.nullKeyVector[i] = false
		}
	}
	if cap(j.serializedKeys) >= logicalRows {
		clear(j.serializedKeys)
		j.serializedKeys = j.serializedKeys[:logicalRows]
	} else {
		j.serializedKeys = make([][]byte, logicalRows)
	}

	if cap(j.serializedKeysLens) < logicalRows {
		j.serializedKeysLens = make([]int, logicalRows)
	} else {
		clear(j.serializedKeysLens)
		j.serializedKeysLens = j.serializedKeysLens[:logicalRows]
	}

	if j.ctx.ProbeFilter != nil {
		j.filterVector, err = expression.VectorizedFilter(j.ctx.SessCtx.GetExprCtx().GetEvalCtx(), j.ctx.SessCtx.GetSessionVars().EnableVectorizedExpression, j.ctx.ProbeFilter, chunk.NewIterator4Chunk(j.currentChunk), j.filterVector)
		if err != nil {
			return err
		}
	}

	// generate serialized key
	j.serializedKeysBuffer, err = codec.SerializeKeys(
		j.ctx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(),
		j.currentChunk,
		j.keyTypes,
		j.keyIndex,
		j.usedRows,
		j.filterVector,
		j.nullKeyVector,
		j.ctx.hashTableMeta.serializeModes,
		j.serializedKeys,
		j.serializedKeysLens,
		j.serializedKeysBuffer)
	if err != nil {
		return err
	}

	// Not all sqls need spill, so we initialize it at runtime, or there will be too many unnecessary memory allocations
	// spillTriggered can only be set in build stage, so it's ok to get it without lock
	if j.ctx.spillHelper.isSpillTriggered() && len(j.spillTmpChk) == 0 {
		for range int(j.ctx.partitionNumber) {
			j.spillTmpChk = append(j.spillTmpChk, chunk.NewChunkWithCapacity(j.ctx.spillHelper.probeSpillFieldTypes, spillChunkSize))
		}
	}

	j.spilledIdx = j.spilledIdx[:0]

	for logicalRowIndex, physicalRowIndex := range j.usedRows {
		if (j.filterVector != nil && !j.filterVector[physicalRowIndex]) || (j.nullKeyVector != nil && j.nullKeyVector[physicalRowIndex]) {
			// explicit set the matchedRowsHeaders[logicalRowIndex] to nil to indicate there is no matched rows
			j.matchedRowsHeaders[logicalRowIndex] = 0
			j.matchedRowsHashValue[logicalRowIndex] = 0
			continue
		}

		j.hash.Reset()

		// As the golang doc described, `Hash.Write` never returns an error.
		// See https://golang.org/pkg/hash/#Hash
		_, _ = j.hash.Write(j.serializedKeys[logicalRowIndex])
		hashValue := j.hash.Sum64()
		j.matchedRowsHashValue[logicalRowIndex] = hashValue
		partIndex := generatePartitionIndex(hashValue, j.ctx.partitionMaskOffset)
		if j.ctx.spillHelper.isPartitionSpilled(int(partIndex)) {
			j.spillTmpChk[partIndex].AppendUint64(0, hashValue)
			j.spillTmpChk[partIndex].AppendBytes(1, j.serializedKeys[logicalRowIndex])
			j.spillTmpChk[partIndex].AppendPartialRow(2, j.currentChunk.GetRow(logicalRowIndex))

			j.spilledIdx = append(j.spilledIdx, logicalRowIndex)

			if j.spillTmpChk[partIndex].IsFull() {
				err := j.ctx.spillHelper.spillProbeChk(int(j.workID), int(partIndex), j.spillTmpChk[partIndex])
				if err != nil {
					return err
				}
				j.spillTmpChk[partIndex].Reset()
			}

			j.matchedRowsHeaders[logicalRowIndex] = 0
		} else {
			j.hashValues[partIndex] = append(j.hashValues[partIndex], posAndHashValue{hashValue: hashValue, pos: logicalRowIndex})
		}
	}

	j.currentProbeRow = 0
	for i := range int(j.ctx.partitionNumber) {
		for index := range j.hashValues[i] {
			j.matchedRowsHeaders[j.hashValues[i][index].pos] = j.ctx.hashTableContext.lookup(i, j.hashValues[i][index].hashValue)
		}
	}
	return
}

func (j *baseJoinProbe) preAllocForSetRestoredChunkForProbe(logicalRowCount int, hashValueCol *chunk.Column, serializedKeysCol *chunk.Column) {
	if cap(j.matchedRowsHeaders) >= logicalRowCount {
		j.matchedRowsHeaders = j.matchedRowsHeaders[:logicalRowCount]
	} else {
		j.matchedRowsHeaders = make([]taggedPtr, logicalRowCount)
	}

	if cap(j.matchedRowsHashValue) >= logicalRowCount {
		j.matchedRowsHashValue = j.matchedRowsHashValue[:logicalRowCount]
	} else {
		j.matchedRowsHashValue = make([]uint64, logicalRowCount)
	}

	for i := range int(j.ctx.partitionNumber) {
		j.hashValues[i] = j.hashValues[i][:0]
	}

	if cap(j.serializedKeysLens) < logicalRowCount {
		j.serializedKeysLens = make([]int, logicalRowCount)
	} else {
		clear(j.serializedKeysLens)
		j.serializedKeysLens = j.serializedKeysLens[:logicalRowCount]
	}

	if cap(j.serializedKeys) >= logicalRowCount {
		clear(j.serializedKeys)
		j.serializedKeys = j.serializedKeys[:logicalRowCount]
	} else {
		j.serializedKeys = make([][]byte, logicalRowCount)
	}

	j.spilledIdx = j.spilledIdx[:0]

	totalMemUsage := 0
	for _, idx := range j.usedRows {
		oldHashValue := hashValueCol.GetUint64(idx)
		newHashVal := rehash(oldHashValue, j.rehashBuf, j.hash)
		j.matchedRowsHashValue[idx] = newHashVal
		partIndex := generatePartitionIndex(newHashVal, j.ctx.partitionMaskOffset)
		if !j.ctx.spillHelper.isPartitionSpilled(int(partIndex)) {
			keyLen := serializedKeysCol.GetRawLength(idx)
			j.serializedKeysLens[idx] = keyLen
			totalMemUsage += keyLen
		}
	}

	if cap(j.serializedKeysBuffer) < totalMemUsage {
		j.serializedKeysBuffer = make([]byte, totalMemUsage)
	} else {
		j.serializedKeysBuffer = j.serializedKeysBuffer[:totalMemUsage]
	}

	start := 0
	for _, idx := range j.usedRows {
		keyLen := j.serializedKeysLens[idx]
		j.serializedKeys[idx] = j.serializedKeysBuffer[start : start : start+keyLen]
		start += keyLen
	}
}

func (j *baseJoinProbe) SetRestoredChunkForProbe(chk *chunk.Chunk) error {
	defer func() {
		if j.ctx.spillHelper.areAllPartitionsSpilled() {
			// We will not call `Probe` function when all partitions are spilled.
			// So it's necessary to manually set `currentProbeRow` to avoid check fail.
			j.currentProbeRow = j.chunkRows
		}
	}()

	if j.currentChunk != nil {
		if j.currentProbeRow < j.chunkRows {
			return errors.New("Previous chunk is not probed yet")
		}
	}

	hashValueCol := chk.Column(0)
	serializedKeysCol := chk.Column(1)
	colNum := chk.NumCols()
	if j.usedColIdx == nil {
		j.usedColIdx = make([]int, 0, colNum-2)
		for i := range colNum - 2 {
			j.usedColIdx = append(j.usedColIdx, i+2)
		}
	}
	j.currentChunk = chk.Prune(j.usedColIdx)
	logicalRows := chk.NumRows()
	j.chunkRows = logicalRows

	if cap(j.selRows) >= logicalRows {
		j.selRows = j.selRows[:logicalRows]
	} else {
		j.selRows = make([]int, 0, logicalRows)
		for i := range logicalRows {
			j.selRows = append(j.selRows, i)
		}
	}

	if chk.Sel() != nil {
		panic("chk.Sel() != nil")
	}

	j.usedRows = j.selRows

	j.preAllocForSetRestoredChunkForProbe(logicalRows, hashValueCol, serializedKeysCol)

	var serializedKeyVectorBufferCapsForTest []int
	if intest.InTest {
		serializedKeyVectorBufferCapsForTest = make([]int, len(j.serializedKeys))
		for i := range j.serializedKeys {
			serializedKeyVectorBufferCapsForTest[i] = cap(j.serializedKeys[i])
		}
	}

	// rehash all rows
	for _, idx := range j.usedRows {
		newHashVal := j.matchedRowsHashValue[idx]
		partIndex := generatePartitionIndex(newHashVal, j.ctx.partitionMaskOffset)
		serializedKeysBytes := serializedKeysCol.GetBytes(idx)
		if j.ctx.spillHelper.isPartitionSpilled(int(partIndex)) {
			j.spillTmpChk[partIndex].AppendUint64(0, newHashVal)
			j.spillTmpChk[partIndex].AppendBytes(1, serializedKeysBytes)
			j.spillTmpChk[partIndex].AppendPartialRow(2, j.currentChunk.GetRow(idx))

			j.spilledIdx = append(j.spilledIdx, idx)

			if j.spillTmpChk[partIndex].IsFull() {
				err := j.ctx.spillHelper.spillProbeChk(int(j.workID), int(partIndex), j.spillTmpChk[partIndex])
				if err != nil {
					return err
				}
				j.spillTmpChk[partIndex].Reset()
			}

			j.matchedRowsHeaders[idx] = 0
		} else {
			j.hashValues[partIndex] = append(j.hashValues[partIndex], posAndHashValue{hashValue: newHashVal, pos: idx})
			j.serializedKeys[idx] = append(j.serializedKeys[idx], serializedKeysBytes...)
			j.matchedRowsHeaders[idx] = j.ctx.hashTableContext.lookup(int(partIndex), newHashVal)
		}
	}

	if intest.InTest {
		for i := range j.serializedKeys {
			if serializedKeyVectorBufferCapsForTest[i] < cap(j.serializedKeys[i]) {
				panic(fmt.Sprintf("Before: %d, After: %d", serializedKeyVectorBufferCapsForTest[i], cap(j.serializedKeys[i])))
			}
		}
	}

	j.currentProbeRow = 0
	return nil
}

func (j *baseJoinProbe) SpillRemainingProbeChunks() error {
	if j.spillTmpChk == nil {
		return nil
	}

	for i := range int(j.ctx.partitionNumber) {
		if j.spillTmpChk[i].NumRows() > 0 {
			err := j.ctx.spillHelper.spillProbeChk(int(j.workID), i, j.spillTmpChk[i])
			if err != nil {
				return err
			}
			j.spillTmpChk[i].Reset()
		}
	}
	return nil
}

func (j *baseJoinProbe) finishLookupCurrentProbeRow() {
	if j.matchedRowsForCurrentProbeRow > 0 {
		j.offsetAndLengthArray = append(j.offsetAndLengthArray, offsetAndLength{offset: j.usedRows[j.currentProbeRow], length: j.matchedRowsForCurrentProbeRow})
	}
	j.matchedRowsForCurrentProbeRow = 0
}

func (j *baseJoinProbe) ResetProbe() {
	// We must reset `cachedBuildRows` or gc will raise error.
	// However, we can't explain it so far.
	j.cachedBuildRows = make([]matchedRowInfo, batchBuildRowSize)

	// Reset `rowIndexInfos`, just in case of gc problems.
	if j.ctx.hasOtherCondition() {
		j.rowIndexInfos = make([]matchedRowInfo, 0, chunk.InitialCapacity)
	}
}

func checkSQLKiller(killer *sqlkiller.SQLKiller, fpName string) error {
	err := killer.HandleSignal()
	failpoint.Inject(fpName, func(val failpoint.Value) {
		if val.(bool) {
			err = exeerrors.ErrQueryInterrupted
		}
	})
	return err
}

