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
	"hash/fnv"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerbase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/serialization"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
)

func isKeyMatched(keyMode keyMode, serializedKey []byte, rowStart unsafe.Pointer, meta *joinTableMeta) bool {
	switch keyMode {
	case OneInt64:
		return *(*int64)(unsafe.Pointer(&serializedKey[0])) == *(*int64)(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr))
	case FixedSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr), meta.joinKeysLength))
	case VariableSerializedKey:
		return bytes.Equal(serializedKey, hack.GetBytesFromPtr(unsafe.Add(rowStart, meta.nullMapLength+sizeOfNextPtr+sizeOfElementSize), int(meta.getSerializedKeyLength(rowStart))))
	default:
		panic("unknown key match type")
	}
}

func commonInitForScanRowTable(base *baseJoinProbe) *rowIter {
	totalRowCount := base.ctx.hashTableContext.hashTable.totalRowCount()
	concurrency := base.ctx.Concurrency
	workID := uint64(base.workID)
	avgRowPerWorker := totalRowCount / uint64(concurrency)
	startIndex := workID * avgRowPerWorker
	endIndex := (workID + 1) * avgRowPerWorker
	if workID == uint64(concurrency-1) {
		endIndex = totalRowCount
	}
	if endIndex > totalRowCount {
		endIndex = totalRowCount
	}
	return base.ctx.hashTableContext.hashTable.createRowIter(startIndex, endIndex)
}

// NewJoinProbe create a join probe used for hash join v2
func NewJoinProbe(ctx *HashJoinCtxV2, workID uint, joinType plannerbase.JoinType, keyIndex []int, joinedColumnTypes, probeKeyTypes []*types.FieldType, rightAsBuildSide bool) ProbeV2 {
	base := baseJoinProbe{
		ctx:                   ctx,
		workID:                workID,
		keyIndex:              keyIndex,
		keyTypes:              probeKeyTypes,
		maxChunkSize:          ctx.SessCtx.GetSessionVars().MaxChunkSize,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
		rightAsBuildSide:      rightAsBuildSide,
		hash:                  fnv.New64(),
		rehashBuf:             make([]byte, serialization.Uint64Len),
	}

	for i := range keyIndex {
		if !mysql.HasNotNullFlag(base.keyTypes[i].GetFlag()) {
			base.hasNullableKey = true
		}
	}
	base.cachedBuildRows = make([]matchedRowInfo, batchBuildRowSize)
	base.nextCachedBuildRowIndex = 0
	base.matchedRowsHeaders = make([]taggedPtr, 0, chunk.InitialCapacity)
	base.matchedRowsHashValue = make([]uint64, 0, chunk.InitialCapacity)
	base.selRows = make([]int, 0, chunk.InitialCapacity)
	for i := range chunk.InitialCapacity {
		base.selRows = append(base.selRows, i)
	}
	base.hashValues = make([][]posAndHashValue, ctx.partitionNumber)
	for i := range int(ctx.partitionNumber) {
		base.hashValues[i] = make([]posAndHashValue, 0, chunk.InitialCapacity)
	}
	base.serializedKeys = make([][]byte, 0, chunk.InitialCapacity)
	if base.ctx.ProbeFilter != nil {
		base.filterVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.hasNullableKey {
		base.nullKeyVector = make([]bool, 0, chunk.InitialCapacity)
	}
	if base.ctx.OtherCondition != nil {
		base.tmpChk = chunk.NewChunkWithCapacity(joinedColumnTypes, chunk.InitialCapacity)
		base.tmpChk.SetInCompleteChunk(true)
		base.selected = make([]bool, 0, chunk.InitialCapacity)
		base.rowIndexInfos = make([]matchedRowInfo, 0, chunk.InitialCapacity)
	}
	switch joinType {
	case plannerbase.InnerJoin:
		return &innerJoinProbe{base}
	case plannerbase.LeftOuterJoin:
		return newOuterJoinProbe(base, !rightAsBuildSide, rightAsBuildSide)
	case plannerbase.RightOuterJoin:
		return newOuterJoinProbe(base, rightAsBuildSide, rightAsBuildSide)
	case plannerbase.SemiJoin:
		if len(base.rUsed) != 0 {
			panic("len(base.rUsed) != 0 for semi join")
		}
		return newSemiJoinProbe(base, !rightAsBuildSide)
	case plannerbase.AntiSemiJoin:
		if len(base.rUsed) != 0 {
			panic("len(base.rUsed) != 0 for anti semi join")
		}
		return newAntiSemiJoinProbe(base, !rightAsBuildSide)
	case plannerbase.LeftOuterSemiJoin:
		if len(base.rUsed) != 0 {
			panic("len(base.rUsed) != 0 for left outer semi join")
		}
		if rightAsBuildSide {
			return newLeftOuterSemiJoinProbe(base, false)
		}
		panic("unsupported join type")
	case plannerbase.AntiLeftOuterSemiJoin:
		if len(base.rUsed) != 0 {
			panic("len(base.rUsed) != 0 for left outer anti semi join")
		}
		if rightAsBuildSide {
			return newLeftOuterSemiJoinProbe(base, true)
		}
		panic("unsupported join type")
	default:
		panic("unsupported join type")
	}
}

type mockJoinProbe struct {
	baseJoinProbe
}

func (*mockJoinProbe) SetChunkForProbe(*chunk.Chunk) error {
	return errors.New("not supported")
}

func (*mockJoinProbe) SetRestoredChunkForProbe(*chunk.Chunk) error {
	return errors.New("not supported")
}

func (*mockJoinProbe) SpillRemainingProbeChunks() error {
	return errors.New("not supported")
}

func (*mockJoinProbe) Probe(*hashjoinWorkerResult, *sqlkiller.SQLKiller) (ok bool, result *hashjoinWorkerResult) {
	panic("not supported")
}

func (*mockJoinProbe) ScanRowTable(*hashjoinWorkerResult, *sqlkiller.SQLKiller) (result *hashjoinWorkerResult) {
	panic("not supported")
}

func (*mockJoinProbe) IsScanRowTableDone() bool {
	panic("not supported")
}

func (*mockJoinProbe) NeedScanRowTable() bool {
	panic("not supported")
}

func (*mockJoinProbe) InitForScanRowTable() {
	panic("not supported")
}

// used for test
func newMockJoinProbe(ctx *HashJoinCtxV2) *mockJoinProbe {
	base := baseJoinProbe{
		ctx:                   ctx,
		lUsed:                 ctx.LUsed,
		rUsed:                 ctx.RUsed,
		lUsedInOtherCondition: ctx.LUsedInOtherCondition,
		rUsedInOtherCondition: ctx.RUsedInOtherCondition,
		rightAsBuildSide:      false,
	}
	base.cachedBuildRows = make([]matchedRowInfo, batchBuildRowSize)
	base.nextCachedBuildRowIndex = 0
	return &mockJoinProbe{base}
}
