// Copyright 2021 PingCAP, Inc.
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

package executor

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func initBuildChunk(numRows int) (*chunk.Chunk, []*types.FieldType) {
	numCols := 6
	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).BuildP())

	oldChk := chunk.NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
		oldChk.AppendString(3, str)
		oldChk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		oldChk.AppendJSON(5, json.CreateBinary(str))
	}
	return oldChk, colTypes
}

func initProbeChunk(numRows int) (*chunk.Chunk, []*types.FieldType) {
	numCols := 3
	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP())
	colTypes = append(colTypes, types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP())

	oldChk := chunk.NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
	}
	return oldChk, colTypes
}

type hashCollision struct {
	count int
}

func (h *hashCollision) Sum64() uint64 {
	h.count++
	return 0
}
func (h hashCollision) Write(p []byte) (n int, err error) { return len(p), nil }
func (h hashCollision) Reset()                            {}
func (h hashCollision) Sum(b []byte) []byte               { panic("not implemented") }
func (h hashCollision) Size() int                         { panic("not implemented") }
func (h hashCollision) BlockSize() int                    { panic("not implemented") }

func TestHashRowContainer(t *testing.T) {
	hashFunc := fnv.New64
	rowContainer, copiedRC := testHashRowContainer(t, hashFunc, false)
	require.Equal(t, int64(0), rowContainer.stat.probeCollision)
	// On windows time.Now() is imprecise, the elapse time may equal 0
	require.True(t, rowContainer.stat.buildTableElapse >= 0)
	require.Equal(t, rowContainer.stat.probeCollision, copiedRC.stat.probeCollision)
	require.Equal(t, rowContainer.stat.buildTableElapse, copiedRC.stat.buildTableElapse)

	rowContainer, copiedRC = testHashRowContainer(t, hashFunc, true)
	require.Equal(t, int64(0), rowContainer.stat.probeCollision)
	require.True(t, rowContainer.stat.buildTableElapse >= 0)
	require.Equal(t, rowContainer.stat.probeCollision, copiedRC.stat.probeCollision)
	require.Equal(t, rowContainer.stat.buildTableElapse, copiedRC.stat.buildTableElapse)

	h := &hashCollision{count: 0}
	hashFuncCollision := func() hash.Hash64 {
		return h
	}
	rowContainer, copiedRC = testHashRowContainer(t, hashFuncCollision, false)
	require.True(t, h.count > 0)
	require.True(t, rowContainer.stat.probeCollision > int64(0))
	require.True(t, rowContainer.stat.buildTableElapse >= 0)
	require.Equal(t, rowContainer.stat.probeCollision, copiedRC.stat.probeCollision)
	require.Equal(t, rowContainer.stat.buildTableElapse, copiedRC.stat.buildTableElapse)
}

func testHashRowContainer(t *testing.T, hashFunc func() hash.Hash64, spill bool) (originRC, copiedRC *hashRowContainer) {
	sctx := mock.NewContext()
	var err error
	numRows := 10

	chk0, colTypes := initBuildChunk(numRows)
	chk1, _ := initBuildChunk(numRows)

	hCtx := &hashContext{
		allTypes:  colTypes[1:3],
		keyColIdx: []int{1, 2},
	}
	hCtx.hasNull = make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		hCtx.hashVals = append(hCtx.hashVals, hashFunc())
	}
	rowContainer := newHashRowContainer(sctx, 0, hCtx, colTypes)
	copiedRC = rowContainer.ShallowCopy()
	tracker := rowContainer.GetMemTracker()
	tracker.SetLabel(memory.LabelForBuildSideResult)
	if spill {
		tracker.SetBytesLimit(1)
		rowContainer.rowContainer.ActionSpillForTest().Action(tracker)
	}
	err = rowContainer.PutChunk(chk0, nil)
	require.NoError(t, err)
	err = rowContainer.PutChunk(chk1, nil)
	require.NoError(t, err)
	rowContainer.ActionSpill().(*chunk.SpillDiskAction).WaitForTest()
	require.Equal(t, spill, rowContainer.alreadySpilledSafeForTest())
	require.Equal(t, spill, rowContainer.rowContainer.GetMemTracker().BytesConsumed() == 0)
	require.Equal(t, !spill, rowContainer.rowContainer.GetMemTracker().BytesConsumed() > 0)
	require.True(t, rowContainer.GetMemTracker().BytesConsumed() > 0) // hashtable need memory
	if rowContainer.alreadySpilledSafeForTest() {
		require.NotNil(t, rowContainer.GetDiskTracker())
		require.True(t, rowContainer.GetDiskTracker().BytesConsumed() > 0)
	}

	probeChk, probeColType := initProbeChunk(2)
	probeRow := probeChk.GetRow(1)
	probeCtx := &hashContext{
		allTypes:  probeColType[1:3],
		keyColIdx: []int{1, 2},
	}
	probeCtx.hasNull = make([]bool, 1)
	probeCtx.hashVals = append(hCtx.hashVals, hashFunc())
	matched, _, err := rowContainer.GetMatchedRowsAndPtrs(hCtx.hashVals[1].Sum64(), probeRow, probeCtx)
	require.NoError(t, err)
	require.Equal(t, 2, len(matched))
	require.Equal(t, chk0.GetRow(1).GetDatumRow(colTypes), matched[0].GetDatumRow(colTypes))
	require.Equal(t, chk1.GetRow(1).GetDatumRow(colTypes), matched[1].GetDatumRow(colTypes))
	return rowContainer, copiedRC
}

func TestConcurrentMapHashTableMemoryUsage(t *testing.T) {
	m := newConcurrentMapHashTable()
	const iterations = 1024 * hack.LoadFactorNum / hack.LoadFactorDen // 6656
	wg := &sync.WaitGroup{}
	wg.Add(2)
	// Note: Now concurrentMapHashTable doesn't support inserting in parallel.
	for i := 0; i < iterations; i++ {
		// Add entry to map.
		m.Put(uint64(i*ShardCount), chunk.RowPtr{ChkIdx: uint32(i), RowIdx: uint32(i)})
	}
	mapMemoryExpected := int64(1024) * hack.DefBucketMemoryUsageForMapIntToPtr
	entryMemoryExpected := 16 * int64(64+128+256+512+1024+2048+4096)
	require.Equal(t, mapMemoryExpected+entryMemoryExpected, m.GetAndCleanMemoryDelta())
	require.Equal(t, int64(0), m.GetAndCleanMemoryDelta())
}
