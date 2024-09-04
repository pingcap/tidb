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
	"math/rand"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func createMockRowTable(maxRowsPerSeg int, segmentCount int, fixedSize bool) *rowTable {
	ret := &rowTable{
		meta: nil,
	}
	for i := 0; i < segmentCount; i++ {
		// no empty segment is allowed
		rows := maxRowsPerSeg
		if !fixedSize {
			rows = int(rand.Int31n(int32(maxRowsPerSeg)) + 1)
		}
		rowSeg := newRowTableSegment(uint(rows))
		rowSeg.rawData = make([]byte, rows)
		for j := 0; j < rows; j++ {
			rowSeg.rowStartOffset = append(rowSeg.rowStartOffset, uint64(j))
			rowSeg.validJoinKeyPos = append(rowSeg.validJoinKeyPos, j)
		}
		ret.segments = append(ret.segments, rowSeg)
	}
	return ret
}

func createRowTable(rows int) (*rowTable, uint8, error) {
	tinyTp := types.NewFieldType(mysql.TypeTiny)
	tinyTp.AddFlag(mysql.NotNullFlag)
	buildKeyIndex := []int{0}
	buildTypes := []*types.FieldType{tinyTp}
	buildKeyTypes := []*types.FieldType{tinyTp}
	probeKeyTypes := []*types.FieldType{tinyTp}
	buildSchema := &expression.Schema{}
	for _, tp := range buildTypes {
		buildSchema.Append(&expression.Column{
			RetType: tp,
		})
	}

	meta := newTableMeta(buildKeyIndex, buildTypes, buildKeyTypes, probeKeyTypes, nil, []int{}, false)
	hasNullableKey := false
	for _, buildKeyType := range buildKeyTypes {
		if !mysql.HasNotNullFlag(buildKeyType.GetFlag()) {
			hasNullableKey = true
			break
		}
	}

	chk := testutil.GenRandomChunks(buildTypes, rows)
	hashJoinCtx := &HashJoinCtxV2{
		hashTableMeta: meta,
	}
	hashJoinCtx.Concurrency = 1
	hashJoinCtx.SetupPartitionInfo()
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, hashJoinCtx.partitionNumber, hasNullableKey, false, false)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	if err != nil {
		return nil, 0, err
	}
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	taggedBits := uint8(maxTaggedBits)
	for _, seg := range hashJoinCtx.hashTableContext.rowTables[0][0].segments {
		taggedBits = min(taggedBits, seg.taggedBits)
	}
	return hashJoinCtx.hashTableContext.rowTables[0][0], taggedBits, nil
}

func TestHashTableSize(t *testing.T) {
	rowTable := createMockRowTable(2, 5, true)
	subTable := newSubTable(rowTable)
	// min hash table size is 32
	require.Equal(t, 32, len(subTable.hashTable))
	rowTable = createMockRowTable(32, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 64, len(subTable.hashTable))
	rowTable = createMockRowTable(33, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 64, len(subTable.hashTable))
	rowTable = createMockRowTable(64, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 128, len(subTable.hashTable))
	rowTable = createMockRowTable(65, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 128, len(subTable.hashTable))
}

func TestBuild(t *testing.T) {
	rowTable, taggedBits, err := createRowTable(1000000)
	require.NoError(t, err)
	tagHelper := &tagPtrHelper{}
	tagHelper.init(taggedBits)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments), tagHelper)
	rowSet := make(map[unsafe.Pointer]struct{}, rowTable.rowCount())
	for _, seg := range rowTable.segments {
		for index := range seg.rowStartOffset {
			loc := seg.getRowPointer(index)
			_, ok := rowSet[loc]
			require.False(t, ok)
			rowSet[loc] = struct{}{}
		}
	}
	rowCount := 0
	for _, locHolder := range subTable.hashTable {
		for locHolder != 0 {
			rowCount++
			loc := tagHelper.toUnsafePointer(locHolder)
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			// use 0 as hashvalue so getNextRowAddress won't exit early
			locHolder = getNextRowAddress(loc, tagHelper, 0)
		}
	}
	require.Equal(t, 0, len(rowSet))
	require.Equal(t, rowTable.rowCount(), uint64(rowCount))
}

func TestConcurrentBuild(t *testing.T) {
	rowTable, tagBits, err := createRowTable(3000000)
	require.NoError(t, err)
	subTable := newSubTable(rowTable)
	segmentCount := len(rowTable.segments)
	buildThreads := 3
	tagHelper := &tagPtrHelper{}
	tagHelper.init(tagBits)
	wg := util.WaitGroupWrapper{}
	for i := 0; i < buildThreads; i++ {
		segmentStart := segmentCount / buildThreads * i
		segmentEnd := segmentCount / buildThreads * (i + 1)
		if i == buildThreads-1 {
			segmentEnd = segmentCount
		}
		wg.Run(func() {
			subTable.build(segmentStart, segmentEnd, tagHelper)
		})
	}
	wg.Wait()
	rowSet := make(map[unsafe.Pointer]struct{}, rowTable.rowCount())
	for _, seg := range rowTable.segments {
		for index := range seg.rowStartOffset {
			loc := seg.getRowPointer(index)
			_, ok := rowSet[loc]
			require.False(t, ok)
			rowSet[loc] = struct{}{}
		}
	}
	for _, locHolder := range subTable.hashTable {
		for locHolder != 0 {
			loc := tagHelper.toUnsafePointer(locHolder)
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			locHolder = getNextRowAddress(loc, tagHelper, 0)
		}
	}
	require.Equal(t, 0, len(rowSet))
}

func TestLookup(t *testing.T) {
	rowTable, tagBits, err := createRowTable(200000)
	require.NoError(t, err)
	tagHelper := &tagPtrHelper{}
	tagHelper.init(tagBits)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments), tagHelper)

	for _, seg := range rowTable.segments {
		for index := range seg.rowStartOffset {
			hashValue := seg.hashValues[index]
			candidate := subTable.lookup(hashValue, tagHelper)
			loc := seg.getRowPointer(index)
			found := false
			for candidate != 0 {
				candidatePtr := tagHelper.toUnsafePointer(candidate)
				if candidatePtr == loc {
					found = true
					break
				}
				candidate = getNextRowAddress(candidatePtr, tagHelper, hashValue)
			}
			require.True(t, found)
		}
	}
}

func checkRowIter(t *testing.T, table *hashTableV2, scanConcurrency int) {
	// first create a map containing all the row locations
	totalRowCount := table.totalRowCount()
	rowSet := make(map[unsafe.Pointer]struct{}, totalRowCount)
	for _, rt := range table.tables {
		for _, seg := range rt.rowData.segments {
			for index := range seg.rowStartOffset {
				loc := seg.getRowPointer(index)
				_, ok := rowSet[loc]
				require.False(t, ok)
				rowSet[loc] = struct{}{}
			}
		}
	}
	// create row iters
	rowIters := make([]*rowIter, 0, scanConcurrency)
	rowPerScan := totalRowCount / uint64(scanConcurrency)
	for i := uint64(0); i < uint64(scanConcurrency); i++ {
		startIndex := rowPerScan * i
		endIndex := rowPerScan * (i + 1)
		if i == uint64(scanConcurrency-1) {
			endIndex = totalRowCount
		}
		rowIters = append(rowIters, table.createRowIter(startIndex, endIndex))
	}

	locCount := uint64(0)
	for _, it := range rowIters {
		for !it.isEnd() {
			locCount++
			loc := it.getValue()
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			it.next()
		}
	}
	require.Equal(t, table.totalRowCount(), locCount)
	require.Equal(t, 0, len(rowSet))
}

func TestRowIter(t *testing.T) {
	partitionNumbers := []int{1, 4, 8}
	// normal case
	for _, partitionNumber := range partitionNumbers {
		// create row tables
		rowTables := make([]*rowTable, 0, partitionNumber)
		for i := 0; i < partitionNumber; i++ {
			rt := createMockRowTable(1024, 16, false)
			rowTables = append(rowTables, rt)
		}
		joinedHashTable := newJoinHashTableForTest(rowTables)
		checkRowIter(t, joinedHashTable, partitionNumber)
	}
	// case with empty row table
	for _, partitionNumber := range partitionNumbers {
		for i := 0; i < partitionNumber; i++ {
			// the i-th row table is an empty row table
			rowTables := make([]*rowTable, 0, partitionNumber)
			for j := 0; j < partitionNumber; j++ {
				if i == j {
					rt := createMockRowTable(0, 0, true)
					rowTables = append(rowTables, rt)
				} else {
					rt := createMockRowTable(1024, 16, false)
					rowTables = append(rowTables, rt)
				}
			}
			joinedHashTable := newJoinHashTableForTest(rowTables)
			checkRowIter(t, joinedHashTable, partitionNumber)
		}
	}
}
