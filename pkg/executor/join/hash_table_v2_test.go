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
		rowSeg := newRowTableSegment()
		// no empty segment is allowed
		rows := maxRowsPerSeg
		if !fixedSize {
			rows = int(rand.Int31n(int32(maxRowsPerSeg)) + 1)
		}
		rowSeg.rawData = make([]byte, rows)
		for j := 0; j < rows; j++ {
			rowSeg.rowStartOffset = append(rowSeg.rowStartOffset, uint64(j))
			rowSeg.validJoinKeyPos = append(rowSeg.validJoinKeyPos, j)
		}
		ret.segments = append(ret.segments, rowSeg)
	}
	return ret
}

func createRowTable(rows int) (*rowTable, error) {
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

	partitionNumber := 1
	chk := testutil.GenRandomChunks(buildTypes, rows)
	hashJoinCtx := &HashJoinCtxV2{
		PartitionNumber: partitionNumber,
		hashTableMeta:   meta,
	}
	hashJoinCtx.Concurrency = 1
	hashJoinCtx.initHashTableContext()
	hashJoinCtx.SessCtx = mock.NewContext()
	builder := createRowTableBuilder(buildKeyIndex, buildKeyTypes, partitionNumber, hasNullableKey, false, false)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, 0)
	if err != nil {
		return nil, err
	}
	builder.appendRemainingRowLocations(0, hashJoinCtx.hashTableContext)
	return hashJoinCtx.hashTableContext.rowTables[0][0], nil
}

func TestHashTableSize(t *testing.T) {
	rowTable := createMockRowTable(10, 5, true)
	subTable := newSubTable(rowTable)
	// min hash table size is 1024
	require.Equal(t, 1024, len(subTable.hashTable))
	rowTable = createMockRowTable(1024, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 2048, len(subTable.hashTable))
	rowTable = createMockRowTable(1025, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 2048, len(subTable.hashTable))
	rowTable = createMockRowTable(2048, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 4096, len(subTable.hashTable))
	rowTable = createMockRowTable(2049, 1, true)
	subTable = newSubTable(rowTable)
	require.Equal(t, 4096, len(subTable.hashTable))
}

func TestBuild(t *testing.T) {
	rowTable, err := createRowTable(1000000)
	require.NoError(t, err)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments))
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
			loc := *(*unsafe.Pointer)(unsafe.Pointer(&locHolder))
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			locHolder = getNextRowAddress(loc)
		}
	}
	require.Equal(t, 0, len(rowSet))
	require.Equal(t, rowTable.rowCount(), uint64(rowCount))
}

func TestConcurrentBuild(t *testing.T) {
	rowTable, err := createRowTable(3000000)
	require.NoError(t, err)
	subTable := newSubTable(rowTable)
	segmentCount := len(rowTable.segments)
	buildThreads := 3
	wg := util.WaitGroupWrapper{}
	for i := 0; i < buildThreads; i++ {
		segmentStart := segmentCount / buildThreads * i
		segmentEnd := segmentCount / buildThreads * (i + 1)
		if i == buildThreads-1 {
			segmentEnd = segmentCount
		}
		wg.Run(func() {
			subTable.build(segmentStart, segmentEnd)
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
			loc := *(*unsafe.Pointer)(unsafe.Pointer(&locHolder))
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			locHolder = getNextRowAddress(loc)
		}
	}
	require.Equal(t, 0, len(rowSet))
}

func TestLookup(t *testing.T) {
	rowTable, err := createRowTable(200000)
	require.NoError(t, err)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments))

	for _, seg := range rowTable.segments {
		for index := range seg.rowStartOffset {
			hashValue := seg.hashValues[index]
			candidate := subTable.lookup(hashValue)
			loc := seg.getRowPointer(index)
			found := false
			for candidate != 0 {
				candidatePtr := *(*unsafe.Pointer)(unsafe.Pointer(&candidate))
				if candidatePtr == loc {
					found = true
					break
				}
				candidate = getNextRowAddress(candidatePtr)
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
	partitionNumbers := []int{1, 5, 10}
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
