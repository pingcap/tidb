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

package partitionedhashjoin

import (
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func createMockRowTable(rows int) *rowTable {
	ret := &rowTable{
		meta: nil,
	}
	rowSeg := newRowTableSegment()
	rowSeg.rawData = make([]byte, rows)
	for i := 0; i < rows; i++ {
		rowSeg.rowLocations = append(rowSeg.rowLocations, uintptr(unsafe.Pointer(&rowSeg.rawData[i])))
		rowSeg.validJoinKeyPos = append(rowSeg.validJoinKeyPos, i)
	}
	ret.segments = append(ret.segments, rowSeg)
	return ret
}

func createRowTable(t *testing.T, rows int) *rowTable {
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
	chk := chunk.GenRandomChunks(buildTypes, rows)
	hashJoinCtx := &PartitionedHashJoinCtx{
		SessCtx:         mock.NewContext(),
		PartitionNumber: partitionNumber,
		hashTableMeta:   meta,
	}
	rowTables := make([]*rowTable, hashJoinCtx.PartitionNumber)
	builder := createRowTableBuilder(buildKeyIndex, buildSchema, meta, partitionNumber, hasNullableKey, false, false)
	err := builder.processOneChunk(chk, hashJoinCtx.SessCtx.GetSessionVars().StmtCtx.TypeCtx(), hashJoinCtx, rowTables)
	require.NoError(t, err)
	builder.appendRemainingRowLocations(rowTables)
	return rowTables[0]
}

func TestHashTableSize(t *testing.T) {
	rowTable := createMockRowTable(10)
	subTable := newSubTable(rowTable)
	// min hash table size is 1024
	require.Equal(t, 1024, len(subTable.hashTable))
	rowTable = createMockRowTable(1024)
	subTable = newSubTable(rowTable)
	require.Equal(t, 1024, len(subTable.hashTable))
	rowTable = createMockRowTable(1025)
	subTable = newSubTable(rowTable)
	require.Equal(t, 2048, len(subTable.hashTable))
}

func TestBuild(t *testing.T) {
	rowTable := createRowTable(t, 1000000)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments))
	rowSet := make(map[uintptr]struct{}, rowTable.rowCount())
	for _, seg := range rowTable.segments {
		for _, loc := range seg.rowLocations {
			_, ok := rowSet[loc]
			require.False(t, ok)
			rowSet[loc] = struct{}{}
		}
	}
	rowCount := 0
	for _, loc := range subTable.hashTable {
		for loc != 0 {
			rowCount++
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			loc = getNextRowAddress(loc)
		}
	}
	require.Equal(t, 0, len(rowSet))
	require.Equal(t, rowTable.rowCount(), rowCount)
}

func TestConcurrentBuild(t *testing.T) {
	rowTable := createRowTable(t, 3000000)
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
	rowSet := make(map[uintptr]struct{}, rowTable.rowCount())
	for _, seg := range rowTable.segments {
		for _, loc := range seg.rowLocations {
			_, ok := rowSet[loc]
			require.False(t, ok)
			rowSet[loc] = struct{}{}
		}
	}
	for _, loc := range subTable.hashTable {
		for loc != 0 {
			_, ok := rowSet[loc]
			require.True(t, ok)
			delete(rowSet, loc)
			loc = getNextRowAddress(loc)
		}
	}
	require.Equal(t, 0, len(rowSet))
}

func TestLookup(t *testing.T) {
	rowTable := createRowTable(t, 200000)
	subTable := newSubTable(rowTable)
	// single thread build
	subTable.build(0, len(rowTable.segments))

	for _, seg := range rowTable.segments {
		for index, loc := range seg.rowLocations {
			hashValue := seg.hashValues[index]
			candidate := subTable.lookup(hashValue)
			found := false
			for candidate != 0 {
				if candidate == loc {
					found = true
					break
				}
				candidate = getNextRowAddress(candidate)
			}
			require.True(t, found)
		}
	}
}
