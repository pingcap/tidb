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

package chunk_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func RunAllocator(t *testing.T) {
	alloc := chunk.ExportedNewAllocator()

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	initCap := 5
	maxChunkSize := 100

	chk := alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	require.NotNil(t, chk)
	check := func() {
		require.Equal(t, len(fieldTypes), chk.NumCols())
		require.Nil(t, chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[0]))
		require.Nil(t, chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[1]))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[2]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[2])))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[3]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[3])))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[4]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[4])))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[5]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[5])))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[6]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[6])))
		require.Equal(t, chunk.ExportedGetFixedLen(fieldTypes[7]), len(chunk.ExportedGetColumnElemBuf(chunk.ExportedGetColumns(chk)[7])))

		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[2]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[2])))
		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[3]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[3])))
		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[4]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[4])))
		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[5]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[5])))
		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[6]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[6])))
		require.Equal(t, initCap*chunk.ExportedGetFixedLen(fieldTypes[7]), cap(chunk.ExportedGetColumnData(chunk.ExportedGetColumns(chk)[7])))
	}
	check()

	// Call Reset and alloc again, check the result.
	alloc.Reset()
	chk = alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	check()

	// Check maxFreeListLen
	for range *chunk.ExportedMaxFreeChunks + 10 {
		alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	}
	alloc.Reset()
	require.Equal(t, len(chunk.ExportedGetAllocatorFree(alloc)), *chunk.ExportedMaxFreeChunks)
}

func RunColumnAllocator(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	var alloc1 chunk.ExportedPoolColumnAllocator
	chunk.ExportedPoolColumnAllocatorInit(&alloc1)
	var alloc2 chunk.ExportedDefaultColumnAllocator

	// Test the basic allocate operation.
	initCap := 5
	for _, ft := range fieldTypes {
		v0 := chunk.ExportedNewColumn(ft, initCap)
		v1 := alloc1.NewColumn(ft, initCap)
		v2 := alloc2.NewColumn(ft, initCap)
		require.Equal(t, v0, v1)
		require.Equal(t, v1, v2)
	}

	ft := fieldTypes[2]
	// Test reuse.
	cols := make([]*chunk.ExportedColumn, 0, *chunk.ExportedMaxFreeColumnsPerType+10)
	for range *chunk.ExportedMaxFreeColumnsPerType + 10 {
		col := alloc1.NewColumn(ft, 20)
		cols = append(cols, col)
	}
	for _, col := range cols {
		chunk.ExportedPoolColumnAllocatorPut(&alloc1, col)
	}

	// Check max column size.
	freeList := chunk.ExportedPoolColumnAllocatorPool(&alloc1)[chunk.ExportedGetFixedLen(ft)]
	require.NotNil(t, freeList)
	require.Equal(t, freeList.Len(), *chunk.ExportedMaxFreeColumnsPerType)
}

func RunNoDuplicateColumnReuse(t *testing.T) {
	// For issue https://github.com/pingcap/tidb/issues/29554
	// Some chunk columns are just references to other chunk columns.
	// So when reusing Chunk, some columns may point to the same memory address.

	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}
	alloc := chunk.ExportedNewAllocator()
	for range *chunk.ExportedMaxFreeChunks + 10 {
		chk := alloc.Alloc(fieldTypes, 5, 10)
		chunk.ExportedMakeRef(chk, 1, 3)
	}
	alloc.Reset()

	a := chunk.ExportedGetColumnAlloc(alloc)
	// Make sure no duplicated column in the pool.
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(a) {
		dup := make(map[*chunk.ExportedColumn]struct{})
		for !chunk.ExportedColumnListEmpty(p) {
			c := chunk.ExportedColumnListPop(p)
			_, exist := dup[c]
			require.False(t, exist)
			dup[c] = struct{}{}
		}
	}
}

func RunAvoidColumnReuse(t *testing.T) {
	// For issue: https://github.com/pingcap/tidb/issues/31981
	// Some chunk columns are references to rpc message.
	// So when reusing Chunk, we should ignore them.

	fieldTypes := []*types.FieldType{
		types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).BuildP(),
	}
	alloc := chunk.ExportedNewAllocator()
	for range *chunk.ExportedMaxFreeChunks + 10 {
		chk := alloc.Alloc(fieldTypes, 5, 10)
		for _, col := range chunk.ExportedGetColumns(chk) {
			chunk.ExportedSetColumnAvoidReusing(col, true)
		}
	}
	alloc.Reset()

	a := chunk.ExportedGetColumnAlloc(alloc)
	// Make sure no duplicated column in the pool.
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(a) {
		require.True(t, chunk.ExportedColumnListEmpty(p))
	}

	// test decoder will set avoid reusing flag.
	chk := alloc.Alloc(fieldTypes, 5, 1024)
	for range 10 {
		for _, col := range chunk.ExportedGetColumns(chk) {
			col.AppendNull()
		}
	}
	codec := chunk.ExportedNewCodec(fieldTypes)
	buf := codec.Encode(chk)

	decoder := chunk.ExportedNewDecoder(
		chunk.ExportedNewChunkWithCapacity(fieldTypes, 0),
		fieldTypes,
	)
	decoder.Reset(buf)
	decoder.ReuseIntermChk(chk)
	for _, col := range chunk.ExportedGetColumns(chk) {
		require.True(t, chunk.ExportedGetColumnAvoidReusing(col))
	}
}

func RunColumnAllocatorLimit(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).BuildP(),
	}

	//set cache size
	chunk.ExportedInitChunkAllocSize(10, 20)
	alloc := chunk.ExportedNewAllocator()
	require.True(t, alloc.CheckReuseAllocSize())
	for range *chunk.ExportedMaxFreeChunks + 10 {
		alloc.Alloc(fieldTypes, 5, 10)
	}
	alloc.Reset()
	require.Equal(t, len(chunk.ExportedGetAllocatorFree(alloc)), 10)
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(chunk.ExportedGetColumnAlloc(alloc)) {
		require.True(t, (chunk.ExportedColumnListLen(p) <= 20))
	}

	//Reduce capacity
	chunk.ExportedInitChunkAllocSize(5, 10)
	alloc = chunk.ExportedNewAllocator()
	for range *chunk.ExportedMaxFreeChunks + 10 {
		alloc.Alloc(fieldTypes, 5, 10)
	}
	alloc.Reset()
	require.Equal(t, len(chunk.ExportedGetAllocatorFree(alloc)), 5)
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(chunk.ExportedGetColumnAlloc(alloc)) {
		require.True(t, (p.Len() <= 10))
	}

	//increase capacity
	chunk.ExportedInitChunkAllocSize(50, 100)
	alloc = chunk.ExportedNewAllocator()
	for range *chunk.ExportedMaxFreeChunks + 10 {
		alloc.Alloc(fieldTypes, 5, 10)
	}
	alloc.Reset()
	require.Equal(t, len(chunk.ExportedGetAllocatorFree(alloc)), 50)
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(chunk.ExportedGetColumnAlloc(alloc)) {
		require.True(t, (p.Len() <= 100))
	}

	//long characters are not cached
	alloc = chunk.ExportedNewAllocator()
	rs := alloc.Alloc([]*types.FieldType{types.NewFieldTypeBuilder().SetType(mysql.TypeVarchar).BuildP()}, 1024, 1024)
	concreteAlloc := chunk.ExportedAllocatorTypeCast(alloc)
	columnAlloc := chunk.ExportedGetAllocatorColumnAlloc(concreteAlloc)
	pool := chunk.ExportedPoolColumnAllocatorPool(columnAlloc)
	nu := len(chunk.ExportedColumnListGetAllocColumns(pool[chunk.ExportedVarElemLen]))
	require.Equal(t, nu, 1)
	for _, col := range chunk.ExportedGetColumns(rs) {
		data := chunk.ExportedGetColumnData(col)
		for range 20480 {
			data = append(data, byte('a'))
		}
		// Update the column data - we can't directly set col.data
		// For this test, we just need to grow the column
		chunk.ExportedSetColumnLength(col, len(data))
	}
	alloc.Reset()
	for _, p := range chunk.ExportedPoolColumnAllocatorPool(columnAlloc) {
		require.True(t, (chunk.ExportedColumnListLen(p) == 0))
	}

	chunk.ExportedInitChunkAllocSize(0, 0)
	alloc = chunk.ExportedNewBasicAllocator()
	require.False(t, alloc.CheckReuseAllocSize())
}

func RunColumnAllocatorCheck(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP(),
		types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).BuildP(),
	}
	chunk.ExportedInitChunkAllocSize(10, 20)
	alloc := chunk.ExportedNewAllocator()
	for range 4 {
		alloc.Alloc(fieldTypes, 5, 10)
	}
	concreteAlloc := chunk.ExportedAllocatorTypeCast(alloc)
	columnAlloc := chunk.ExportedGetAllocatorColumnAlloc(concreteAlloc)
	col := chunk.ExportedPoolColumnAllocatorNewColumn(columnAlloc, types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP(), 10)
	col.Reset(types.ETDatetime)
	alloc.Reset()
	pool := chunk.ExportedPoolColumnAllocatorPool(columnAlloc)
	num := chunk.ExportedColumnListLen(pool[chunk.ExportedGetFixedLen(types.NewFieldTypeBuilder().SetType(mysql.TypeFloat).BuildP())])
	require.Equal(t, num, 4)
	num = chunk.ExportedColumnListLen(pool[chunk.ExportedGetFixedLen(types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).BuildP())])
	require.Equal(t, num, 4)
}

func RunReuseHookAllocator(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	var reuse atomic.Int64

	chunk.ExportedInitChunkAllocSize(0, 0)
	alloc := chunk.ExportedNewReuseHookAllocator(chunk.ExportedNewBasicAllocator(), func() {
		reuse.Add(1)
	})
	// as we init MaxFreeChunks and MaxFreeColumns as 0, the reuse is still 0 after alloc
	chk := alloc.Alloc(fieldTypes, 5, 100)
	require.NotNil(t, chk)
	require.Equal(t, int64(0), reuse.Load())

	chunk.ExportedInitChunkAllocSize(10, 20)
	alloc = chunk.ExportedNewReuseHookAllocator(chunk.ExportedNewBasicAllocator(), func() {
		reuse.Add(1)
	})
	chk = alloc.Alloc(fieldTypes, 5, 100)
	require.NotNil(t, chk)
	require.Equal(t, int64(1), reuse.Load())
	// Another alloc will not touch it
	chk = alloc.Alloc(fieldTypes, 5, 100)
	require.NotNil(t, chk)
	require.Equal(t, int64(1), reuse.Load())
}

func RunSyncAllocator(t *testing.T) {
	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDatetime),
	}

	alloc := chunk.ExportedNewSyncAllocator(chunk.ExportedNewBasicAllocator())

	wg := &sync.WaitGroup{}
	for range 1000 {
		wg.Add(1)
		go func() {
			for range 10 {
				for range 100 {
					chk := alloc.Alloc(fieldTypes, 5, 100)
					require.NotNil(t, chk)
				}
				alloc.Reset()
			}

			wg.Done()
		}()
	}
	wg.Wait()
}
