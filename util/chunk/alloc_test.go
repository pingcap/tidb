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

package chunk

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestAllocator(t *testing.T) {
	alloc := NewAllocator()

	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}

	initCap := 5
	maxChunkSize := 100

	chk := alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	require.NotNil(t, chk)
	check := func() {
		require.Equal(t, len(fieldTypes), chk.NumCols())
		require.Nil(t, chk.columns[0].elemBuf)
		require.Nil(t, chk.columns[1].elemBuf)
		require.Equal(t, getFixedLen(fieldTypes[2]), len(chk.columns[2].elemBuf))
		require.Equal(t, getFixedLen(fieldTypes[3]), len(chk.columns[3].elemBuf))
		require.Equal(t, getFixedLen(fieldTypes[4]), len(chk.columns[4].elemBuf))
		require.Equal(t, getFixedLen(fieldTypes[5]), len(chk.columns[5].elemBuf))
		require.Equal(t, getFixedLen(fieldTypes[6]), len(chk.columns[6].elemBuf))
		require.Equal(t, getFixedLen(fieldTypes[7]), len(chk.columns[7].elemBuf))

		require.Equal(t, initCap*getFixedLen(fieldTypes[2]), cap(chk.columns[2].data))
		require.Equal(t, initCap*getFixedLen(fieldTypes[3]), cap(chk.columns[3].data))
		require.Equal(t, initCap*getFixedLen(fieldTypes[4]), cap(chk.columns[4].data))
		require.Equal(t, initCap*getFixedLen(fieldTypes[5]), cap(chk.columns[5].data))
		require.Equal(t, initCap*getFixedLen(fieldTypes[6]), cap(chk.columns[6].data))
		require.Equal(t, initCap*getFixedLen(fieldTypes[7]), cap(chk.columns[7].data))
	}
	check()

	// Call Reset and alloc again, check the result.
	alloc.Reset()
	chk = alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	check()

	// Check maxFreeListLen
	for i := 0; i < maxFreeChunks+10; i++ {
		alloc.Alloc(fieldTypes, initCap, maxChunkSize)
	}
	alloc.Reset()
	require.Equal(t, len(alloc.free), maxFreeChunks)
}

func TestColumnAllocator(t *testing.T) {
	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}

	var alloc1 poolColumnAllocator
	alloc1.init()
	var alloc2 DefaultColumnAllocator

	// Test the basic allocate operation.
	initCap := 5
	for _, ft := range fieldTypes {
		v0 := NewColumn(ft, initCap)
		v1 := alloc1.NewColumn(ft, initCap)
		v2 := alloc2.NewColumn(ft, initCap)
		require.Equal(t, v0, v1)
		require.Equal(t, v1, v2)
	}

	ft := fieldTypes[2]
	// Test reuse.
	cols := make([]*Column, 0, maxFreeColumnsPerType+10)
	for i := 0; i < maxFreeColumnsPerType+10; i++ {
		col := alloc1.NewColumn(ft, 20)
		cols = append(cols, col)
	}
	for _, col := range cols {
		alloc1.put(col)
	}

	// Check max column size.
	freeList := alloc1.pool[getFixedLen(ft)]
	require.NotNil(t, freeList)
	require.Len(t, freeList, maxFreeColumnsPerType)
}

func TestNoDuplicateColumnReuse(t *testing.T) {
	// For issue https://github.com/pingcap/tidb/issues/29554
	// Some chunk columns are just references to other chunk columns.
	// So when reusing Chunk, some columns may point to the same memory address.

	fieldTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeJSON},
		{Tp: mysql.TypeFloat},
		{Tp: mysql.TypeNewDecimal},
		{Tp: mysql.TypeDouble},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeTimestamp},
		{Tp: mysql.TypeDatetime},
	}
	alloc := NewAllocator()
	for i := 0; i < maxFreeChunks+10; i++ {
		chk := alloc.Alloc(fieldTypes, 5, 10)
		chk.MakeRef(1, 3)
	}
	alloc.Reset()

	a := alloc.columnAlloc
	// Make sure no duplicated column in the pool.
	for _, p := range a.pool {
		dup := make(map[*Column]struct{})
		for !p.empty() {
			c := p.pop()
			_, exist := dup[c]
			require.False(t, exist)
			dup[c] = struct{}{}
		}
	}
}
