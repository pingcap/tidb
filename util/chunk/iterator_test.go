// Copyright 2017 PingCAP, Inc.
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

func TestIteratorOnSel(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		chk.AppendInt64(0, int64(i))
		if i%2 == 0 {
			sel = append(sel, i)
		}
	}
	chk.SetSel(sel)
	it := NewIterator4Chunk(chk)
	cnt := 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		require.Equal(t, int64(0), row.GetInt64(0)%2)
		cnt++
	}
	require.Equal(t, 1024/2, cnt)
}

func checkEqual(it Iterator, exp []int64, t *testing.T) {
	require.Equal(t, len(exp), it.Len())
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		require.Equal(t, exp[i], row.GetInt64(0))
	}
}

func TestMultiIterator(t *testing.T) {
	it := NewMultiIterator(NewIterator4Chunk(new(Chunk)))
	require.Equal(t, it.End(), it.Begin())

	it = NewMultiIterator(NewIterator4Chunk(new(Chunk)), NewIterator4List(new(List)))
	require.Equal(t, it.End(), it.Begin())

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	n := 10
	var expected []int64
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
		expected = append(expected, int64(i))
	}
	it = NewMultiIterator(NewIterator4Chunk(chk))
	checkEqual(it, expected, t)

	it = NewMultiIterator(NewIterator4Chunk(new(Chunk)), NewIterator4Chunk(chk), NewIterator4Chunk(new(Chunk)))
	checkEqual(it, expected, t)

	li := NewList(fields, 32, 1024)
	chk2 := New(fields, 32, 1024)
	for i := n; i < n*2; i++ {
		expected = append(expected, int64(i))
		chk2.AppendInt64(0, int64(i))
	}
	li.Add(chk2)

	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4Chunk(chk2)), expected, t)
	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4List(li)), expected, t)
	rc := NewRowContainer(fields, 1024)
	rc.m.records.inMemory = li
	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4RowContainer(rc)), expected, t)

	li.Clear()
	li.Add(chk)
	checkEqual(NewMultiIterator(NewIterator4List(li), NewIterator4Chunk(chk2)), expected, t)
	rc = NewRowContainer(fields, 1024)
	rc.m.records.inMemory = new(List)
	checkEqual(NewMultiIterator(NewIterator4RowContainer(rc), NewIterator4List(li), NewIterator4Chunk(chk2)), expected, t)
}

func TestIterator(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	n := 10
	var expected []int64
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
		expected = append(expected, int64(i))
	}
	var rows []Row
	li := NewList(fields, 1, 2)
	li2 := NewList(fields, 8, 16)
	var ptrs []RowPtr
	var ptrs2 []RowPtr
	for i := 0; i < n; i++ {
		rows = append(rows, chk.GetRow(i))
		ptr := li.AppendRow(chk.GetRow(i))
		ptrs = append(ptrs, ptr)
		ptr2 := li2.AppendRow(chk.GetRow(i))
		ptrs2 = append(ptrs2, ptr2)
	}

	it := NewIterator4Slice(rows)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, rows[i], it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, rows[0], it.Begin())

	it = NewIterator4Chunk(chk)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, chk.GetRow(i), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, chk.GetRow(0), it.Begin())

	it = NewIterator4List(li)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, li.GetRow(ptrs[i]), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, li.GetRow(ptrs[0]), it.Begin())

	it = NewIterator4RowPtr(li, ptrs)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, li.GetRow(ptrs[i]), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, li.GetRow(ptrs[0]), it.Begin())

	it = NewIterator4RowPtr(li2, ptrs2)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, li2.GetRow(ptrs2[i]), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, li2.GetRow(ptrs2[0]), it.Begin())

	rc := NewRowContainer(fields, 1024)
	rc.m.records.inMemory = li
	it = NewIterator4RowContainer(rc)
	checkEqual(it, expected, t)
	it.Begin()
	for i := 0; i < 5; i++ {
		require.Equal(t, li.GetRow(ptrs[i]), it.Current())
		it.Next()
	}
	it.ReachEnd()
	require.Equal(t, it.End(), it.Current())
	require.Equal(t, li.GetRow(ptrs[0]), it.Begin())

	it = NewIterator4Slice(nil)
	require.Equal(t, it.End(), it.Begin())
	it = NewIterator4Chunk(new(Chunk))
	require.Equal(t, it.End(), it.Begin())
	it = NewIterator4List(new(List))
	require.Equal(t, it.End(), it.Begin())
	it = NewIterator4RowPtr(li, nil)
	require.Equal(t, it.End(), it.Begin())
	rc = NewRowContainer(fields, 1024)
	rc.m.records.inMemory = NewList(fields, 1, 1)
	it = NewIterator4RowContainer(rc)
	require.Equal(t, it.End(), it.Begin())
}
