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
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
)

func (s *testChunkSuite) TestIteratorOnSel(c *check.C) {
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
		c.Assert(row.GetInt64(0)%2, check.Equals, int64(0))
		cnt++
	}
	c.Assert(cnt, check.Equals, 1024/2)
}

func checkEqual(it Iterator, exp []int64, c *check.C) {
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		c.Assert(row.GetInt64(0), check.Equals, exp[i])
	}
}

func (s *testChunkSuite) TestMultiIterator(c *check.C) {
	it := NewMultiIterator(NewIterator4Chunk(new(Chunk)))
	c.Assert(it.Begin(), check.Equals, it.End())

	it = NewMultiIterator(NewIterator4Chunk(new(Chunk)), NewIterator4List(new(List)))
	c.Assert(it.Begin(), check.Equals, it.End())

	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := New(fields, 32, 1024)
	n := 10
	var expected []int64
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
		expected = append(expected, int64(i))
	}
	it = NewMultiIterator(NewIterator4Chunk(chk))
	checkEqual(it, expected, c)

	it = NewMultiIterator(NewIterator4Chunk(new(Chunk)), NewIterator4Chunk(chk), NewIterator4Chunk(new(Chunk)))
	checkEqual(it, expected, c)

	li := NewList(fields, 32, 1024)
	chk2 := New(fields, 32, 1024)
	for i := n; i < n*2; i++ {
		expected = append(expected, int64(i))
		chk2.AppendInt64(0, int64(i))
	}
	li.Add(chk2)

	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4Chunk(chk2)), expected, c)
	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4List(li)), expected, c)
	checkEqual(NewMultiIterator(NewIterator4Chunk(chk), NewIterator4RowContainer(&RowContainer{records: li})), expected, c)

	li.Clear()
	li.Add(chk)
	checkEqual(NewMultiIterator(NewIterator4List(li), NewIterator4Chunk(chk2)), expected, c)
	checkEqual(NewMultiIterator(NewIterator4RowContainer(&RowContainer{records: new(List)}), NewIterator4List(li), NewIterator4Chunk(chk2)), expected, c)
}

func (s *testChunkSuite) TestIterator(c *check.C) {
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
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, rows[i])
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, rows[0])

	it = NewIterator4Chunk(chk)
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, chk.GetRow(i))
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, chk.GetRow(0))

	it = NewIterator4List(li)
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, li.GetRow(ptrs[i]))
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, li.GetRow(ptrs[0]))

	it = NewIterator4RowPtr(li, ptrs)
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, li.GetRow(ptrs[i]))
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, li.GetRow(ptrs[0]))

	it = NewIterator4RowPtr(li2, ptrs2)
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, li2.GetRow(ptrs2[i]))
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, li2.GetRow(ptrs2[0]))

	rc := &RowContainer{
		records: li,
	}
	it = NewIterator4RowContainer(rc)
	checkIterator(c, it, expected)
	it.Begin()
	for i := 0; i < 5; i++ {
		c.Assert(it.Current(), check.Equals, li.GetRow(ptrs[i]))
		it.Next()
	}
	it.ReachEnd()
	c.Assert(it.Current(), check.Equals, it.End())
	c.Assert(it.Begin(), check.Equals, li.GetRow(ptrs[0]))

	it = NewIterator4Slice(nil)
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4Chunk(new(Chunk))
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4List(new(List))
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4RowPtr(li, nil)
	c.Assert(it.Begin(), check.Equals, it.End())
	rc = &RowContainer{
		records: NewList(fields, 1, 1),
	}
	it = NewIterator4RowContainer(rc)
	c.Assert(it.Begin(), check.Equals, it.End())
}

func checkIterator(c *check.C, it Iterator, expected []int64) {
	var got []int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		got = append(got, row.GetInt64(0))
	}
	c.Assert(got, check.DeepEquals, expected)
}
