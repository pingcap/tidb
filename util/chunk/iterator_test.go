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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

func (s *testChunkSuite) TestIterator(c *check.C) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	chk := NewChunk(fields)
	n := 10
	var expected []int64
	for i := 0; i < n; i++ {
		chk.AppendInt64(0, int64(i))
		expected = append(expected, int64(i))
	}
	var rows []Row
	li := NewList(fields, 1)
	li2 := NewList(fields, 5)
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

	it = NewIterator4Slice(nil)
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4Chunk(new(Chunk))
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4List(new(List))
	c.Assert(it.Begin(), check.Equals, it.End())
	it = NewIterator4RowPtr(li, nil)
	c.Assert(it.Begin(), check.Equals, it.End())
}

func checkIterator(c *check.C, it Iterator, expected []int64) {
	var got []int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		got = append(got, row.GetInt64(0))
	}
	c.Assert(got, check.DeepEquals, expected)
}
