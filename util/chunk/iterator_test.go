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
	chk := NewChunkWithCapacity(fields, 32)
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

	var itb Iterable = IterableRows(rows)
	it := itb.Iterator()
	checkIterator(c, it, expected)
	it = itb.Iterator()
	for i := 0; i < 5; i++ {
		c.Assert(it.Next(), check.Equals, rows[i])
	}
	it.ReachEnd()
	c.Assert(it.HasNext(), check.Equals, false)
	it = itb.Iterator()
	c.Assert(it.Next(), check.Equals, rows[0])

	itb = chk
	it = itb.Iterator()
	checkIterator(c, it, expected)
	it = itb.Iterator()
	for i := 0; i < 5; i++ {
		c.Assert(it.Next(), check.Equals, chk.GetRow(i))
	}
	it.ReachEnd()
	c.Assert(it.Next(), check.Equals, NoneRow)
	it = itb.Iterator()
	c.Assert(it.Next(), check.Equals, chk.GetRow(0))

	itb = li
	it = itb.Iterator()
	checkIterator(c, it, expected)
	it = itb.Iterator()
	for i := 0; i < 5; i++ {
		c.Assert(it.Next(), check.Equals, li.GetRow(ptrs[i]))
	}
	it.ReachEnd()
	c.Assert(it.HasNext(), check.Equals, false)
	it = itb.Iterator()
	c.Assert(it.Next(), check.Equals, li.GetRow(ptrs[0]))

	itb = Iterable4RowPtr{li, ptrs}
	it = itb.Iterator()
	checkIterator(c, it, expected)
	it = itb.Iterator()
	for i := 0; i < 5; i++ {
		c.Assert(it.Next(), check.Equals, li.GetRow(ptrs[i]))
	}
	it.ReachEnd()
	c.Assert(it.HasNext(), check.Equals, false)
	it = itb.Iterator()
	c.Assert(it.Next(), check.Equals, li.GetRow(ptrs[0]))

	itb = Iterable4RowPtr{li2, ptrs2}
	it = itb.Iterator()
	checkIterator(c, it, expected)
	it = itb.Iterator()
	for i := 0; i < 5; i++ {
		c.Assert(it.Next(), check.Equals, li2.GetRow(ptrs2[i]))
	}
	it.ReachEnd()
	c.Assert(it.HasNext(), check.Equals, false)
	it = itb.Iterator()
	c.Assert(it.Next(), check.Equals, li2.GetRow(ptrs2[0]))

	itb = IterableRows(nil)
	it = itb.Iterator()
	c.Assert(it.HasNext(), check.Equals, false)
	c.Assert(it.Next(), check.Equals, NoneRow)
	itb = new(Chunk)
	it = itb.Iterator()
	c.Assert(it.HasNext(), check.Equals, false)
	c.Assert(it.Next(), check.Equals, NoneRow)
	itb = new(List)
	it = itb.Iterator()
	c.Assert(it.HasNext(), check.Equals, false)
	c.Assert(it.Next(), check.Equals, NoneRow)
	itb = Iterable4RowPtr{li, nil}
	it = itb.Iterator()
	c.Assert(it.HasNext(), check.Equals, false)
	c.Assert(it.Next(), check.Equals, NoneRow)
}

func checkIterator(c *check.C, it Iterator, expected []int64) {
	var got []int64
	for it.HasNext() {
		row := it.Next()
		got = append(got, row.GetInt64(0))
	}
	c.Assert(got, check.DeepEquals, expected)
}
