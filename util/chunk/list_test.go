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

func (s *testChunkSuite) TestList(c *check.C) {
	fields := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	l := NewList(fields, 2)
	srcChunk := NewChunk(fields)
	srcChunk.AppendInt64(0, 1)
	srcRow := srcChunk.GetRow(0)

	// Test basic append.
	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(l.NumChunks(), check.Equals, 3)
	c.Assert(l.Len(), check.Equals, 5)
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test chunk reuse.
	l.Reset()
	c.Assert(len(l.freelist), check.Equals, 3)

	for i := 0; i < 5; i++ {
		l.AppendRow(srcRow)
	}
	c.Assert(len(l.freelist), check.Equals, 0)

	// Test add chunk then append row.
	l.Reset()
	nChunk := NewChunk(fields)
	nChunk.AppendNull(0)
	l.Add(nChunk)
	ptr := l.AppendRow(srcRow)
	c.Assert(l.NumChunks(), check.Equals, 1)
	c.Assert(ptr.ChkIdx, check.Equals, uint32(0))
	c.Assert(ptr.RowIdx, check.Equals, uint32(1))
	row := l.GetRow(ptr)
	c.Assert(row.GetInt64(0), check.Equals, int64(1))

	// Test iteration.
	l.Reset()
	for i := 0; i < 5; i++ {
		tmp := NewChunk(fields)
		tmp.AppendInt64(0, int64(i))
		l.AppendRow(tmp.GetRow(0))
	}
	expected := []int64{0, 1, 2, 3, 4}
	var results []int64
	err := l.Walk(func(r Row) error {
		results = append(results, r.GetInt64(0))
		return nil
	})
	c.Assert(err, check.IsNil)
	c.Assert(results, check.DeepEquals, expected)
}
