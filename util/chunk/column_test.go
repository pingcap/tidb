// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

func equalColumn(c1, c2 *Column) bool {
	if c1.length != c2.length ||
		c1.nullCount != c2.nullCount {
		return false
	}
	if len(c1.nullBitmap) != len(c2.nullBitmap) ||
		len(c1.offsets) != len(c2.offsets) ||
		len(c1.data) != len(c2.data) ||
		len(c1.elemBuf) != len(c2.elemBuf) {
		return false
	}
	for i := range c1.nullBitmap {
		if c1.nullBitmap[i] != c2.nullBitmap[i] {
			return false
		}
	}
	for i := range c1.offsets {
		if c1.offsets[i] != c2.offsets[i] {
			return false
		}
	}
	for i := range c1.data {
		if c1.data[i] != c2.data[i] {
			return false
		}
	}
	for i := range c1.elemBuf {
		if c1.elemBuf[i] != c2.elemBuf[i] {
			return false
		}
	}
	return true
}

func (s *testChunkSuite) TestColumnCopy(c *check.C) {
	col := newFixedLenColumn(8, 10)
	for i := 0; i < 10; i++ {
		col.AppendInt64(int64(i))
	}

	c1 := col.copyConstruct()
	c.Check(equalColumn(col, c1), check.IsTrue)
}

func (s *testChunkSuite) TestLargeStringColumnOffset(c *check.C) {
	numRows := 1
	col := newVarLenColumn(numRows, nil)
	col.offsets[0] = 6 << 30
	c.Check(col.offsets[0], check.Equals, int64(6<<30)) // test no overflow.
}

func (s *testChunkSuite) TestI64Column(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendInt64(int64(i))
	}

	i64s := col.Int64s()
	for i := 0; i < 1024; i++ {
		c.Assert(i64s[i], check.Equals, int64(i))
		i64s[i] ++
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetInt64(0), check.Equals, int64(i+1))
		i++
	}
}

func (s *testChunkSuite) TestF64Column(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendFloat64(float64(i))
	}

	f64s := col.Float64s()
	for i := 0; i < 1024; i++ {
		c.Assert(f64s[i], check.Equals, float64(i))
		f64s[i] /= 2
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetFloat64(0), check.Equals, float64(i)/2)
		i++
	}
}

func (s *testChunkSuite) TestF32Column(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeFloat)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendFloat32(float32(i))
	}

	f32s := col.Float32s()
	for i := 0; i < 1024; i++ {
		c.Assert(f32s[i], check.Equals, float32(i))
		f32s[i] /= 2
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetFloat32(0), check.Equals, float32(i)/2)
		i++
	}
}

func (s *testChunkSuite) TestStringColumn(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	for i := 0; i < 1024; i++ {
		col.AppendString(fmt.Sprintf("%v", i))
	}
	for i := 0; i < 1024; i++ {
		c.Assert(col.GetString(i), check.Equals, fmt.Sprintf("%v", i))
	}
}
