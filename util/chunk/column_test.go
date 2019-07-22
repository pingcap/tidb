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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
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
		i64s[i]++
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

func (s *testChunkSuite) TestMyDecimal(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1); err != nil {
			c.Fatal(err)
		}
		col.AppendMyDecimal(d)
	}

	ds := col.MyDecimals()
	for i := 0; i < 1024; i++ {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1); err != nil {
			c.Fatal(err)
		}
		c.Assert(d.Compare(&ds[i]), check.Equals, 0)

		if err := types.DecimalAdd(&ds[i], d, &ds[i]); err != nil {
			c.Fatal(err)
		}
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		d := new(types.MyDecimal)
		if err := d.FromFloat64(float64(i) * 1.1 * 2); err != nil {
			c.Fatal(err)
		}

		delta := new(types.MyDecimal)
		if err := types.DecimalSub(d, row.GetMyDecimal(0), delta); err != nil {
			c.Fatal(err)
		}

		fDelta, err := delta.ToFloat64()
		if err != nil {
			c.Fatal(err)
		}
		if fDelta > 0.0001 || fDelta < -0.0001 {
			c.Fatal()
		}

		i++
	}
}

func (s *testChunkSuite) TestStringColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeVarString)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendString(fmt.Sprintf("%v", i*i))
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetString(0), check.Equals, fmt.Sprintf("%v", i*i))
		c.Assert(col.GetString(i), check.Equals, fmt.Sprintf("%v", i*i))
		i++
	}
}

func (s *testChunkSuite) TestSetColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeSet)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendSet(types.Set{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		s1 := col.GetSet(i)
		s2 := row.GetSet(0)
		c.Assert(s1.Name, check.Equals, s2.Name)
		c.Assert(s1.Value, check.Equals, s2.Value)
		c.Assert(s1.Name, check.Equals, fmt.Sprintf("%v", i))
		c.Assert(s1.Value, check.Equals, uint64(i))
		i++
	}
}

func (s *testChunkSuite) TestJSONColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeJSON)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		j := new(json.BinaryJSON)
		if err := j.UnmarshalJSON([]byte(fmt.Sprintf(`{"%v":%v}`, i, i))); err != nil {
			c.Fatal(err)
		}
		col.AppendJSON(*j)
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		j1 := col.GetJSON(i)
		j2 := row.GetJSON(0)
		c.Assert(j1.String(), check.Equals, j2.String())
		i++
	}
}

func (s *testChunkSuite) TestTimeColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDatetime)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendTime(types.CurrentTime(mysql.TypeDatetime))
		time.Sleep(time.Millisecond / 10)
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		j1 := col.GetTime(i)
		j2 := row.GetTime(0)
		c.Assert(j1.Compare(j2), check.Equals, 0)
		i++
	}
}

func (s *testChunkSuite) TestDurationColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDuration)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		j1 := col.GetDuration(i, 0)
		j2 := row.GetDuration(0, 0)
		c.Assert(j1.Compare(j2), check.Equals, 0)
		i++
	}
}

func (s *testChunkSuite) TestEnumColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeEnum)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendEnum(types.Enum{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		s1 := col.GetEnum(i)
		s2 := row.GetEnum(0)
		c.Assert(s1.Name, check.Equals, s2.Name)
		c.Assert(s1.Value, check.Equals, s2.Value)
		c.Assert(s1.Name, check.Equals, fmt.Sprintf("%v", i))
		c.Assert(s1.Value, check.Equals, uint64(i))
		i++
	}
}

func (s *testChunkSuite) TestNullsColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		if i%2 == 0 {
			col.AppendNull()
			continue
		}
		col.AppendInt64(int64(i))
	}

	it := NewIterator4Chunk(chk)
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		if i%2 == 0 {
			c.Assert(row.IsNull(0), check.Equals, true)
			c.Assert(col.IsNull(i), check.Equals, true)
		} else {
			c.Assert(row.GetInt64(0), check.Equals, int64(i))
		}
		i++
	}
}
