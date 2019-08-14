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
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func equalColumn(c1, c2 *Column) bool {
	if c1.length != c2.length ||
		c1.nullCount() != c2.nullCount() {
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

	c1 := col.CopyConstruct(nil)
	c.Check(equalColumn(col, c1), check.IsTrue)

	c2 := newFixedLenColumn(8, 10)
	c2 = col.CopyConstruct(c2)
	c.Check(equalColumn(col, c2), check.IsTrue)
}

func (s *testChunkSuite) TestColumnCopyReconstructFixedLen(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	results := make([]int64, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			col.AppendNull()
			nulls = append(nulls, true)
			results = append(results, 0)
			continue
		}

		v := rand.Int63()
		col.AppendInt64(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	col = col.CopyReconstruct(sel, nil)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(col.IsNull(n), check.Equals, true)
		} else {
			c.Assert(col.GetInt64(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, col.nullCount())
	c.Assert(col.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			col.AppendNull()
		} else {
			col.AppendInt64(int64(i * i * i))
		}
	}

	c.Assert(col.length, check.Equals, len(sel)+128)
	c.Assert(col.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(col.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(col.GetInt64(len(sel)+i), check.Equals, int64(i*i*i))
			c.Assert(col.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestColumnCopyReconstructVarLen(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	results := make([]string, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			col.AppendNull()
			nulls = append(nulls, true)
			results = append(results, "")
			continue
		}

		v := fmt.Sprintf("%v", rand.Int63())
		col.AppendString(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	col = col.CopyReconstruct(sel, nil)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(col.IsNull(n), check.Equals, true)
		} else {
			c.Assert(col.GetString(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, col.nullCount())
	c.Assert(col.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			col.AppendNull()
		} else {
			col.AppendString(fmt.Sprintf("%v", i*i*i))
		}
	}

	c.Assert(col.length, check.Equals, len(sel)+128)
	c.Assert(col.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(col.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(col.GetString(len(sel)+i), check.Equals, fmt.Sprintf("%v", i*i*i))
			c.Assert(col.IsNull(len(sel)+i), check.Equals, false)
		}
	}
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
	var i int
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetInt64(0), check.Equals, int64(i+1))
		c.Assert(col.GetInt64(i), check.Equals, int64(i+1))
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
		c.Assert(col.GetFloat64(int(i)), check.Equals, float64(i)/2)
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
		c.Assert(col.GetFloat32(int(i)), check.Equals, float32(i)/2)
		i++
	}
}

func (s *testChunkSuite) TestDurationSliceColumn(c *check.C) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDuration)}, 1024)
	col := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col.AppendDuration(types.Duration{Duration: time.Duration(i)})
	}

	ds := col.GoDurations()
	for i := 0; i < 1024; i++ {
		c.Assert(ds[i], check.Equals, time.Duration(i))
		d := types.Duration{Duration: ds[i]}
		d, _ = d.Add(d)
		ds[i] = d.Duration
	}

	it := NewIterator4Chunk(chk)
	var i int64
	for row := it.Begin(); row != it.End(); row = it.Next() {
		c.Assert(row.GetDuration(0, 0).Duration, check.Equals, time.Duration(i)*2)
		c.Assert(col.GetDuration(int(i), 0).Duration, check.Equals, time.Duration(i)*2)
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

	ds := col.Decimals()
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

func (s *testChunkSuite) TestReconstructFixedLen(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	results := make([]int64, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			col.AppendNull()
			nulls = append(nulls, true)
			results = append(results, 0)
			continue
		}

		v := rand.Int63()
		col.AppendInt64(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	col.reconstruct(sel)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(col.IsNull(n), check.Equals, true)
		} else {
			c.Assert(col.GetInt64(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, col.nullCount())
	c.Assert(col.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			col.AppendNull()
		} else {
			col.AppendInt64(int64(i * i * i))
		}
	}

	c.Assert(col.length, check.Equals, len(sel)+128)
	c.Assert(col.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(col.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(col.GetInt64(len(sel)+i), check.Equals, int64(i*i*i))
			c.Assert(col.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestReconstructVarLen(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeVarString), 1024)
	results := make([]string, 0, 1024)
	nulls := make([]bool, 0, 1024)
	sel := make([]int, 0, 1024)
	for i := 0; i < 1024; i++ {
		if rand.Intn(10) < 6 {
			sel = append(sel, i)
		}

		if rand.Intn(10) < 2 {
			col.AppendNull()
			nulls = append(nulls, true)
			results = append(results, "")
			continue
		}

		v := fmt.Sprintf("%v", rand.Int63())
		col.AppendString(v)
		results = append(results, v)
		nulls = append(nulls, false)
	}

	col.reconstruct(sel)
	nullCnt := 0
	for n, i := range sel {
		if nulls[i] {
			nullCnt++
			c.Assert(col.IsNull(n), check.Equals, true)
		} else {
			c.Assert(col.GetString(n), check.Equals, results[i])
		}
	}
	c.Assert(nullCnt, check.Equals, col.nullCount())
	c.Assert(col.length, check.Equals, len(sel))

	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			col.AppendNull()
		} else {
			col.AppendString(fmt.Sprintf("%v", i*i*i))
		}
	}

	c.Assert(col.length, check.Equals, len(sel)+128)
	c.Assert(col.nullCount(), check.Equals, nullCnt+128/2)
	for i := 0; i < 128; i++ {
		if i%2 == 0 {
			c.Assert(col.IsNull(len(sel)+i), check.Equals, true)
		} else {
			c.Assert(col.GetString(len(sel)+i), check.Equals, fmt.Sprintf("%v", i*i*i))
			c.Assert(col.IsNull(len(sel)+i), check.Equals, false)
		}
	}
}

func (s *testChunkSuite) TestPreAllocInt64(c *check.C) {
	col := NewColumn(types.NewFieldType(mysql.TypeLonglong), 128)
	col.ResizeInt64(256)
	i64s := col.Int64s()
	c.Assert(len(i64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(col.IsNull(i), check.Equals, true)
	}
	col.AppendInt64(2333)
	c.Assert(col.IsNull(256), check.Equals, false)
	c.Assert(len(col.Int64s()), check.Equals, 257)
	c.Assert(col.Int64s()[256], check.Equals, int64(2333))
}

func (s *testChunkSuite) TestPreAllocUint64(c *check.C) {
	tll := types.NewFieldType(mysql.TypeLonglong)
	tll.Flag |= mysql.UnsignedFlag
	col := NewColumn(tll, 128)
	col.ResizeUint64(256)
	u64s := col.Uint64s()
	c.Assert(len(u64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(col.IsNull(i), check.Equals, true)
	}
	col.AppendUint64(2333)
	c.Assert(col.IsNull(256), check.Equals, false)
	c.Assert(len(col.Uint64s()), check.Equals, 257)
	c.Assert(col.Uint64s()[256], check.Equals, uint64(2333))
}

func (s *testChunkSuite) TestPreAllocFloat32(c *check.C) {
	col := newFixedLenColumn(sizeFloat32, 128)
	col.ResizeFloat32(256)
	f32s := col.Float32s()
	c.Assert(len(f32s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(col.IsNull(i), check.Equals, true)
	}
	col.AppendFloat32(2333)
	c.Assert(col.IsNull(256), check.Equals, false)
	c.Assert(len(col.Float32s()), check.Equals, 257)
	c.Assert(col.Float32s()[256], check.Equals, float32(2333))
}

func (s *testChunkSuite) TestPreAllocFloat64(c *check.C) {
	col := newFixedLenColumn(sizeFloat64, 128)
	col.ResizeFloat64(256)
	f64s := col.Float64s()
	c.Assert(len(f64s), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(col.IsNull(i), check.Equals, true)
	}
	col.AppendFloat64(2333)
	c.Assert(col.IsNull(256), check.Equals, false)
	c.Assert(len(col.Float64s()), check.Equals, 257)
	c.Assert(col.Float64s()[256], check.Equals, float64(2333))
}

func (s *testChunkSuite) TestPreAllocDecimal(c *check.C) {
	col := newFixedLenColumn(sizeMyDecimal, 128)
	col.ResizeDecimal(256)
	ds := col.Decimals()
	c.Assert(len(ds), check.Equals, 256)
	for i := 0; i < 256; i++ {
		c.Assert(col.IsNull(i), check.Equals, true)
	}
	col.AppendMyDecimal(new(types.MyDecimal))
	c.Assert(col.IsNull(256), check.Equals, false)
	c.Assert(len(col.Float64s()), check.Equals, 257)
}

func (s *testChunkSuite) TestNull(c *check.C) {
	col := newFixedLenColumn(sizeFloat64, 32)
	col.ResizeFloat64(1024)
	c.Assert(col.nullCount(), check.Equals, 1024)

	notNulls := make(map[int]struct{})
	for i := 0; i < 512; i++ {
		idx := rand.Intn(1024)
		notNulls[idx] = struct{}{}
		col.SetNull(idx, false)
	}

	c.Assert(col.nullCount(), check.Equals, 1024-len(notNulls))
	for idx := range notNulls {
		c.Assert(col.IsNull(idx), check.Equals, false)
	}

	col.ResizeFloat64(8)
	col.SetNulls(0, 8, true)
	col.SetNull(7, false)
	c.Assert(col.nullCount(), check.Equals, 7)

	col.ResizeFloat64(8)
	col.SetNulls(0, 8, true)
	c.Assert(col.nullCount(), check.Equals, 8)

	col.ResizeFloat64(9)
	col.SetNulls(0, 9, true)
	col.SetNull(8, false)
	c.Assert(col.nullCount(), check.Equals, 8)
}

func (s *testChunkSuite) TestSetNulls(c *check.C) {
	col := newFixedLenColumn(sizeFloat64, 32)
	col.ResizeFloat64(1024)
	c.Assert(col.nullCount(), check.Equals, 1024)

	col.SetNulls(0, 1024, false)
	c.Assert(col.nullCount(), check.Equals, 0)

	nullMap := make(map[int]struct{})
	for i := 0; i < 100; i++ {
		begin := rand.Intn(1024)
		l := rand.Intn(37)
		end := begin + l
		if end > 1024 {
			end = 1024
		}
		for i := begin; i < end; i++ {
			nullMap[i] = struct{}{}
		}
		col.SetNulls(begin, end, true)

		c.Assert(col.nullCount(), check.Equals, len(nullMap))
		for k := range nullMap {
			c.Assert(col.IsNull(k), check.Equals, true)
		}
	}
}

func (s *testChunkSuite) TestResizeReserve(c *check.C) {
	cI64s := newFixedLenColumn(sizeInt64, 0)
	c.Assert(cI64s.length, check.Equals, 0)
	for i := 0; i < 100; i++ {
		t := rand.Intn(1024)
		cI64s.ResizeInt64(t)
		c.Assert(cI64s.length, check.Equals, t)
		c.Assert(len(cI64s.Int64s()), check.Equals, t)
	}
	cI64s.ResizeInt64(0)
	c.Assert(cI64s.length, check.Equals, 0)
	c.Assert(len(cI64s.Int64s()), check.Equals, 0)

	cStrs := newVarLenColumn(0, nil)
	for i := 0; i < 100; i++ {
		t := rand.Intn(1024)
		cStrs.ReserveString(t)
		c.Assert(cStrs.length, check.Equals, 0)
	}
	cStrs.ReserveString(0)
	c.Assert(cStrs.length, check.Equals, 0)
}

func BenchmarkDurationRow(b *testing.B) {
	chk1 := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDuration)}, 1024)
	col1 := chk1.Column(0)
	for i := 0; i < 1024; i++ {
		col1.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}
	chk2 := chk1.CopyConstruct()
	result := chk1.CopyConstruct()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.Reset()
		it1 := NewIterator4Chunk(chk1)
		it2 := NewIterator4Chunk(chk2)
		for r1, r2 := it1.Begin(), it2.Begin(); r1 != it1.End() && r2 != it2.End(); r1, r2 = it1.Next(), it2.Next() {
			d1 := r1.GetDuration(0, 0)
			d2 := r2.GetDuration(0, 0)
			r, err := d1.Add(d2)
			if err != nil {
				b.Fatal(err)
			}
			result.AppendDuration(0, r)
		}
	}
}

func BenchmarkDurationVec(b *testing.B) {
	chk := NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDuration)}, 1024)
	col1 := chk.Column(0)
	for i := 0; i < 1024; i++ {
		col1.AppendDuration(types.Duration{Duration: time.Second * time.Duration(i)})
	}
	col2 := col1.CopyConstruct(nil)
	result := col1.CopyConstruct(nil)

	ds1 := col1.GoDurations()
	ds2 := col2.GoDurations()
	rs := result.GoDurations()

	b.ResetTimer()
	for k := 0; k < b.N; k++ {
		result.ResizeDuration(1024)
		for i := 0; i < 1024; i++ {
			d1 := types.Duration{Duration: ds1[i]}
			d2 := types.Duration{Duration: ds2[i]}
			r, err := d1.Add(d2)
			if err != nil {
				b.Fatal(err)
			}
			rs[i] = r.Duration
		}
	}
}
