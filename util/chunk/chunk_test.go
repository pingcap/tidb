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
	"fmt"
	"math"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/util/types/json"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testChunkSuite{})

type testChunkSuite struct{}

func (s *testChunkSuite) TestChunk(c *C) {
	numCols := 6
	numRows := 10
	chk := NewChunk(numCols)
	strFmt := "%d.12345"
	for i := 0; i < numRows; i++ {
		chk.AppendNull()
		chk.AppendInt64(int64(i))
		str := fmt.Sprintf(strFmt, i)
		chk.AppendString(str)
		chk.AppendBytes([]byte(str))
		chk.AppendMyDecimal(types.NewDecFromStringForTest(str))
		chk.AppendJSON(json.CreateJSON(str))
	}
	c.Assert(chk.NumCols(), Equals, numCols)
	c.Assert(chk.NumRows(), Equals, numRows)
	c.Assert(chk.DataSize(), Equals, len(chk.data))
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		intV, isNull := row.GetInt64(0)
		c.Assert(intV, Equals, int64(0))
		c.Assert(isNull, IsTrue)
		intV, isNull = row.GetInt64(1)
		c.Assert(intV, Equals, int64(i))
		str := fmt.Sprintf(strFmt, i)
		strV, isNull := row.GetString(2)
		c.Assert(isNull, IsFalse)
		c.Assert(strV, Equals, str)
		bytesV, isNull := row.GetBytes(3)
		c.Assert(isNull, IsFalse)
		c.Assert(bytesV, BytesEquals, []byte(str))
		decV, isNull := row.GetMyDecimal(4)
		c.Assert(isNull, IsFalse)
		c.Assert(decV.String(), Equals, str)
		jsonV, isNull := row.GetJSON(5)
		c.Assert(isNull, IsFalse)
		c.Assert(jsonV.Str, Equals, str)
	}

	chk2 := NewChunk(numCols)
	for i := 0; i < numRows; i++ {
		row := chk.GetRow(i)
		chk2.AppendRow(row)
	}
	c.Assert(chk2.offsets, DeepEquals, chk.offsets)
	c.Assert(chk2.data, BytesEquals, chk.data)
	c.Assert(len(chk2.objMap), Equals, len(chk.objMap))

	chk.Reset(6)
	f32Val := float32(1.2)
	chk.AppendFloat32(f32Val)
	f64Val := 1.3
	chk.AppendFloat64(f64Val)
	tVal := types.TimeFromDays(1)
	chk.AppendTime(tVal)
	durVal := types.Duration{Duration: time.Hour, Fsp: 6}
	chk.AppendDuration(durVal)
	enumVal := types.Enum{Name: "abc", Value: 100}
	chk.AppendEnum(enumVal)
	setVal := types.Set{Name: "def", Value: 101}
	chk.AppendSet(setVal)

	row := chk.GetRow(0)
	f32, _ := row.GetFloat32(0)
	c.Assert(f32, Equals, f32Val)
	f64, _ := row.GetFloat64(1)
	c.Assert(f64, Equals, f64Val)
	t, _ := row.GetTime(2)
	c.Assert(t.Compare(tVal), Equals, 0)
	dur, _ := row.GetDuration(3)
	c.Assert(dur, DeepEquals, durVal)
	enum, _ := row.GetEnum(4)
	c.Assert(enum, DeepEquals, enumVal)
	set, _ := row.GetSet(5)
	c.Assert(set, DeepEquals, setVal)
}

func BenchmarkAppendInt(b *testing.B) {
	b.ReportAllocs()
	chk := NewChunk(1)
	for i := 0; i < b.N; i++ {
		appendInt(chk)
	}
}

func appendInt(chk *Chunk) {
	resetChunk(chk)
	for i := 0; i < 1000; i++ {
		chk.AppendInt64(int64(i))
	}
}

func BenchmarkAppendString(b *testing.B) {
	b.ReportAllocs()
	chk := NewChunk(1)
	for i := 0; i < b.N; i++ {
		appendString(chk)
	}
}

func appendString(chk *Chunk) {
	resetChunk(chk)
	for i := 0; i < 1000; i++ {
		chk.AppendString("abcd")
	}
}

func BenchmarkAppendRow(b *testing.B) {
	b.ReportAllocs()
	rowChk := NewChunk(4)
	rowChk.AppendNull()
	rowChk.AppendInt64(1)
	rowChk.AppendString("abcd")
	rowChk.AppendBytes([]byte("abcd"))

	chk := NewChunk(4)
	for i := 0; i < b.N; i++ {
		appendRow(chk, rowChk.GetRow(0))
	}
}

func appendRow(chk *Chunk, row Row) {
	resetChunk(chk)
	for i := 0; i < 1000; i++ {
		chk.AppendRow(row)
	}
}

func resetChunk(chk *Chunk) {
	chk.offsets = chk.offsets[:0]
	chk.data = chk.data[:0]
}

func BenchmarkAccess(b *testing.B) {
	rowChk := NewChunk(4)
	rowChk.AppendNull()
	rowChk.AppendInt64(math.MaxUint16)
	row := rowChk.GetRow(0)
	var sum int64
	for i := 0; i < b.N; i++ {
		v, _ := row.GetInt64(1)
		sum += v
	}
	fmt.Println(sum)
}
