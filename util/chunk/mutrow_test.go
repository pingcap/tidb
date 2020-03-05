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
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testChunkSuite) TestMutRow(c *check.C) {
	mutRow := MutRowFromTypes(allTypes)
	row := mutRow.ToRow()
	sc := new(stmtctx.StatementContext)
	for i := 0; i < row.Len(); i++ {
		val := zeroValForType(allTypes[i])
		d := row.GetDatum(i, allTypes[i])
		d2 := types.NewDatum(val)
		cmp, err := d.CompareDatum(sc, &d2)
		c.Assert(err, check.IsNil)
		c.Assert(cmp, check.Equals, 0)
	}

	mutRow = MutRowFromValues("abc", 123)
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(mutRow.ToRow().GetString(0), check.Equals, "abc")
	c.Assert(row.IsNull(1), check.IsFalse)
	c.Assert(mutRow.ToRow().GetInt64(1), check.Equals, int64(123))
	mutRow.SetValues("abcd", 456)
	row = mutRow.ToRow()
	c.Assert(row.GetString(0), check.Equals, "abcd")
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.GetInt64(1), check.Equals, int64(456))
	c.Assert(row.IsNull(1), check.IsFalse)
	mutRow.SetDatums(types.NewStringDatum("defgh"), types.NewIntDatum(33))
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.GetString(0), check.Equals, "defgh")
	c.Assert(row.IsNull(1), check.IsFalse)
	c.Assert(row.GetInt64(1), check.Equals, int64(33))

	mutRow.SetRow(MutRowFromValues("foobar", nil).ToRow())
	row = mutRow.ToRow()
	c.Assert(row.IsNull(0), check.IsFalse)
	c.Assert(row.IsNull(1), check.IsTrue)

	nRow := MutRowFromValues(nil, 111).ToRow()
	c.Assert(nRow.IsNull(0), check.IsTrue)
	c.Assert(nRow.IsNull(1), check.IsFalse)
	mutRow.SetRow(nRow)
	row = mutRow.ToRow()
	c.Assert(row.IsNull(0), check.IsTrue)
	c.Assert(row.IsNull(1), check.IsFalse)

	j, err := json.ParseBinaryFromString("true")
	t := types.NewTime(types.FromDate(2000, 1, 1, 1, 0, 0, 0), mysql.TypeDatetime, types.MaxFsp)
	c.Assert(err, check.IsNil)
	mutRow = MutRowFromValues(j, t)
	row = mutRow.ToRow()
	c.Assert(row.GetJSON(0), check.DeepEquals, j)
	c.Assert(row.GetTime(1), check.DeepEquals, t)

	retTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDuration)}
	chk := New(retTypes, 1, 1)
	dur, err := types.ParseDuration(sc, "01:23:45", 0)
	c.Assert(err, check.IsNil)
	chk.AppendDuration(0, dur)
	mutRow = MutRowFromTypes(retTypes)
	mutRow.SetValue(0, dur)
	c.Assert(chk.columns[0].data, check.BytesEquals, mutRow.c.columns[0].data)
	mutRow.SetDatum(0, types.NewDurationDatum(dur))
	c.Assert(chk.columns[0].data, check.BytesEquals, mutRow.c.columns[0].data)
}

func BenchmarkMutRowSetRow(b *testing.B) {
	b.ReportAllocs()
	rowChk := newChunk(8, 0)
	rowChk.AppendInt64(0, 1)
	rowChk.AppendString(1, "abcd")
	row := rowChk.GetRow(0)
	mutRow := MutRowFromValues(1, "abcd")
	for i := 0; i < b.N; i++ {
		mutRow.SetRow(row)
	}
}

func BenchmarkMutRowSetDatums(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	datums := []types.Datum{types.NewDatum(1), types.NewDatum("abcd")}
	for i := 0; i < b.N; i++ {
		mutRow.SetDatums(datums...)
	}
}

func BenchmarkMutRowSetValues(b *testing.B) {
	b.ReportAllocs()
	mutRow := MutRowFromValues(1, "abcd")
	for i := 0; i < b.N; i++ {
		mutRow.SetValues(1, "abcd")
	}
}

func BenchmarkMutRowFromTypes(b *testing.B) {
	b.ReportAllocs()
	tps := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarchar),
	}
	for i := 0; i < b.N; i++ {
		MutRowFromTypes(tps)
	}
}

func BenchmarkMutRowFromDatums(b *testing.B) {
	b.ReportAllocs()
	datums := []types.Datum{types.NewDatum(1), types.NewDatum("abc")}
	for i := 0; i < b.N; i++ {
		MutRowFromDatums(datums)
	}
}

func BenchmarkMutRowFromValues(b *testing.B) {
	b.ReportAllocs()
	values := []interface{}{1, "abc"}
	for i := 0; i < b.N; i++ {
		MutRowFromValues(values)
	}
}

func (s *testChunkSuite) TestMutRowShallowCopyPartialRow(c *check.C) {
	colTypes := make([]*types.FieldType, 0, 3)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeTimestamp})

	mutRow := MutRowFromTypes(colTypes)
	row := MutRowFromValues("abc", 123, types.ZeroTimestamp).ToRow()
	mutRow.ShallowCopyPartialRow(0, row)
	c.Assert(row.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(row.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))
	c.Assert(row.GetTime(2), check.DeepEquals, mutRow.ToRow().GetTime(2))

	row.c.Reset()
	d := types.NewStringDatum("dfg")
	row.c.AppendDatum(0, &d)
	d = types.NewIntDatum(567)
	row.c.AppendDatum(1, &d)
	d = types.NewTimeDatum(types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6))
	row.c.AppendDatum(2, &d)

	c.Assert(d.GetMysqlTime(), check.DeepEquals, mutRow.ToRow().GetTime(2))
	c.Assert(row.GetString(0), check.Equals, mutRow.ToRow().GetString(0))
	c.Assert(row.GetInt64(1), check.Equals, mutRow.ToRow().GetInt64(1))
	c.Assert(row.GetTime(2), check.DeepEquals, mutRow.ToRow().GetTime(2))
}

var rowsNum = 1024

func BenchmarkMutRowShallowCopyPartialRow(b *testing.B) {
	b.ReportAllocs()
	colTypes := make([]*types.FieldType, 0, 8)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarString})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeDatetime})

	mutRow := MutRowFromTypes(colTypes)
	row := MutRowFromValues("abc", "abcdefg", 123, 456, types.ZeroDatetime).ToRow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < rowsNum; j++ {
			mutRow.ShallowCopyPartialRow(0, row)
		}
	}
}

func BenchmarkChunkAppendPartialRow(b *testing.B) {
	b.ReportAllocs()
	chk := newChunkWithInitCap(rowsNum, 0, 0, 8, 8, sizeTime)
	row := MutRowFromValues("abc", "abcdefg", 123, 456, types.ZeroDatetime).ToRow()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chk.Reset()
		for j := 0; j < rowsNum; j++ {
			chk.AppendPartialRow(0, row)
		}
	}
}
