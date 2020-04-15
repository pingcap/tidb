// Copyright 2018 PingCAP, Inc.
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
	"runtime"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/types/json"
)

var _ = check.Suite(&testCodecSuite{})

type testCodecSuite struct{}

func (s *testCodecSuite) TestCodec(c *check.C) {
	if runtime.Version() >= "go1.14" {
		// TODO: fix https://github.com/pingcap/tidb/v4/issues/15154
		c.Skip("cannot pass checkptr, TODO to fix https://github.com/pingcap/tidb/v4/issues/15154")
	}
	numCols := 6
	numRows := 10

	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeNewDecimal})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeJSON})

	oldChk := NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		oldChk.AppendNull(0)
		oldChk.AppendInt64(1, int64(i))
		oldChk.AppendString(2, str)
		oldChk.AppendString(3, str)
		oldChk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		oldChk.AppendJSON(5, json.CreateBinary(str))
	}

	codec := NewCodec(colTypes)
	buffer := codec.Encode(oldChk)

	newChk := NewChunkWithCapacity(colTypes, numRows)
	remained := codec.DecodeToChunk(buffer, newChk)

	c.Assert(len(remained), check.Equals, 0)
	c.Assert(newChk.NumCols(), check.Equals, numCols)
	c.Assert(newChk.NumRows(), check.Equals, numRows)
	for i := 0; i < numRows; i++ {
		row := newChk.GetRow(i)
		str := fmt.Sprintf("%d.12345", i)
		c.Assert(row.IsNull(0), check.IsTrue)
		c.Assert(row.IsNull(1), check.IsFalse)
		c.Assert(row.IsNull(2), check.IsFalse)
		c.Assert(row.IsNull(3), check.IsFalse)
		c.Assert(row.IsNull(4), check.IsFalse)
		c.Assert(row.IsNull(5), check.IsFalse)

		c.Assert(row.GetInt64(1), check.Equals, int64(i))
		c.Assert(row.GetString(2), check.Equals, str)
		c.Assert(row.GetString(3), check.Equals, str)
		c.Assert(row.GetMyDecimal(4).String(), check.Equals, str)
		c.Assert(string(row.GetJSON(5).GetString()), check.Equals, str)
	}
}

func (s *testCodecSuite) TestEstimateTypeWidth(c *check.C) {
	var colType *types.FieldType

	colType = &types.FieldType{Tp: mysql.TypeLonglong}
	c.Assert(EstimateTypeWidth(colType), check.Equals, 8) // fixed-witch type

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 31}
	c.Assert(EstimateTypeWidth(colType), check.Equals, 31) // colLen <= 32

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 999}
	c.Assert(EstimateTypeWidth(colType), check.Equals, 515) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 2000}
	c.Assert(EstimateTypeWidth(colType), check.Equals, 516) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString}
	c.Assert(EstimateTypeWidth(colType), check.Equals, 32) // value after guessing
}

func BenchmarkEncodeChunk(b *testing.B) {
	numCols := 4
	numRows := 1024

	chk := &Chunk{columns: make([]*Column, numCols)}
	for i := 0; i < numCols; i++ {
		chk.columns[i] = &Column{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
		}
	}

	codec := &Codec{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Encode(chk)
	}
}

func BenchmarkDecode(b *testing.B) {
	numCols := 4
	numRows := 1024

	colTypes := make([]*types.FieldType, numCols)
	chk := &Chunk{columns: make([]*Column, numCols)}
	for i := 0; i < numCols; i++ {
		chk.columns[i] = &Column{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
		}
		colTypes[i] = &types.FieldType{
			Tp: mysql.TypeLonglong,
		}
	}
	codec := &Codec{colTypes}
	buffer := codec.Encode(chk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.Decode(buffer)
	}
}

func BenchmarkDecodeToChunk(b *testing.B) {
	numCols := 4
	numRows := 1024

	colTypes := make([]*types.FieldType, numCols)
	chk := &Chunk{
		columns: make([]*Column, numCols),
	}
	for i := 0; i < numCols; i++ {
		chk.columns[i] = &Column{
			length:     numRows,
			nullBitmap: make([]byte, numRows/8+1),
			data:       make([]byte, numRows*8),
			elemBuf:    make([]byte, 8),
		}
		colTypes[i] = &types.FieldType{
			Tp: mysql.TypeLonglong,
		}
	}
	codec := &Codec{colTypes}
	buffer := codec.Encode(chk)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.DecodeToChunk(buffer, chk)
	}
}

func BenchmarkDecodeToChunkWithVariableType(b *testing.B) {
	numCols := 6
	numRows := 1024

	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeLonglong})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeVarchar})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeNewDecimal})
	colTypes = append(colTypes, &types.FieldType{Tp: mysql.TypeJSON})

	chk := NewChunkWithCapacity(colTypes, numRows)
	for i := 0; i < numRows; i++ {
		str := fmt.Sprintf("%d.12345", i)
		chk.AppendNull(0)
		chk.AppendInt64(1, int64(i))
		chk.AppendString(2, str)
		chk.AppendString(3, str)
		chk.AppendMyDecimal(4, types.NewDecFromStringForTest(str))
		chk.AppendJSON(5, json.CreateBinary(str))
	}
	codec := &Codec{colTypes}
	buffer := codec.Encode(chk)

	chk.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		codec.DecodeToChunk(buffer, chk)
	}
}
