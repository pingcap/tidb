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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunk

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/stretchr/testify/require"
)

func TestCodec(t *testing.T) {
	numCols := 6
	numRows := 10

	colTypes := make([]*types.FieldType, 0, numCols)
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeLonglong))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeLonglong))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeVarchar))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeVarchar))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeNewDecimal))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeJSON))

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

	require.Empty(t, remained)
	require.Equal(t, numCols, newChk.NumCols())
	require.Equal(t, numRows, newChk.NumRows())
	for i := 0; i < numRows; i++ {
		row := newChk.GetRow(i)
		str := fmt.Sprintf("%d.12345", i)
		require.True(t, row.IsNull(0))
		require.False(t, row.IsNull(1))
		require.False(t, row.IsNull(2))
		require.False(t, row.IsNull(3))
		require.False(t, row.IsNull(4))
		require.False(t, row.IsNull(5))

		require.Equal(t, int64(i), row.GetInt64(1))
		require.Equal(t, str, row.GetString(2))
		require.Equal(t, str, row.GetString(3))
		require.Equal(t, str, row.GetMyDecimal(4).String())
		require.Equal(t, str, string(row.GetJSON(5).GetString()))
	}
}

func TestEstimateTypeWidth(t *testing.T) {
	var colType *types.FieldType

	colType = types.NewFieldType(mysql.TypeLonglong)
	require.Equal(t, 8, EstimateTypeWidth(colType)) // fixed-witch type

	colType = types.NewFieldType(mysql.TypeString)
	colType.SetFlen(31)
	require.Equal(t, 31, EstimateTypeWidth(colType)) // colLen <= 32

	colType = types.NewFieldType(mysql.TypeString)
	colType.SetFlen(999)
	require.Equal(t, 515, EstimateTypeWidth(colType)) // colLen < 1000

	colType = types.NewFieldType(mysql.TypeString)
	colType.SetFlen(2000)
	require.Equal(t, 516, EstimateTypeWidth(colType)) // colLen < 1000

	colType = types.NewFieldType(mysql.TypeString)
	require.Equal(t, 32, EstimateTypeWidth(colType)) // value after guessing
}

func BenchmarkEncodeChunk(b *testing.B) {
	numCols := 4
	numRows := 1024

	colTypes := make([]*types.FieldType, numCols)
	for i := 0; i < numCols; i++ {
		colTypes[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	chk := NewChunkWithCapacity(colTypes, numRows)

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
	for i := 0; i < numCols; i++ {
		colTypes[i] = types.NewFieldType(mysql.TypeLonglong)
	}
	chk := NewChunkWithCapacity(colTypes, numRows)
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
		colTypes[i] = types.NewFieldType(mysql.TypeLonglong)
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
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeLonglong))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeLonglong))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeVarchar))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeVarchar))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeNewDecimal))
	colTypes = append(colTypes, types.NewFieldType(mysql.TypeJSON))

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
