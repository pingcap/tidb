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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestColumn(t *testing.T) {
	ctx := mock.NewContext()
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), UniqueID: 1}

	require.True(t, col.Equal(nil, col))
	require.False(t, col.Equal(nil, &Column{}))
	require.False(t, col.IsCorrelated())
	require.True(t, col.Equal(nil, col.Decorrelate(nil)))

	marshal, err := col.MarshalJSON()
	require.NoError(t, err)
	require.EqualValues(t, []byte{0x22, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x23, 0x31, 0x22}, marshal)

	intDatum := types.NewIntDatum(1)
	corCol := &CorrelatedColumn{Column: *col, Data: &intDatum}
	invalidCorCol := &CorrelatedColumn{Column: Column{}}
	schema := NewSchema(&Column{UniqueID: 1})
	require.True(t, corCol.Equal(nil, corCol))
	require.False(t, corCol.Equal(nil, invalidCorCol))
	require.True(t, corCol.IsCorrelated())
	require.False(t, corCol.ConstItem(nil))
	require.True(t, corCol.Decorrelate(schema).Equal(nil, col))
	require.True(t, invalidCorCol.Decorrelate(schema).Equal(nil, invalidCorCol))

	intCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data: &intDatum}
	intVal, isNull, err := intCorCol.EvalInt(ctx, chunk.Row{})
	require.Equal(t, int64(1), intVal)
	require.False(t, isNull)
	require.NoError(t, err)

	realDatum := types.NewFloat64Datum(1.2)
	realCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDouble)},
		Data: &realDatum}
	realVal, isNull, err := realCorCol.EvalReal(ctx, chunk.Row{})
	require.Equal(t, float64(1.2), realVal)
	require.False(t, isNull)
	require.NoError(t, err)

	decimalDatum := types.NewDecimalDatum(types.NewDecFromStringForTest("1.2"))
	decimalCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)},
		Data: &decimalDatum}
	decVal, isNull, err := decimalCorCol.EvalDecimal(ctx, chunk.Row{})
	require.Zero(t, decVal.Compare(types.NewDecFromStringForTest("1.2")))
	require.False(t, isNull)
	require.NoError(t, err)

	stringDatum := types.NewStringDatum("abc")
	stringCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeVarchar)},
		Data: &stringDatum}
	strVal, isNull, err := stringCorCol.EvalString(ctx, chunk.Row{})
	require.Equal(t, "abc", strVal)
	require.False(t, isNull)
	require.NoError(t, err)

	durationCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDuration)},
		Data: &durationDatum}
	durationVal, isNull, err := durationCorCol.EvalDuration(ctx, chunk.Row{})
	require.Zero(t, durationVal.Compare(duration))
	require.False(t, isNull)
	require.NoError(t, err)

	timeDatum := types.NewTimeDatum(tm)
	timeCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDatetime)},
		Data: &timeDatum}
	timeVal, isNull, err := timeCorCol.EvalTime(ctx, chunk.Row{})
	require.Zero(t, timeVal.Compare(tm))
	require.False(t, isNull)
	require.NoError(t, err)
}

func TestColumnHashCode(t *testing.T) {
	col1 := &Column{
		UniqueID: 12,
	}
	require.EqualValues(t, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc}, col1.HashCode(nil))

	col2 := &Column{
		UniqueID: 2,
	}
	require.EqualValues(t, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2}, col2.HashCode(nil))
}

func TestColumn2Expr(t *testing.T) {
	cols := make([]*Column, 0, 5)
	for i := 0; i < 5; i++ {
		cols = append(cols, &Column{UniqueID: int64(i)})
	}

	exprs := Column2Exprs(cols)
	for i := range exprs {
		require.True(t, exprs[i].Equal(nil, cols[i]))
	}
}

func TestColInfo2Col(t *testing.T) {
	col0, col1 := &Column{ID: 0}, &Column{ID: 1}
	cols := []*Column{col0, col1}
	colInfo := &model.ColumnInfo{ID: 0}
	res := ColInfo2Col(cols, colInfo)
	require.True(t, res.Equal(nil, col1))

	colInfo.ID = 3
	res = ColInfo2Col(cols, colInfo)
	require.Nil(t, res)
}

func TestIndexInfo2Cols(t *testing.T) {
	col0 := &Column{UniqueID: 0, ID: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &Column{UniqueID: 1, ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colInfo0 := &model.ColumnInfo{ID: 0, Name: model.NewCIStr("0")}
	colInfo1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("1")}
	indexCol0, indexCol1 := &model.IndexColumn{Name: model.NewCIStr("0")}, &model.IndexColumn{Name: model.NewCIStr("1")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1}}

	cols := []*Column{col0}
	colInfos := []*model.ColumnInfo{colInfo0}
	resCols, lengths := IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 1)
	require.Len(t, lengths, 1)
	require.True(t, resCols[0].Equal(nil, col0))

	cols = []*Column{col1}
	colInfos = []*model.ColumnInfo{colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 0)
	require.Len(t, lengths, 0)

	cols = []*Column{col0, col1}
	colInfos = []*model.ColumnInfo{colInfo0, colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 2)
	require.Len(t, lengths, 2)
	require.True(t, resCols[0].Equal(nil, col0))
	require.True(t, resCols[1].Equal(nil, col1))
}

func TestColHybird(t *testing.T) {
	ctx := mock.NewContext()

	// bit
	ft := types.NewFieldType(mysql.TypeBit)
	col := &Column{RetType: ft, Index: 0}
	input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		num, err := types.ParseBitStr(fmt.Sprintf("0b%b", i))
		require.NoError(t, err)
		input.AppendBytes(0, num)
	}
	result := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), 1024)
	require.Nil(t, col.VecEvalInt(ctx, input, result))

	it := chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalInt(ctx, row)
		require.NoError(t, err)
		require.Equal(t, result.GetInt64(i), v)
	}

	// use a container which has the different field type with bit
	result = chunk.NewColumn(types.NewFieldType(mysql.TypeString), 1024)
	require.Nil(t, col.VecEvalInt(ctx, input, result))
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalInt(ctx, row)
		require.NoError(t, err)
		require.Equal(t, result.GetInt64(i), v)
	}

	// enum
	ft = types.NewFieldType(mysql.TypeEnum)
	col.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendEnum(0, types.Enum{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result = chunk.NewColumn(types.NewFieldType(mysql.TypeString), 1024)
	require.Nil(t, col.VecEvalString(ctx, input, result))

	it = chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalString(ctx, row)
		require.NoError(t, err)
		require.Equal(t, result.GetString(i), v)
	}

	// set
	ft = types.NewFieldType(mysql.TypeSet)
	col.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendSet(0, types.Set{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result = chunk.NewColumn(types.NewFieldType(mysql.TypeString), 1024)
	require.Nil(t, col.VecEvalString(ctx, input, result))

	it = chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalString(ctx, row)
		require.NoError(t, err)
		require.Equal(t, result.GetString(i), v)
	}
}
