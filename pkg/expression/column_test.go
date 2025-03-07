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

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestColumn(t *testing.T) {
	ctx := mock.NewContext()
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), UniqueID: 1}

	require.True(t, col.EqualColumn(col))
	require.False(t, col.EqualColumn(&Column{}))
	require.False(t, col.IsCorrelated())
	require.True(t, col.EqualColumn(col.Decorrelate(nil)))

	intDatum := types.NewIntDatum(1)
	corCol := &CorrelatedColumn{Column: *col, Data: &intDatum}
	invalidCorCol := &CorrelatedColumn{Column: Column{}}
	schema := NewSchema(&Column{UniqueID: 1})
	require.True(t, corCol.EqualColumn(corCol))
	require.False(t, corCol.EqualColumn(invalidCorCol))
	require.True(t, corCol.IsCorrelated())
	require.Equal(t, ConstNone, corCol.ConstLevel())
	require.True(t, col.EqualColumn(corCol.Decorrelate(schema)))
	require.True(t, invalidCorCol.EqualColumn(invalidCorCol.Decorrelate(schema)))

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
	require.EqualValues(t, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc}, col1.HashCode())

	col2 := &Column{
		UniqueID: 2,
	}
	require.EqualValues(t, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2}, col2.HashCode())
}

func TestColumn2Expr(t *testing.T) {
	cols := make([]*Column, 0, 5)
	for i := 0; i < 5; i++ {
		cols = append(cols, &Column{UniqueID: int64(i)})
	}

	exprs := Column2Exprs(cols)
	for i := range exprs {
		require.True(t, cols[i].EqualColumn(exprs[i]))
	}
}

func TestColInfo2Col(t *testing.T) {
	col0, col1 := &Column{ID: 0}, &Column{ID: 1}
	cols := []*Column{col0, col1}
	colInfo := &model.ColumnInfo{ID: 0}
	res := ColInfo2Col(cols, colInfo)
	require.True(t, res.EqualColumn(col1))

	colInfo.ID = 3
	res = ColInfo2Col(cols, colInfo)
	require.Nil(t, res)
}

func TestIndexInfo2Cols(t *testing.T) {
	col0 := &Column{UniqueID: 0, ID: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &Column{UniqueID: 1, ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colInfo0 := &model.ColumnInfo{ID: 0, Name: ast.NewCIStr("0")}
	colInfo1 := &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("1")}
	indexCol0, indexCol1 := &model.IndexColumn{Name: ast.NewCIStr("0")}, &model.IndexColumn{Name: ast.NewCIStr("1")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1}}

	cols := []*Column{col0}
	colInfos := []*model.ColumnInfo{colInfo0}
	resCols, lengths := IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	require.Len(t, resCols, 1)
	require.Len(t, lengths, 1)
	require.True(t, resCols[0].EqualColumn(col0))

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
	require.True(t, resCols[0].EqualColumn(col0))
	require.True(t, resCols[1].EqualColumn(col1))
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

func TestInColumnArray(t *testing.T) {
	// normal case, col is in column array
	col0, col1 := &Column{ID: 0, UniqueID: 0}, &Column{ID: 1, UniqueID: 1}
	cols := []*Column{col0, col1}
	require.True(t, col0.InColumnArray(cols))

	// abnormal case, col is not in column array
	require.False(t, col0.InColumnArray([]*Column{col1}))

	// abnormal case, input is nil
	require.False(t, col0.InColumnArray(nil))
}

func TestGcColumnExprIsTidbShard(t *testing.T) {
	ctx := mock.NewContext()

	// abnormal case
	// nil, not tidb_shard
	require.False(t, GcColumnExprIsTidbShard(nil))

	// `a = 1`, not tidb_shard
	ft := types.NewFieldType(mysql.TypeLonglong)
	col := &Column{RetType: ft, Index: 0}
	d1 := types.NewDatum(1)
	con := &Constant{Value: d1, RetType: ft}
	expr := NewFunctionInternal(ctx, ast.EQ, ft, col, con)
	require.False(t, GcColumnExprIsTidbShard(expr))

	// normal case
	// tidb_shard(a) = 1
	shardExpr := NewFunctionInternal(ctx, ast.TiDBShard, ft, col)
	require.True(t, GcColumnExprIsTidbShard(shardExpr))
}

func TestFieldTypeHashEquals(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeLonglong)
	ft2 := types.NewFieldType(mysql.TypeLonglong)
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, ft.Equals(ft2))

	// flag diff
	ft.DelFlag(mysql.NotNullFlag)
	ft2.AddFlag(mysql.NotNullFlag)
	hasher1.Reset()
	hasher2.Reset()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// flen diff
	ft.AddFlag(mysql.NotNullFlag)
	ft2.AddFlag(mysql.NotNullFlag)
	ft2.SetFlen(ft2.GetFlen() + 1)
	hasher1.Reset()
	hasher2.Reset()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// decimal diff
	ft2.SetFlen(ft.GetFlen())
	ft2.SetDecimal(ft.GetDecimal() + 1)
	hasher2.Reset()
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// charset diff
	ft2.SetDecimal(ft.GetDecimal())
	ft2.SetCharset(ft.GetCharset() + "1")
	hasher2.Reset()
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// collate diff
	ft2.SetCharset(ft.GetCharset())
	ft2.SetCollate(ft.GetCollate() + "1")
	hasher2.Reset()
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// tp diff
	ft2.SetCollate(ft.GetCollate())
	ft2.SetType(ft.GetType() + 1)
	hasher2.Reset()
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// elems diff
	ft2.SetType(ft.GetType())
	ft.SetElems([]string{""})
	ft2.SetElems([]string{"a"})
	hasher1.Reset()
	hasher2.Reset()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// elemsIsBinaryLit diff
	ft.SetElems([]string{"a", "b"})
	ft2.SetElems([]string{"a", "b"})
	ft.SetElemWithIsBinaryLit(0, "1", true)
	ft.SetElemWithIsBinaryLit(1, "2", false)
	ft2.SetElemWithIsBinaryLit(0, "1", true)
	ft2.SetElemWithIsBinaryLit(1, "2", true)
	hasher1.Reset()
	hasher2.Reset()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// array diff
	ft.SetElems([]string{"a", "b"})
	ft2.SetElems([]string{"a", "b"})
	ft.SetElemWithIsBinaryLit(0, "1", true)
	ft.SetElemWithIsBinaryLit(1, "2", true)
	ft2.SetElemWithIsBinaryLit(0, "1", true)
	ft2.SetElemWithIsBinaryLit(1, "2", true)
	ft.SetArray(true)
	ft2.SetArray(false)
	hasher1.Reset()
	hasher2.Reset()
	ft.Hash64(hasher1)
	ft2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, ft.Equals(ft2))

	// same
	ft2.SetArray(true)
	hasher2.Reset()
	ft2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, ft.Equals(ft2))
}

func TestColumnHashEquals(t *testing.T) {
	col1 := &Column{UniqueID: 1}
	col2 := &Column{UniqueID: 1}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	col1.Hash64(hasher1)
	col2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, col1.Equals(col2))

	// diff uniqueID
	col2.UniqueID = 2
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff ID
	col2.UniqueID = col1.UniqueID
	col2.ID = 2
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff RetType
	col2.ID = col1.ID
	col2.RetType = types.NewFieldType(mysql.TypeLonglong)
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff Index
	col2.RetType = col1.RetType
	col2.Index = 1
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff VirtualExpr see TestColumnHashEuqals4VirtualExpr

	// diff OrigName
	col2.Index = col1.Index
	col2.OrigName = "a"
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff IsHidden
	col2.OrigName = col1.OrigName
	col2.IsHidden = true
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff IsPrefix
	col2.IsHidden = col1.IsHidden
	col2.IsPrefix = true
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff InOperand
	col2.IsPrefix = col1.IsPrefix
	col2.InOperand = true
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff collationInfo
	col2.InOperand = col1.InOperand
	col2.collationInfo = collationInfo{
		collation: "aa",
	}
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	// diff CorrelatedColUniqueID
	col2.collationInfo = col1.collationInfo
	col2.CorrelatedColUniqueID = 1
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))
}

func TestColumnHashEuqals4VirtualExpr(t *testing.T) {
	col1 := &Column{UniqueID: 1, VirtualExpr: NewZero()}
	col2 := &Column{UniqueID: 1, VirtualExpr: nil}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	col1.Hash64(hasher1)
	col2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, col1.Equals(col2))

	col2.VirtualExpr = NewZero()
	hasher2.Reset()
	col2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, col1.Equals(col2))

	col1.VirtualExpr = nil
	col2.VirtualExpr = nil
	hasher1.Reset()
	hasher2.Reset()
	col1.Hash64(hasher1)
	col2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, col1.Equals(col2))
}

func TestColumnEqualsWithNilWrappedInAny(t *testing.T) {
	col1 := &Column{UniqueID: 1}
	// Test: other is nil
	require.False(t, col1.Equals(nil))
	// Test: other is *Column(nil) wrapped in any
	var col2 *Column = nil
	var col2AsAny any = col2
	require.False(t, col1.Equals(col2AsAny))
	// Test: both Columns are nil
	var col3 *Column = nil
	require.True(t, col3.Equals(col2AsAny))
	// Test: two Columns with the same values
	col4 := &Column{UniqueID: 1}
	require.True(t, col1.Equals(col4))
	// Test: two Columns with different values
	col5 := &Column{UniqueID: 2}
	require.False(t, col1.Equals(col5))
}
