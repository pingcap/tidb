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

package expression

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testEvaluatorSuite) TestColumn(c *C) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), UniqueID: 1}

	c.Assert(col.Equal(nil, col), IsTrue)
	c.Assert(col.Equal(nil, &Column{}), IsFalse)
	c.Assert(col.IsCorrelated(), IsFalse)
	c.Assert(col.Equal(nil, col.Decorrelate(nil)), IsTrue)

	marshal, err := col.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(marshal, DeepEquals, []byte{0x22, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x23, 0x31, 0x22})

	intDatum := types.NewIntDatum(1)
	corCol := &CorrelatedColumn{Column: *col, Data: &intDatum}
	invalidCorCol := &CorrelatedColumn{Column: Column{}}
	schema := NewSchema(&Column{UniqueID: 1})
	c.Assert(corCol.Equal(nil, corCol), IsTrue)
	c.Assert(corCol.Equal(nil, invalidCorCol), IsFalse)
	c.Assert(corCol.IsCorrelated(), IsTrue)
	c.Assert(corCol.ConstItem(nil), IsFalse)
	c.Assert(corCol.Decorrelate(schema).Equal(nil, col), IsTrue)
	c.Assert(invalidCorCol.Decorrelate(schema).Equal(nil, invalidCorCol), IsTrue)

	intCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data: &intDatum}
	intVal, isNull, err := intCorCol.EvalInt(s.ctx, chunk.Row{})
	c.Assert(intVal, Equals, int64(1))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	realDatum := types.NewFloat64Datum(1.2)
	realCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDouble)},
		Data: &realDatum}
	realVal, isNull, err := realCorCol.EvalReal(s.ctx, chunk.Row{})
	c.Assert(realVal, Equals, float64(1.2))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	decimalDatum := types.NewDecimalDatum(types.NewDecFromStringForTest("1.2"))
	decimalCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)},
		Data: &decimalDatum}
	decVal, isNull, err := decimalCorCol.EvalDecimal(s.ctx, chunk.Row{})
	c.Assert(decVal.Compare(types.NewDecFromStringForTest("1.2")), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	stringDatum := types.NewStringDatum("abc")
	stringCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeVarchar)},
		Data: &stringDatum}
	strVal, isNull, err := stringCorCol.EvalString(s.ctx, chunk.Row{})
	c.Assert(strVal, Equals, "abc")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	durationCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDuration)},
		Data: &durationDatum}
	durationVal, isNull, err := durationCorCol.EvalDuration(s.ctx, chunk.Row{})
	c.Assert(durationVal.Compare(duration), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	timeDatum := types.NewTimeDatum(tm)
	timeCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDatetime)},
		Data: &timeDatum}
	timeVal, isNull, err := timeCorCol.EvalTime(s.ctx, chunk.Row{})
	c.Assert(timeVal.Compare(tm), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestColumnHashCode(c *C) {
	col1 := &Column{
		UniqueID: 12,
	}
	c.Assert(col1.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc})

	col2 := &Column{
		UniqueID: 2,
	}
	c.Assert(col2.HashCode(nil), DeepEquals, []byte{0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2})
}

func (s *testEvaluatorSuite) TestColumn2Expr(c *C) {
	cols := make([]*Column, 0, 5)
	for i := 0; i < 5; i++ {
		cols = append(cols, &Column{UniqueID: int64(i)})
	}

	exprs := Column2Exprs(cols)
	for i := range exprs {
		c.Assert(exprs[i].Equal(nil, cols[i]), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestColInfo2Col(c *C) {
	col0, col1 := &Column{ID: 0}, &Column{ID: 1}
	cols := []*Column{col0, col1}
	colInfo := &model.ColumnInfo{ID: 0}
	res := ColInfo2Col(cols, colInfo)
	c.Assert(res.Equal(nil, col1), IsTrue)

	colInfo.ID = 3
	res = ColInfo2Col(cols, colInfo)
	c.Assert(res, IsNil)
}

func (s *testEvaluatorSuite) TestIndexInfo2Cols(c *C) {
	col0 := &Column{UniqueID: 0, ID: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &Column{UniqueID: 1, ID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
	colInfo0 := &model.ColumnInfo{ID: 0, Name: model.NewCIStr("0")}
	colInfo1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("1")}
	indexCol0, indexCol1 := &model.IndexColumn{Name: model.NewCIStr("0")}, &model.IndexColumn{Name: model.NewCIStr("1")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1}}

	cols := []*Column{col0}
	colInfos := []*model.ColumnInfo{colInfo0}
	resCols, lengths := IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 1)
	c.Assert(len(lengths), Equals, 1)
	c.Assert(resCols[0].Equal(nil, col0), IsTrue)

	cols = []*Column{col1}
	colInfos = []*model.ColumnInfo{colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 0)
	c.Assert(len(lengths), Equals, 0)

	cols = []*Column{col0, col1}
	colInfos = []*model.ColumnInfo{colInfo0, colInfo1}
	resCols, lengths = IndexInfo2PrefixCols(colInfos, cols, indexInfo)
	c.Assert(len(resCols), Equals, 2)
	c.Assert(len(lengths), Equals, 2)
	c.Assert(resCols[0].Equal(nil, col0), IsTrue)
	c.Assert(resCols[1].Equal(nil, col1), IsTrue)
}

func (s *testEvaluatorSuite) TestColHybird(c *C) {
	ctx := mock.NewContext()

	// bit
	ft := types.NewFieldType(mysql.TypeBit)
	col := &Column{RetType: ft, Index: 0}
	input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		num, err := types.ParseBitStr(fmt.Sprintf("0b%b", i))
		c.Assert(err, IsNil)
		input.AppendBytes(0, num)
	}
	result, err := newBuffer(types.ETInt, 1024)
	c.Assert(err, IsNil)
	c.Assert(col.VecEvalInt(ctx, input, result), IsNil)

	it := chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalInt(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetInt64(i))
	}

	// use a container which has the different field type with bit
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(col.VecEvalInt(ctx, input, result), IsNil)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalInt(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetInt64(i))
	}

	// enum
	ft = types.NewFieldType(mysql.TypeEnum)
	col.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendEnum(0, types.Enum{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(col.VecEvalString(ctx, input, result), IsNil)

	it = chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalString(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetString(i))
	}

	// set
	ft = types.NewFieldType(mysql.TypeSet)
	col.RetType = ft
	input = chunk.New([]*types.FieldType{ft}, 1024, 1024)
	for i := 0; i < 1024; i++ {
		input.AppendSet(0, types.Set{Name: fmt.Sprintf("%v", i), Value: uint64(i)})
	}
	result, err = newBuffer(types.ETString, 1024)
	c.Assert(err, IsNil)
	c.Assert(col.VecEvalString(ctx, input, result), IsNil)

	it = chunk.NewIterator4Chunk(input)
	for row, i := it.Begin(), 0; row != it.End(); row, i = it.Next(), i+1 {
		v, _, err := col.EvalString(ctx, row)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, result.GetString(i))
	}
}
