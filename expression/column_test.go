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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testEvaluatorSuite) TestColumn(c *C) {
	defer testleak.AfterTest(c)()

	col := &Column{RetType: types.NewFieldType(mysql.TypeLonglong), FromID: 0, Position: 0}
	c.Assert(col.Equal(col, nil), IsTrue)
	c.Assert(col.Equal(&Column{FromID: 1}, nil), IsFalse)
	c.Assert(col.IsCorrelated(), IsFalse)
	c.Assert(col.Equal(col.Decorrelate(nil), nil), IsTrue)

	marshal, err := col.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(marshal, DeepEquals, []byte{0x22, 0x22})

	intDatum := types.NewIntDatum(1)
	corCol := &CorrelatedColumn{Column: *col, Data: &intDatum}
	invalidCorCol := &CorrelatedColumn{Column: Column{FromID: 1}}
	schema := NewSchema(&Column{FromID: 0, Position: 0})
	c.Assert(corCol.Equal(corCol, nil), IsTrue)
	c.Assert(corCol.Equal(invalidCorCol, nil), IsFalse)
	c.Assert(corCol.IsCorrelated(), IsTrue)
	c.Assert(corCol.Decorrelate(schema).Equal(col, nil), IsTrue)
	c.Assert(invalidCorCol.Decorrelate(schema).Equal(invalidCorCol, nil), IsTrue)

	intCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data: &intDatum}
	intVal, isNull, err := intCorCol.EvalInt(s.ctx, nil)
	c.Assert(intVal, Equals, int64(1))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	realDatum := types.NewFloat64Datum(1.2)
	realCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDouble)},
		Data: &realDatum}
	realVal, isNull, err := realCorCol.EvalReal(s.ctx, nil)
	c.Assert(realVal, Equals, float64(1.2))
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	decimalDatum := types.NewDecimalDatum(types.NewDecFromStringForTest("1.2"))
	decimalCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)},
		Data: &decimalDatum}
	decVal, isNull, err := decimalCorCol.EvalDecimal(s.ctx, nil)
	c.Assert(decVal.Compare(types.NewDecFromStringForTest("1.2")), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	stringDatum := types.NewStringDatum("abc")
	stringCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeVarchar)},
		Data: &stringDatum}
	strVal, isNull, err := stringCorCol.EvalString(s.ctx, nil)
	c.Assert(strVal, Equals, "abc")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	durationCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDuration)},
		Data: &durationDatum}
	durationVal, isNull, err := durationCorCol.EvalDuration(s.ctx, nil)
	c.Assert(durationVal.Compare(duration), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	timeDatum := types.NewTimeDatum(tm)
	timeCorCol := &CorrelatedColumn{Column: Column{RetType: types.NewFieldType(mysql.TypeDatetime)},
		Data: &timeDatum}
	timeVal, isNull, err := timeCorCol.EvalTime(s.ctx, nil)
	c.Assert(timeVal.Compare(tm), Equals, 0)
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestColumnHashCode(c *C) {
	defer testleak.AfterTest(c)()

	col1 := &Column{
		FromID:   1,
		Position: 12,
	}
	c.Assert(col1.HashCode(), DeepEquals, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc})

	col2 := &Column{
		FromID:   11,
		Position: 2,
	}
	c.Assert(col2.HashCode(), DeepEquals, []byte{0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2})
}

func (s *testEvaluatorSuite) TestColumn2Expr(c *C) {
	defer testleak.AfterTest(c)()

	cols := make([]*Column, 0, 5)
	for i := 0; i < 5; i++ {
		cols = append(cols, &Column{FromID: i})
	}

	exprs := Column2Exprs(cols)
	for i := range exprs {
		c.Assert(exprs[i].Equal(cols[i], nil), IsTrue)
	}
}

func (s *testEvaluatorSuite) TestColInfo2Col(c *C) {
	defer testleak.AfterTest(c)()

	col0, col1 := &Column{ColName: model.NewCIStr("col0")}, &Column{ColName: model.NewCIStr("col1")}
	cols := []*Column{col0, col1}
	colInfo := &model.ColumnInfo{Name: model.NewCIStr("col1")}
	res := ColInfo2Col(cols, colInfo)
	c.Assert(res.Equal(col1, nil), IsTrue)

	colInfo.Name = model.NewCIStr("col2")
	res = ColInfo2Col(cols, colInfo)
	c.Assert(res, IsNil)
}

func (s *testEvaluatorSuite) TestIndexInfo2Cols(c *C) {
	defer testleak.AfterTest(c)()

	col0 := &Column{ColName: model.NewCIStr("col0"), RetType: types.NewFieldType(mysql.TypeLonglong)}
	col1 := &Column{ColName: model.NewCIStr("col1"), RetType: types.NewFieldType(mysql.TypeLonglong)}
	indexCol0, indexCol1 := &model.IndexColumn{Name: model.NewCIStr("col0")}, &model.IndexColumn{Name: model.NewCIStr("col1")}
	indexInfo := &model.IndexInfo{Columns: []*model.IndexColumn{indexCol0, indexCol1}}

	cols := []*Column{col0}
	resCols, lengths := IndexInfo2Cols(cols, indexInfo)
	c.Assert(len(resCols), Equals, 1)
	c.Assert(len(lengths), Equals, 1)
	c.Assert(resCols[0].Equal(col0, nil), IsTrue)

	cols = []*Column{col1}
	resCols, lengths = IndexInfo2Cols(cols, indexInfo)
	c.Assert(len(resCols), Equals, 0)
	c.Assert(len(lengths), Equals, 0)

	cols = []*Column{col0, col1}
	resCols, lengths = IndexInfo2Cols(cols, indexInfo)
	c.Assert(len(resCols), Equals, 2)
	c.Assert(len(lengths), Equals, 2)
	c.Assert(resCols[0].Equal(col0, nil), IsTrue)
	c.Assert(resCols[1].Equal(col1, nil), IsTrue)
}
