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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
)

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	res := NewValuesFunc(s.ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestEvaluateExprWithNull(c *C) {
	tblInfo := newTestTableBuilder("").add("col0", mysql.TypeLonglong).add("col1", mysql.TypeLonglong).build()
	schema := tableInfoToSchemaForTest(tblInfo)
	col0 := schema.Columns[0]
	col1 := schema.Columns[1]
	schema.Columns = schema.Columns[:1]
	innerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, col1, One.Clone())
	c.Assert(err, IsNil)
	outerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, col0, innerIfNull)
	c.Assert(err, IsNil)

	res := EvaluateExprWithNull(s.ctx, schema, outerIfNull)
	c.Assert(res.String(), Equals, "ifnull(Column#1, 1)")

	schema.Columns = append(schema.Columns, col1)
	// ifnull(null, ifnull(null, 1))
	res = EvaluateExprWithNull(s.ctx, schema, outerIfNull)
	c.Assert(res.Equal(s.ctx, One), IsTrue)
}

func (s *testEvaluatorSuite) TestConstant(c *C) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	c.Assert(Zero.IsCorrelated(), IsFalse)
	c.Assert(Zero.ConstItem(), IsTrue)
	c.Assert(Zero.Decorrelate(nil).Equal(s.ctx, Zero), IsTrue)
	c.Assert(Zero.HashCode(sc), DeepEquals, []byte{0x0, 0x8, 0x0})
	c.Assert(Zero.Equal(s.ctx, One), IsFalse)
	res, err := Zero.MarshalJSON()
	c.Assert(err, IsNil)
	c.Assert(res, DeepEquals, []byte{0x22, 0x30, 0x22})
}

func (s *testEvaluatorSuite) TestIsBinaryLiteral(c *C) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeEnum)}
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeSet
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeBit
	c.Assert(IsBinaryLiteral(col), IsFalse)
	col.RetType.Tp = mysql.TypeDuration
	c.Assert(IsBinaryLiteral(col), IsFalse)

	con := &Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum([]byte{byte(0), byte(1)})}
	c.Assert(IsBinaryLiteral(con), IsTrue)
	con.Value = types.NewIntDatum(1)
	c.Assert(IsBinaryLiteral(con), IsFalse)
}

func (s *testEvaluatorSuite) TestConstItem(c *C) {
	sf := newFunction(ast.Rand)
	c.Assert(sf.ConstItem(), Equals, false)
	sf = newFunction(ast.UUID)
	c.Assert(sf.ConstItem(), Equals, false)
	sf = newFunction(ast.GetParam, One)
	c.Assert(sf.ConstItem(), Equals, false)
	sf = newFunction(ast.Abs, One)
	c.Assert(sf.ConstItem(), Equals, true)
}

type testTableBuilder struct {
	tableName   string
	columnNames []string
	tps         []byte
}

func newTestTableBuilder(tableName string) *testTableBuilder {
	return &testTableBuilder{tableName: tableName}
}

func (builder *testTableBuilder) add(name string, tp byte) *testTableBuilder {
	builder.columnNames = append(builder.columnNames, name)
	builder.tps = append(builder.tps, tp)
	return builder
}

func (builder *testTableBuilder) build() *model.TableInfo {
	ti := &model.TableInfo{
		ID:    1,
		Name:  model.NewCIStr(builder.tableName),
		State: model.StatePublic,
	}
	for i, colName := range builder.columnNames {
		tp := builder.tps[i]
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		ti.Columns = append(ti.Columns, &model.ColumnInfo{
			ID:        int64(i + 1),
			Name:      model.NewCIStr(colName),
			Offset:    i,
			FieldType: *fieldType,
			State:     model.StatePublic,
		})
	}
	return ti
}

func tableInfoToSchemaForTest(tableInfo *model.TableInfo) *Schema {
	columns := tableInfo.Columns
	schema := NewSchema(make([]*Column, 0, len(columns))...)
	for i, col := range columns {
		schema.Append(&Column{
			UniqueID: int64(i),
			TblName:  tableInfo.Name,
			ColName:  col.Name,
			ID:       col.ID,
			RetType:  &col.FieldType,
		})
	}
	return schema
}
