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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testEvaluatorSuite) TestNewValuesFunc(c *C) {
	res := NewValuesFunc(s.ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	c.Assert(res.FuncName.O, Equals, "values")
	c.Assert(res.RetType.Tp, Equals, mysql.TypeLonglong)
	_, ok := res.Function.(*builtinValuesIntSig)
	c.Assert(ok, IsTrue)
}

func (s *testEvaluatorSuite) TestEvaluateExprWithNull(c *C) {
	tblInfo := newTestTableBuilder("").add("col0", mysql.TypeLonglong, 0).add("col1", mysql.TypeLonglong, 0).build()
	schema := tableInfoToSchemaForTest(tblInfo)
	col0 := schema.Columns[0]
	col1 := schema.Columns[1]
	schema.Columns = schema.Columns[:1]
	innerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, col1, NewOne())
	c.Assert(err, IsNil)
	outerIfNull, err := newFunctionForTest(s.ctx, ast.Ifnull, col0, innerIfNull)
	c.Assert(err, IsNil)

	res := EvaluateExprWithNull(s.ctx, schema, outerIfNull)
	c.Assert(res.String(), Equals, "ifnull(Column#1, 1)")

	schema.Columns = append(schema.Columns, col1)
	// ifnull(null, ifnull(null, 1))
	res = EvaluateExprWithNull(s.ctx, schema, outerIfNull)
	c.Assert(res.Equal(s.ctx, NewOne()), IsTrue)
}

func (s *testEvaluatorSerialSuites) TestEvaluateExprWithNullAndParameters(c *C) {
	tblInfo := newTestTableBuilder("").add("col0", mysql.TypeLonglong, 0).build()
	schema := tableInfoToSchemaForTest(tblInfo)
	col0 := schema.Columns[0]

	defer func(original bool) {
		s.ctx.GetSessionVars().StmtCtx.UseCache = original
	}(s.ctx.GetSessionVars().StmtCtx.UseCache)
	s.ctx.GetSessionVars().StmtCtx.UseCache = true

	// cases for parameters
	ltWithoutParam, err := newFunctionForTest(s.ctx, ast.LT, col0, NewOne())
	c.Assert(err, IsNil)
	res := EvaluateExprWithNull(s.ctx, schema, ltWithoutParam)
	c.Assert(res.Equal(s.ctx, NewNull()), IsTrue) // the expression is evaluated to null

	param := NewOne()
	param.ParamMarker = &ParamMarker{ctx: s.ctx, order: 0}
	s.ctx.GetSessionVars().PreparedParams = append(s.ctx.GetSessionVars().PreparedParams, types.NewIntDatum(10))
	ltWithParam, err := newFunctionForTest(s.ctx, ast.LT, col0, param)
	c.Assert(err, IsNil)
	res = EvaluateExprWithNull(s.ctx, schema, ltWithParam)
	_, isScalarFunc := res.(*ScalarFunction)
	c.Assert(isScalarFunc, IsTrue) // the expression with parameters is not evaluated
}

<<<<<<< HEAD
func (s *testEvaluatorSuite) TestConstant(c *C) {
=======
func TestEvaluateExprWithNullNoChangeRetType(t *testing.T) {
	ctx := createContext(t)
	tblInfo := newTestTableBuilder("").add("col_str", mysql.TypeString, 0).build()
	schema := tableInfoToSchemaForTest(tblInfo)

	castStrAsJSON := BuildCastFunction(ctx, schema.Columns[0], types.NewFieldType(mysql.TypeJSON))
	jsonConstant := &Constant{Value: types.NewDatum("123"), RetType: types.NewFieldType(mysql.TypeJSON)}

	// initially has ParseToJSONFlag
	flagInCast := castStrAsJSON.(*ScalarFunction).RetType.GetFlag()
	require.True(t, mysql.HasParseToJSONFlag(flagInCast))

	// cast's ParseToJSONFlag removed by `DisableParseJSONFlag4Expr`
	eq, err := newFunctionForTest(ctx, ast.EQ, jsonConstant, castStrAsJSON)
	require.NoError(t, err)
	flagInCast = eq.(*ScalarFunction).GetArgs()[1].(*ScalarFunction).RetType.GetFlag()
	require.False(t, mysql.HasParseToJSONFlag(flagInCast))

	// after EvaluateExprWithNull, this flag should be still false
	EvaluateExprWithNull(ctx, schema, eq)
	flagInCast = eq.(*ScalarFunction).GetArgs()[1].(*ScalarFunction).RetType.GetFlag()
	require.False(t, mysql.HasParseToJSONFlag(flagInCast))
}

func TestConstant(t *testing.T) {
	ctx := createContext(t)
>>>>>>> 1f40fc72a... expression: use cloned RetType at `evaluateExprWithNull` when it may be changed. (#35759)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	c.Assert(NewZero().IsCorrelated(), IsFalse)
	c.Assert(NewZero().ConstItem(sc), IsTrue)
	c.Assert(NewZero().Decorrelate(nil).Equal(s.ctx, NewZero()), IsTrue)
	c.Assert(NewZero().HashCode(sc), DeepEquals, []byte{0x0, 0x8, 0x0})
	c.Assert(NewZero().Equal(s.ctx, NewOne()), IsFalse)
	res, err := NewZero().MarshalJSON()
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
	c.Assert(sf.ConstItem(s.ctx.GetSessionVars().StmtCtx), Equals, false)
	sf = newFunction(ast.UUID)
	c.Assert(sf.ConstItem(s.ctx.GetSessionVars().StmtCtx), Equals, false)
	sf = newFunction(ast.GetParam, NewOne())
	c.Assert(sf.ConstItem(s.ctx.GetSessionVars().StmtCtx), Equals, false)
	sf = newFunction(ast.Abs, NewOne())
	c.Assert(sf.ConstItem(s.ctx.GetSessionVars().StmtCtx), Equals, true)
}

func (s *testEvaluatorSuite) TestVectorizable(c *C) {
	exprs := make([]Expression, 0, 4)
	sf := newFunction(ast.Rand)
	column := &Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	exprs = append(exprs, sf)
	exprs = append(exprs, NewOne())
	exprs = append(exprs, NewNull())
	exprs = append(exprs, column)
	c.Assert(Vectorizable(exprs), Equals, true)

	column0 := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeString),
	}
	column1 := &Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeString),
	}
	column2 := &Column{
		UniqueID: 3,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	exprs = exprs[:0]
	sf = newFunction(ast.SetVar, column0, column1)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)

	exprs = exprs[:0]
	sf = newFunction(ast.GetVar, column0)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)

	exprs = exprs[:0]
	sf = newFunction(ast.NextVal, column0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.LastVal, column0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.SetVal, column1, column2)
	exprs = append(exprs, sf)
	c.Assert(Vectorizable(exprs), Equals, false)
}

type testTableBuilder struct {
	tableName   string
	columnNames []string
	tps         []byte
	flags       []uint
}

func newTestTableBuilder(tableName string) *testTableBuilder {
	return &testTableBuilder{tableName: tableName}
}

func (builder *testTableBuilder) add(name string, tp byte, flag uint) *testTableBuilder {
	builder.columnNames = append(builder.columnNames, name)
	builder.tps = append(builder.tps, tp)
	builder.flags = append(builder.flags, flag)
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
		fieldType.Flag = builder.flags[i]
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
			ID:       col.ID,
			RetType:  &col.FieldType,
		})
	}
	return schema
}

func (s *testEvaluatorSuite) TestEvalExpr(c *C) {
	ctx := mock.NewContext()
	eTypes := []types.EvalType{types.ETInt, types.ETReal, types.ETDecimal, types.ETString, types.ETTimestamp, types.ETDatetime, types.ETDuration}
	tNames := []string{"int", "real", "decimal", "string", "timestamp", "datetime", "duration"}
	for i := 0; i < len(tNames); i++ {
		ft := eType2FieldType(eTypes[i])
		colExpr := &Column{Index: 0, RetType: ft}
		input := chunk.New([]*types.FieldType{ft}, 1024, 1024)
		fillColumnWithGener(eTypes[i], input, 0, nil)
		colBuf := chunk.NewColumn(ft, 1024)
		colBuf2 := chunk.NewColumn(ft, 1024)
		var err error
		c.Assert(colExpr.Vectorized(), IsTrue)
		ctx.GetSessionVars().EnableVectorizedExpression = false
		err = EvalExpr(ctx, colExpr, colExpr.GetType().EvalType(), input, colBuf)
		if err != nil {
			c.Fatal(err)
		}
		ctx.GetSessionVars().EnableVectorizedExpression = true
		err = EvalExpr(ctx, colExpr, colExpr.GetType().EvalType(), input, colBuf2)
		if err != nil {
			c.Fatal(err)
		}
		for j := 0; j < 1024; j++ {
			isNull := colBuf.IsNull(j)
			isNull2 := colBuf2.IsNull(j)
			c.Assert(isNull, Equals, isNull2)
			if isNull {
				continue
			}
			c.Assert(string(colBuf.GetRaw(j)), Equals, string(colBuf2.GetRaw(j)))
		}
	}
}
