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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestNewValuesFunc(t *testing.T) {
	ctx := createContext(t)
	res := NewValuesFunc(ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	require.Equal(t, "values", res.FuncName.O)
	require.Equal(t, mysql.TypeLonglong, res.RetType.GetType())
	_, ok := res.Function.(*builtinValuesIntSig)
	require.True(t, ok)
}

func TestEvaluateExprWithNull(t *testing.T) {
	ctx := createContext(t)
	tblInfo := newTestTableBuilder("").add("col0", mysql.TypeLonglong, 0).add("col1", mysql.TypeLonglong, 0).build()
	schema := tableInfoToSchemaForTest(tblInfo)
	col0 := schema.Columns[0]
	col1 := schema.Columns[1]
	schema.Columns = schema.Columns[:1]
	innerIfNull, err := newFunctionForTest(ctx, ast.Ifnull, col1, NewOne())
	require.NoError(t, err)
	outerIfNull, err := newFunctionForTest(ctx, ast.Ifnull, col0, innerIfNull)
	require.NoError(t, err)

	res := EvaluateExprWithNull(ctx, schema, outerIfNull)
	require.Equal(t, "ifnull(Column#1, 1)", res.String())
	schema.Columns = append(schema.Columns, col1)
	// ifnull(null, ifnull(null, 1))
	res = EvaluateExprWithNull(ctx, schema, outerIfNull)
	require.True(t, res.Equal(ctx, NewOne()))
}

func TestEvaluateExprWithNullAndParameters(t *testing.T) {
	ctx := createContext(t)
	tblInfo := newTestTableBuilder("").add("col0", mysql.TypeLonglong, 0).build()
	schema := tableInfoToSchemaForTest(tblInfo)
	col0 := schema.Columns[0]

	ctx.GetSessionVars().StmtCtx.UseCache = true

	// cases for parameters
	ltWithoutParam, err := newFunctionForTest(ctx, ast.LT, col0, NewOne())
	require.NoError(t, err)
	res := EvaluateExprWithNull(ctx, schema, ltWithoutParam)
	require.True(t, res.Equal(ctx, NewNull())) // the expression is evaluated to null
	param := NewOne()
	param.ParamMarker = &ParamMarker{ctx: ctx, order: 0}
	ctx.GetSessionVars().PreparedParams = append(ctx.GetSessionVars().PreparedParams, types.NewIntDatum(10))
	ltWithParam, err := newFunctionForTest(ctx, ast.LT, col0, param)
	require.NoError(t, err)
	res = EvaluateExprWithNull(ctx, schema, ltWithParam)
	_, isScalarFunc := res.(*ScalarFunction)
	require.True(t, isScalarFunc) // the expression with parameters is not evaluated
}

func TestConstant(t *testing.T) {
	ctx := createContext(t)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	require.False(t, NewZero().IsCorrelated())
	require.True(t, NewZero().ConstItem(sc))
	require.True(t, NewZero().Decorrelate(nil).Equal(ctx, NewZero()))
	require.Equal(t, []byte{0x0, 0x8, 0x0}, NewZero().HashCode(sc))
	require.False(t, NewZero().Equal(ctx, NewOne()))
	res, err := NewZero().MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, []byte{0x22, 0x30, 0x22}, res)
}

func TestIsBinaryLiteral(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeEnum)}
	require.False(t, IsBinaryLiteral(col))
	col.RetType.SetType(mysql.TypeSet)
	require.False(t, IsBinaryLiteral(col))
	col.RetType.SetType(mysql.TypeBit)
	require.False(t, IsBinaryLiteral(col))
	col.RetType.SetType(mysql.TypeDuration)
	require.False(t, IsBinaryLiteral(col))

	con := &Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum([]byte{byte(0), byte(1)})}
	require.True(t, IsBinaryLiteral(con))
	con.Value = types.NewIntDatum(1)
	require.False(t, IsBinaryLiteral(col))
}

func TestConstItem(t *testing.T) {
	ctx := createContext(t)
	sf := newFunction(ast.Rand)
	require.False(t, sf.ConstItem(ctx.GetSessionVars().StmtCtx))
	sf = newFunction(ast.UUID)
	require.False(t, sf.ConstItem(ctx.GetSessionVars().StmtCtx))
	sf = newFunction(ast.GetParam, NewOne())
	require.False(t, sf.ConstItem(ctx.GetSessionVars().StmtCtx))
	sf = newFunction(ast.Abs, NewOne())
	require.True(t, sf.ConstItem(ctx.GetSessionVars().StmtCtx))
}

func TestVectorizable(t *testing.T) {
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
	require.True(t, Vectorizable(exprs))

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
	require.False(t, Vectorizable(exprs))

	exprs = exprs[:0]
	sf = newFunction(ast.GetVar, column0)
	exprs = append(exprs, sf)
	require.False(t, Vectorizable(exprs))

	exprs = exprs[:0]
	sf = newFunction(ast.NextVal, column0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.LastVal, column0)
	exprs = append(exprs, sf)
	sf = newFunction(ast.SetVal, column1, column2)
	exprs = append(exprs, sf)
	require.False(t, Vectorizable(exprs))
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
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.SetFlen(flen)
		fieldType.SetDecimal(decimal)
		charset, collate := types.DefaultCharsetForType(tp)
		fieldType.SetCharset(charset)
		fieldType.SetCollate(collate)
		fieldType.SetFlag(builder.flags[i])
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

func TestEvalExpr(t *testing.T) {
	ctx := createContext(t)
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
		require.True(t, colExpr.Vectorized())
		ctx.GetSessionVars().EnableVectorizedExpression = false
		err = EvalExpr(ctx, colExpr, colExpr.GetType().EvalType(), input, colBuf)
		require.NoError(t, err)
		ctx.GetSessionVars().EnableVectorizedExpression = true
		err = EvalExpr(ctx, colExpr, colExpr.GetType().EvalType(), input, colBuf2)
		require.NoError(t, err)
		for j := 0; j < 1024; j++ {
			isNull := colBuf.IsNull(j)
			isNull2 := colBuf2.IsNull(j)
			require.Equal(t, isNull2, isNull)
			if isNull {
				continue
			}
			require.Equal(t, string(colBuf2.GetRaw(j)), string(colBuf.GetRaw(j)))
		}
	}
}
