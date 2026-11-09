// Copyright 2026 PingCAP, Inc.
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
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTiDBRowIDScalarIntegerHandle(t *testing.T) {
	ctx := mock.NewContext()
	expr, err := NewTiDBRowID(
		ctx,
		NewInt64Const(101),
		TiDBRowIDExtraHandle,
		nil,
		nil,
		NewInt64Const(-7),
	)
	require.NoError(t, err)
	require.True(t, expr.(*ScalarFunction).Vectorized())

	token, isNull, err := expr.EvalString(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	expected, err := tablecodec.EncodeTiDBRowID(101, kv.IntHandle(-7))
	require.NoError(t, err)
	require.Equal(t, expected, token)

	retType := expr.GetType(ctx)
	require.Equal(t, mysql.TypeVarString, retType.GetType())
	require.Equal(t, mysql.MaxFieldVarCharLength, retType.GetFlen())
	require.Equal(t, charset.CharsetASCII, retType.GetCharset())
	require.Equal(t, charset.CollationASCII, retType.GetCollate())
	require.False(t, mysql.HasNotNullFlag(retType.GetFlag()))

	input := chunk.NewChunkWithCapacity(nil, 3)
	input.SetNumVirtualRows(3)
	result := chunk.NewColumn(retType, 3)
	require.NoError(t, expr.VecEvalString(ctx, input, result))
	require.Equal(t, 3, result.Rows())
	for i := range result.Rows() {
		require.Equal(t, expected, result.GetString(i))
	}
}

func TestTiDBRowIDScalarNullInputs(t *testing.T) {
	ctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	input := chunk.NewChunkWithCapacity([]*types.FieldType{intType, intType}, 3)
	input.AppendNull(0)
	input.AppendInt64(1, 1)
	input.AppendInt64(0, 101)
	input.AppendNull(1)
	input.AppendInt64(0, 101)
	input.AppendInt64(1, 2)

	expr, err := NewTiDBRowID(
		ctx,
		&Column{Index: 0, RetType: intType},
		TiDBRowIDIntHandle,
		nil,
		nil,
		&Column{Index: 1, RetType: intType},
	)
	require.NoError(t, err)

	for i := range 2 {
		_, isNull, err := expr.EvalString(ctx, input.GetRow(i))
		require.NoError(t, err)
		require.True(t, isNull)
	}
	expected, err := tablecodec.EncodeTiDBRowID(101, kv.IntHandle(2))
	require.NoError(t, err)
	token, isNull, err := expr.EvalString(ctx, input.GetRow(2))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, expected, token)

	result := chunk.NewColumn(expr.GetType(ctx), input.NumRows())
	require.NoError(t, expr.VecEvalString(ctx, input, result))
	require.True(t, result.IsNull(0))
	require.True(t, result.IsNull(1))
	require.False(t, result.IsNull(2))
	require.Equal(t, expected, result.GetString(2))

	tableInfo, primaryIndex := tiDBRowIDCommonHandleMetadata()
	commonExpr, err := NewTiDBRowID(
		ctx,
		NewInt64Const(202),
		TiDBRowIDCommonHandle,
		tableInfo,
		primaryIndex,
		NewNull(),
		NewInt64Const(1),
	)
	require.NoError(t, err)
	_, isNull, err = commonExpr.EvalString(ctx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)
}

func TestTiDBRowIDScalarCommonHandle(t *testing.T) {
	ctx := mock.NewContext()
	tableInfo, primaryIndex := tiDBRowIDCommonHandleMetadata()

	expr, err := NewTiDBRowID(
		ctx,
		NewInt64Const(202),
		TiDBRowIDCommonHandle,
		tableInfo,
		primaryIndex,
		NewStrConst("Common"),
		NewInt64Const(8),
	)
	require.NoError(t, err)

	datums := []types.Datum{types.NewStringDatum("Common"), types.NewIntDatum(8)}
	datums[0].SetCollation(charset.CollationUTF8MB4)
	commonBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, datums...)
	require.NoError(t, err)
	commonHandle, err := kv.NewCommonHandle(commonBytes)
	require.NoError(t, err)
	expected, err := tablecodec.EncodeTiDBRowID(202, commonHandle)
	require.NoError(t, err)
	token, isNull, err := expr.EvalString(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, expected, token)

	sig := expr.(*ScalarFunction).Function.(*builtinTiDBRowIDSig)
	require.NotSame(t, tableInfo, sig.tableInfo)
	require.NotSame(t, primaryIndex, sig.primaryIndex)
	clone := expr.Clone().(*ScalarFunction).Function.(*builtinTiDBRowIDSig)
	require.NotSame(t, sig.tableInfo, clone.tableInfo)
	require.NotSame(t, sig.primaryIndex, clone.primaryIndex)

	primaryIndex.Columns[0].Length = 3
	prefixExpr, err := NewTiDBRowID(
		ctx,
		NewInt64Const(202),
		TiDBRowIDCommonHandle,
		tableInfo,
		primaryIndex,
		NewStrConst("Common"),
		NewInt64Const(8),
	)
	require.NoError(t, err)
	prefixDatums := []types.Datum{types.NewStringDatum("Com"), types.NewIntDatum(8)}
	prefixDatums[0].SetCollation(charset.CollationUTF8MB4)
	prefixBytes, err := codec.EncodeKey(ctx.GetSessionVars().StmtCtx.TimeZone(), nil, prefixDatums...)
	require.NoError(t, err)
	prefixHandle, err := kv.NewCommonHandle(prefixBytes)
	require.NoError(t, err)
	expectedPrefixToken, err := tablecodec.EncodeTiDBRowID(202, prefixHandle)
	require.NoError(t, err)
	prefixToken, isNull, err := prefixExpr.EvalString(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, expectedPrefixToken, prefixToken)
}

func tiDBRowIDCommonHandleMetadata() (*model.TableInfo, *model.IndexInfo) {
	stringType := types.NewFieldType(mysql.TypeVarchar)
	stringType.SetCharset(charset.CharsetUTF8MB4)
	stringType.SetCollate(charset.CollationUTF8MB4)
	stringType.AddFlag(mysql.NotNullFlag)
	intType := types.NewFieldType(mysql.TypeLonglong)
	intType.AddFlag(mysql.NotNullFlag)
	tableInfo := &model.TableInfo{
		IsCommonHandle: true,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("a"), Offset: 0, FieldType: *stringType},
			{ID: 2, Name: pmodel.NewCIStr("b"), Offset: 1, FieldType: *intType},
		},
	}
	primaryIndex := &model.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Primary: true,
		Columns: []*model.IndexColumn{
			{Name: pmodel.NewCIStr("a"), Offset: 0, Length: types.UnspecifiedLength},
			{Name: pmodel.NewCIStr("b"), Offset: 1, Length: types.UnspecifiedLength},
		},
	}
	return tableInfo, primaryIndex
}

func TestTiDBRowIDPublicFunctionRequiresPlannerBinding(t *testing.T) {
	ctx := mock.NewContext()
	_, err := newFunctionForTest(ctx, ast.TiDBRowID)
	require.True(t, errIncorrectArgs.Equal(err))
	_, err = newFunctionForTest(ctx, ast.TiDBRowID, NewOne(), NewOne())
	require.True(t, ErrIncorrectParameterCount.Equal(err))
}

func TestTiDBRowIDPredicate(t *testing.T) {
	ctx := mock.NewContext()
	first, err := tablecodec.EncodeTiDBRowID(101, kv.IntHandle(1))
	require.NoError(t, err)
	second, err := tablecodec.EncodeTiDBRowID(101, kv.IntHandle(2))
	require.NoError(t, err)
	foreign, err := tablecodec.EncodeTiDBRowID(202, kv.IntHandle(1))
	require.NoError(t, err)

	evaluate := func(t *testing.T, not bool, candidates ...Expression) (int64, bool, error) {
		t.Helper()
		predicate, err := NewTiDBRowIDPredicate(ctx, not, NewStrConst(first), candidates...)
		require.NoError(t, err)
		require.False(t, predicate.(*ScalarFunction).Vectorized())
		return predicate.EvalInt(ctx, chunk.Row{})
	}

	t.Run("equal", func(t *testing.T) {
		result, isNull, err := evaluate(t, false, NewStrConst(strings.ToUpper(first)))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(1), result)

		result, isNull, err = evaluate(t, false, NewStrConst(foreign))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(0), result)

		_, isNull, err = evaluate(t, false, NewNull())
		require.NoError(t, err)
		require.True(t, isNull)

		_, _, err = evaluate(t, false, NewStrConst("malformed"))
		require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err))
	})

	t.Run("in validates every candidate", func(t *testing.T) {
		_, _, err := evaluate(
			t,
			false,
			NewStrConst(first),
			NewStrConst("malformed"),
		)
		require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err))
	})

	t.Run("in null semantics", func(t *testing.T) {
		result, isNull, err := evaluate(t, false, NewStrConst(first), NewNull())
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(1), result)

		_, isNull, err = evaluate(t, false, NewStrConst(second), NewNull())
		require.NoError(t, err)
		require.True(t, isNull)

		result, isNull, err = evaluate(t, true, NewStrConst(first), NewNull())
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(0), result)

		_, isNull, err = evaluate(t, true, NewStrConst(second), NewNull())
		require.NoError(t, err)
		require.True(t, isNull)

		result, isNull, err = evaluate(t, true, NewStrConst(second))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, int64(1), result)
	})
}

func TestTiDBRowIDRangePredicate(t *testing.T) {
	ctx := mock.NewContext()
	tableInfo := &model.TableInfo{ID: 101}
	rowID, err := NewTiDBRowID(
		ctx,
		NewInt64Const(tableInfo.ID),
		TiDBRowIDIntHandle,
		tableInfo,
		nil,
		NewInt64Const(2),
	)
	require.NoError(t, err)
	first, err := tablecodec.EncodeTiDBRowID(tableInfo.ID, kv.IntHandle(1))
	require.NoError(t, err)
	second, err := tablecodec.EncodeTiDBRowID(tableInfo.ID, kv.IntHandle(2))
	require.NoError(t, err)
	third, err := tablecodec.EncodeTiDBRowID(tableInfo.ID, kv.IntHandle(3))
	require.NoError(t, err)

	evaluate := func(op TiDBRowIDRangeOp, not bool, bounds ...Expression) (int64, bool, error) {
		predicate, err := NewTiDBRowIDRangePredicate(ctx, op, not, rowID, bounds...)
		require.NoError(t, err)
		require.False(t, predicate.(*ScalarFunction).Vectorized())
		return predicate.EvalInt(ctx, chunk.Row{})
	}

	result, isNull, err := evaluate(TiDBRowIDRangeGT, false, NewStrConst(first))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(1), result)
	result, isNull, err = evaluate(TiDBRowIDRangeLE, false, NewStrConst(strings.ToUpper(second)))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(1), result)
	result, isNull, err = evaluate(TiDBRowIDRangeBetween, false, NewStrConst(first), NewStrConst(third))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(1), result)
	result, isNull, err = evaluate(TiDBRowIDRangeBetween, true, NewStrConst(first), NewStrConst(third))
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(0), result)

	_, isNull, err = evaluate(TiDBRowIDRangeBetween, false, NewNull(), NewStrConst(first))
	require.NoError(t, err)
	require.False(t, isNull)
	_, isNull, err = evaluate(TiDBRowIDRangeBetween, false, NewNull(), NewStrConst(third))
	require.NoError(t, err)
	require.True(t, isNull)
	_, _, err = evaluate(TiDBRowIDRangeGT, false, NewStrConst("malformed"))
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err))
	foreign, err := tablecodec.EncodeTiDBRowID(tableInfo.ID+1, kv.IntHandle(1))
	require.NoError(t, err)
	_, _, err = evaluate(TiDBRowIDRangeGT, false, NewStrConst(foreign))
	require.True(t, tablecodec.ErrInvalidTiDBRowID.Equal(err))
}

func BenchmarkTiDBRowIDProjection(b *testing.B) {
	const rowCount = 1024
	ctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	intType.AddFlag(mysql.NotNullFlag)
	stringType := types.NewFieldType(mysql.TypeVarchar)
	stringType.SetCharset(charset.CharsetUTF8MB4)
	stringType.SetCollate(charset.CollationUTF8MB4)
	stringType.AddFlag(mysql.NotNullFlag)

	intInput := chunk.NewChunkWithCapacity([]*types.FieldType{intType, intType}, rowCount)
	for i := range rowCount {
		intInput.AppendInt64(0, 101)
		intInput.AppendInt64(1, int64(i))
	}
	intExpr, err := NewTiDBRowID(
		ctx,
		&Column{Index: 0, RetType: intType},
		TiDBRowIDIntHandle,
		nil,
		nil,
		&Column{Index: 1, RetType: intType},
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkTiDBRowIDProjection(b, ctx, intExpr, intInput, "Integer")

	tableInfo, primaryIndex := tiDBRowIDCommonHandleMetadata()
	commonInput := chunk.NewChunkWithCapacity([]*types.FieldType{intType, stringType, intType}, rowCount)
	for i := range rowCount {
		commonInput.AppendInt64(0, 202)
		commonInput.AppendString(1, "common-handle")
		commonInput.AppendInt64(2, int64(i))
	}
	commonExpr, err := NewTiDBRowID(
		ctx,
		&Column{Index: 0, RetType: intType},
		TiDBRowIDCommonHandle,
		tableInfo,
		primaryIndex,
		&Column{Index: 1, RetType: stringType},
		&Column{Index: 2, RetType: intType},
	)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkTiDBRowIDProjection(b, ctx, commonExpr, commonInput, "Common")
}

func benchmarkTiDBRowIDProjection(
	b *testing.B,
	ctx *mock.Context,
	expr Expression,
	input *chunk.Chunk,
	handleKind string,
) {
	b.Helper()
	b.Run(handleKind+"/Scalar", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(input.NumRows()), "rows/op")
		var token string
		for range b.N {
			iterator := chunk.NewIterator4Chunk(input)
			for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
				var err error
				token, _, err = expr.EvalString(ctx, row)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		runtime.KeepAlive(token)
	})
	b.Run(handleKind+"/Vector", func(b *testing.B) {
		result := chunk.NewColumn(expr.GetType(ctx), input.NumRows())
		b.ReportAllocs()
		b.ReportMetric(float64(input.NumRows()), "rows/op")
		for range b.N {
			if err := expr.VecEvalString(ctx, input, result); err != nil {
				b.Fatal(err)
			}
		}
		runtime.KeepAlive(result)
	})
}
