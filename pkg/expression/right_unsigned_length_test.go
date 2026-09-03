// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0.

package expression

import (
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestRightUnsignedLength(t *testing.T) {
	ctx := createContext(t)
	utf8Input := primitiveValsToConstants(ctx, []any{"你好"})[0]
	binaryInput := &Constant{
		Value: types.NewStringDatum("abc"),
		RetType: types.NewFieldTypeBuilder().
			SetType(mysql.TypeString).
			SetFlag(mysql.BinaryFlag).
			SetCharset(charset.CharsetBin).
			SetCollate(charset.CollationBin).
			BuildP(),
	}
	unsignedLength := primitiveValsToConstants(ctx, []any{uint64(math.MaxUint64)})[0]
	signedLength := primitiveValsToConstants(ctx, []any{int64(-1)})[0]

	testCases := []struct {
		name     string
		str      Expression
		length   Expression
		expected string
		binary   bool
	}{
		{name: "utf8/unsigned", str: utf8Input, length: unsignedLength, expected: "你好"},
		{name: "binary/unsigned", str: binaryInput, length: unsignedLength, expected: "abc", binary: true},
		{name: "utf8/signed_negative", str: utf8Input, length: signedLength, expected: ""},
		{name: "binary/signed_negative", str: binaryInput, length: signedLength, expected: "", binary: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			for _, vectorized := range []bool{false, true} {
				mode := "scalar"
				if vectorized {
					mode = "vectorized"
				}
				t.Run(mode, func(t *testing.T) {
					expr, err := NewFunctionBase(
						ctx,
						ast.Right,
						types.NewFieldType(mysql.TypeVarString),
						testCase.str,
						testCase.length,
					)
					require.NoError(t, err)
					f := expr.(*ScalarFunction).Function
					if testCase.binary {
						require.IsType(t, &builtinRightSig{}, f)
					} else {
						require.IsType(t, &builtinRightUTF8Sig{}, f)
					}

					if !vectorized {
						actual, isNull, err := expr.EvalString(ctx, chunk.Row{})
						require.NoError(t, err)
						require.False(t, isNull)
						require.Equal(t, testCase.expected, actual)
						return
					}

					input := chunk.NewChunkWithCapacity(nil, 1)
					input.SetNumVirtualRows(1)
					require.True(t, expr.Vectorized())
					result := chunk.NewColumn(expr.GetType(ctx.GetEvalCtx()), 1)
					require.NoError(t, expr.VecEvalString(ctx, input, result))
					require.False(t, result.IsNull(0))
					require.Equal(t, testCase.expected, result.GetString(0))
				})
			}
		})
	}
}
