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
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundIntegerTestCase struct {
	value uint64
	frac  uint64
	want  uint64
}

func signedRoundIntegerBits(value int64) uint64 {
	return uint64(value)
}

func testRoundIntegerCases(t *testing.T, valueUnsigned, fracUnsigned bool, cases []roundIntegerTestCase) {
	t.Helper()
	ctx := createContext(t)
	valueType := types.NewFieldType(mysql.TypeLonglong)
	fracType := types.NewFieldType(mysql.TypeLonglong)
	if valueUnsigned {
		valueType.AddFlag(mysql.UnsignedFlag)
	}
	if fracUnsigned {
		fracType.AddFlag(mysql.UnsignedFlag)
	}

	fn, err := funcs[ast.Round].getFunction(ctx, []Expression{
		&Column{Index: 0, RetType: valueType},
		&Column{Index: 1, RetType: fracType},
	})
	require.NoError(t, err)
	require.True(t, fn.vectorized() && fn.isChildrenVectorized())

	input := chunk.NewChunkWithCapacity([]*types.FieldType{valueType, fracType}, len(cases))
	for _, tc := range cases {
		if valueUnsigned {
			input.AppendUint64(0, tc.value)
		} else {
			input.AppendInt64(0, int64(tc.value))
		}
		if fracUnsigned {
			input.AppendUint64(1, tc.frac)
		} else {
			input.AppendInt64(1, int64(tc.frac))
		}
	}

	for i, tc := range cases {
		got, isNull, err := fn.evalInt(ctx, input.GetRow(i))
		require.NoErrorf(t, err, "scalar case %d", i)
		require.Falsef(t, isNull, "scalar case %d", i)
		assert.Equalf(t, tc.want, uint64(got), "scalar case %d", i)
	}

	result := chunk.NewColumn(fn.getRetTp(), len(cases))
	require.NoError(t, fn.vecEvalInt(ctx, input, result))
	for i, tc := range cases {
		require.Falsef(t, result.IsNull(i), "vectorized case %d", i)
		assert.Equalf(t, tc.want, result.GetUint64(i), "vectorized case %d", i)
	}
}

func testRoundIntegerOverflow(t *testing.T, valueUnsigned bool, value uint64, frac int64) {
	t.Helper()
	ctx := createContext(t)
	valueType := types.NewFieldType(mysql.TypeLonglong)
	fracType := types.NewFieldType(mysql.TypeLonglong)
	if valueUnsigned {
		valueType.AddFlag(mysql.UnsignedFlag)
	}
	fn, err := funcs[ast.Round].getFunction(ctx, []Expression{
		&Column{Index: 0, RetType: valueType},
		&Column{Index: 1, RetType: fracType},
	})
	require.NoError(t, err)
	require.True(t, fn.vectorized() && fn.isChildrenVectorized())

	input := chunk.NewChunkWithCapacity([]*types.FieldType{valueType, fracType}, 1)
	if valueUnsigned {
		input.AppendUint64(0, value)
	} else {
		input.AppendInt64(0, int64(value))
	}
	input.AppendInt64(1, frac)

	_, _, err = fn.evalInt(ctx, input.GetRow(0))
	require.Truef(t, types.ErrOverflow.Equal(err), "scalar error: %v", err)
	result := chunk.NewColumn(fn.getRetTp(), 1)
	err = fn.vecEvalInt(ctx, input, result)
	require.Truef(t, types.ErrOverflow.Equal(err), "vectorized error: %v", err)
}

func TestRoundIntegerExactness(t *testing.T) {
	t.Run("signed value and frac", func(t *testing.T) {
		testRoundIntegerCases(t, false, false, []roundIntegerTestCase{
			{value: signedRoundIntegerBits(-717754013), frac: 24, want: signedRoundIntegerBits(-717754013)},
			{value: 717754013, frac: 24, want: 717754013},
			{value: signedRoundIntegerBits(-5), frac: signedRoundIntegerBits(-1), want: signedRoundIntegerBits(-10)},
			{value: signedRoundIntegerBits(-15), frac: signedRoundIntegerBits(-1), want: signedRoundIntegerBits(-20)},
			{value: signedRoundIntegerBits(-25), frac: signedRoundIntegerBits(-1), want: signedRoundIntegerBits(-30)},
			{value: 5, frac: signedRoundIntegerBits(-1), want: 10},
			{value: 15, frac: signedRoundIntegerBits(-1), want: 20},
			{value: 25, frac: signedRoundIntegerBits(-1), want: 30},
			{value: signedRoundIntegerBits(-150), frac: signedRoundIntegerBits(-2), want: signedRoundIntegerBits(-200)},
			{value: 149, frac: signedRoundIntegerBits(-2), want: 100},
			{value: 150, frac: signedRoundIntegerBits(-2), want: 200},
			{value: signedRoundIntegerBits(math.MinInt64), frac: signedRoundIntegerBits(-2), want: signedRoundIntegerBits(-9223372036854775800)},
			{value: 1, frac: signedRoundIntegerBits(math.MinInt64), want: 0},
		})
	})

	t.Run("unsigned value", func(t *testing.T) {
		testRoundIntegerCases(t, true, false, []roundIntegerTestCase{
			{value: 12345678901234567890, frac: 24, want: 12345678901234567890},
			{value: math.MaxUint64, frac: 24, want: math.MaxUint64},
			{value: 15, frac: signedRoundIntegerBits(-1), want: 20},
			{value: 25, frac: signedRoundIntegerBits(-1), want: 30},
			{value: 149, frac: signedRoundIntegerBits(-2), want: 100},
		})
	})

	t.Run("unsigned frac", func(t *testing.T) {
		testRoundIntegerCases(t, false, true, []roundIntegerTestCase{
			{value: signedRoundIntegerBits(-717754013), frac: math.MaxUint64, want: signedRoundIntegerBits(-717754013)},
			{value: 717754013, frac: 24, want: 717754013},
		})
	})

	t.Run("overflow", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			unsigned bool
			value    uint64
			frac     int64
		}{
			{name: "signed maximum", value: math.MaxInt64, frac: -1},
			{name: "signed minimum one digit", value: signedRoundIntegerBits(math.MinInt64), frac: -1},
			{name: "signed minimum three digits", value: signedRoundIntegerBits(math.MinInt64), frac: -3},
			{name: "unsigned maximum", unsigned: true, value: math.MaxUint64, frac: -1},
			{name: "unsigned nineteen digits", unsigned: true, value: math.MaxUint64, frac: -19},
		} {
			t.Run(tc.name, func(t *testing.T) {
				testRoundIntegerOverflow(t, tc.unsigned, tc.value, tc.frac)
			})
		}
	})
}
