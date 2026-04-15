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

package util

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// TestNullRejectBuiltinRegistrySnapshot guards against silent builtin registry
// drift. When this hash breaks, the builtin set has changed — review whether
// new functions should be added to nullRejectNullPreservingFunctions or
// nullRejectRejectNullTests in null_misc_builtins.go.
func TestNullRejectBuiltinRegistrySnapshot(t *testing.T) {
	names := expression.RegisteredBuiltinFunctionNames()
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))

	require.NotEmpty(t, names)
	require.Equal(t, "a5ce0716b778fb8e0b488d3a11c402d8a8224191757a9e02ece80895d5d67e05", hex.EncodeToString(sum[:]))

	for name := range nullRejectRejectNullTests {
		require.Contains(t, names, name)
	}
}

func TestIsNullRejectedProofModes(t *testing.T) {
	sctx := mock.NewContext()
	require.NoError(t, sctx.GetSessionVars().SetSystemVar(vardef.BlockEncryptionMode, "aes-128-ecb"))
	exprCtx := sctx.GetExprCtx()

	innerA := newNullRejectIntColumn(1)
	innerB := newNullRejectIntColumn(2)
	outerC := newNullRejectIntColumn(3)
	innerS := newNullRejectStringColumn(4)
	innerUnsignedD := newNullRejectUintColumn(5)
	innerSchema := expression.NewSchema(innerA, innerB, innerS, innerUnsignedD)

	gtInnerAZero := newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), innerA, expression.NewZero())
	eqInnerAZero := newNullRejectFunc(t, exprCtx, ast.EQ, types.NewFieldType(mysql.TypeTiny), innerA, expression.NewZero())
	gtOuterCZero := newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), outerC, expression.NewZero())
	likeWrappedInnerA := newNullRejectLike(t, exprCtx, expression.BuildCastFunction(exprCtx, innerA, types.NewFieldType(mysql.TypeVarString)))
	coalesceInnerA := newNullRejectFunc(t, exprCtx, ast.Coalesce, types.NewFieldType(mysql.TypeLonglong), innerA, expression.NewOne())
	coalesceInnerATwo := newNullRejectFunc(t, exprCtx, ast.Coalesce, types.NewFieldType(mysql.TypeLonglong), innerA, newNullRejectIntConst(2))
	nullSafeEqInnerA := newNullRejectFunc(t, exprCtx, ast.NullEQ, types.NewFieldType(mysql.TypeTiny), innerA, expression.NewOne())
	fieldInnerA := newNullRejectFunc(t, exprCtx, ast.Field, types.NewFieldType(mysql.TypeLonglong), innerA, expression.NewOne())
	formatNullLocaleEq := newNullRejectFunc(t, exprCtx, ast.EQ, types.NewFieldType(mysql.TypeTiny),
		newNullRejectFunc(t, exprCtx, ast.Format, types.NewFieldType(mysql.TypeVarString),
			newNullRejectIntConst(12345),
			newNullRejectIntConst(0),
			innerS,
		),
		newNullRejectStringConst("12,345"),
	)
	quoteInnerSLikeA := newNullRejectFunc(t, exprCtx, ast.Like, types.NewFieldType(mysql.TypeTiny),
		newNullRejectFunc(t, exprCtx, ast.Quote, types.NewFieldType(mysql.TypeVarString), innerS),
		newNullRejectStringConst("A%"),
		newNullRejectIntConst(92),
	)
	issue66824LikePredicate := newNullRejectFunc(t, exprCtx, ast.GE, types.NewFieldType(mysql.TypeTiny),
		expression.NewOne(),
		newNullRejectFunc(t, exprCtx, ast.LogicAnd, types.NewFieldType(mysql.TypeTiny),
			newNullRejectFunc(t, exprCtx, ast.LogicOr, types.NewFieldType(mysql.TypeTiny), innerA, expression.NewNull()),
			newNullRejectFunc(t, exprCtx, ast.NE, types.NewFieldType(mysql.TypeTiny), outerC, outerC),
		),
	)
	ifInnerANullThenZeroElseOuterC := newNullRejectFunc(t, exprCtx, ast.If, types.NewFieldType(mysql.TypeLonglong),
		newNullRejectFunc(t, exprCtx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), innerA),
		expression.NewZero(),
		outerC,
	)
	truncateUnsignedByNullableScale := newNullRejectFunc(
		t,
		exprCtx,
		ast.Truncate,
		newNullRejectUintFieldType(mysql.TypeLonglong),
		newNullRejectUintConst(123),
		innerUnsignedD,
	)
	aesEncryptIgnoringNullableIV := newNullRejectFunc(
		t,
		exprCtx,
		ast.AesEncrypt,
		types.NewFieldType(mysql.TypeVarString),
		newNullRejectStringConst("pingcap"),
		newNullRejectStringConst("123"),
		innerS,
	)
	aesDecryptIgnoringNullableIV := newNullRejectFunc(
		t,
		exprCtx,
		ast.AesDecrypt,
		types.NewFieldType(mysql.TypeVarString),
		newNullRejectFunc(t, exprCtx, ast.Unhex, types.NewFieldType(mysql.TypeVarString), newNullRejectStringConst("996E0CA8688D7AD20819B90B273E01C6")),
		newNullRejectStringConst("123"),
		innerS,
	)
	jsonSetNullableValue := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONSet,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `{}`),
		newNullRejectStringConst("$.a"),
		innerS,
	)
	jsonInsertNullableValue := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONInsert,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `{}`),
		newNullRejectStringConst("$.a"),
		innerS,
	)
	jsonReplaceNullableValue := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONReplace,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `{"a": 1}`),
		newNullRejectStringConst("$.a"),
		innerS,
	)
	jsonArrayAppendNullableValue := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONArrayAppend,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `[]`),
		newNullRejectStringConst("$"),
		innerS,
	)
	jsonArrayInsertNullableValue := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONArrayInsert,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `[]`),
		newNullRejectStringConst("$[0]"),
		innerS,
	)
	jsonMergePatchNullableDoc := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONMergePatch,
		types.NewFieldType(mysql.TypeJSON),
		expression.BuildCastFunction(exprCtx, innerS, types.NewFieldType(mysql.TypeJSON)),
		newNullRejectJSONConst(t, `null`),
		newNullRejectJSONConst(t, `{"a": 1}`),
		newNullRejectJSONConst(t, `[1, 2, 3]`),
	)
	jsonSearchNullableEscape := newNullRejectFunc(
		t,
		exprCtx,
		ast.JSONSearch,
		types.NewFieldType(mysql.TypeJSON),
		newNullRejectJSONConst(t, `["abc"]`),
		newNullRejectStringConst("one"),
		newNullRejectStringConst("abc"),
		innerS,
	)

	cases := []struct {
		name     string
		expr     expression.Expression
		expected bool
	}{
		{
			name: "or_needs_both_sides_non_true",
			expr: newNullRejectFunc(t, exprCtx, ast.LogicOr, types.NewFieldType(mysql.TypeTiny),
				gtInnerAZero,
				newNullRejectFunc(t, exprCtx, ast.LogicAnd, types.NewFieldType(mysql.TypeTiny), eqInnerAZero, gtOuterCZero)),
			expected: true,
		},
		{
			name:     "not_uses_must_null",
			expr:     newNullRejectFunc(t, exprCtx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), gtInnerAZero),
			expected: true,
		},
		{
			name:     "is_null_accepts_null",
			expr:     newNullRejectFunc(t, exprCtx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), innerA),
			expected: false,
		},
		{
			name:     "is_true_rejects_null",
			expr:     newNullRejectFunc(t, exprCtx, ast.IsTruthWithNull, types.NewFieldType(mysql.TypeTiny), innerA),
			expected: true,
		},
		{
			name: "not_is_null_rejects_null",
			expr: newNullRejectFunc(
				t,
				exprCtx,
				ast.UnaryNot,
				types.NewFieldType(mysql.TypeTiny),
				newNullRejectFunc(t, exprCtx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), innerA),
			),
			expected: true,
		},
		{
			name:     "null_preserving_wrapper_propagates_must_null",
			expr:     likeWrappedInnerA,
			expected: true,
		},
		{
			name:     "coalesce_constant_fallback_can_still_be_non_true",
			expr:     newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), coalesceInnerATwo, newNullRejectIntConst(2)),
			expected: true,
		},
		{
			name:     "null_hiding_wrapper_stays_conservative",
			expr:     newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), coalesceInnerA, expression.NewZero()),
			expected: false,
		},
		{
			name: "in_with_all_list_items_null_rejected",
			expr: newNullRejectFunc(t, exprCtx, ast.In, types.NewFieldType(mysql.TypeTiny),
				expression.NewOne(), innerA, innerB),
			expected: true,
		},
		{
			name: "in_with_non_null_candidate_is_not_proven",
			expr: newNullRejectFunc(t, exprCtx, ast.In, types.NewFieldType(mysql.TypeTiny),
				expression.NewOne(), innerA, expression.NewOne()),
			expected: false,
		},
		{
			name:     "null_safe_eq_with_non_null_constant_rejects_null",
			expr:     nullSafeEqInnerA,
			expected: true,
		},
		{
			name:     "format_with_null_locale_is_not_null_rejected",
			expr:     formatNullLocaleEq,
			expected: false,
		},
		{
			name: "field_with_null_input_can_make_not_predicate_true",
			expr: newNullRejectFunc(t, exprCtx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny),
				newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), fieldInnerA, expression.NewZero())),
			expected: false,
		},
		{
			name:     "quote_with_null_input_can_make_not_predicate_true",
			expr:     newNullRejectFunc(t, exprCtx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), quoteInnerSLikeA),
			expected: false,
		},
		{
			// Issue #66824: an outer comparison can still evaluate TRUE when the
			// inner AND branch is merely nonTrue rather than mustNull.
			name:     "comparison_over_non_true_and_is_not_null_rejected",
			expr:     issue66824LikePredicate,
			expected: false,
		},
		{
			name:     "if_condition_folded_after_nullification_stays_provable",
			expr:     newNullRejectFunc(t, exprCtx, ast.GT, types.NewFieldType(mysql.TypeTiny), ifInnerANullThenZeroElseOuterC, expression.NewZero()),
			expected: true,
		},
		{
			name: "truncate_with_unsigned_nullable_scale_is_not_null_preserving",
			expr: newNullRejectFunc(
				t,
				exprCtx,
				ast.GT,
				types.NewFieldType(mysql.TypeTiny),
				truncateUnsignedByNullableScale,
				expression.NewZero(),
			),
			expected: false,
		},
		{
			name:     "aes_encrypt_ignores_nullable_iv_in_ecb_mode",
			expr:     newNullRejectNotNull(t, exprCtx, aesEncryptIgnoringNullableIV),
			expected: false,
		},
		{
			name:     "aes_decrypt_ignores_nullable_iv_in_ecb_mode",
			expr:     newNullRejectNotNull(t, exprCtx, aesDecryptIgnoringNullableIV),
			expected: false,
		},
		{
			name:     "json_set_nullable_value_becomes_json_null",
			expr:     newNullRejectNotNull(t, exprCtx, jsonSetNullableValue),
			expected: false,
		},
		{
			name:     "json_insert_nullable_value_becomes_json_null",
			expr:     newNullRejectNotNull(t, exprCtx, jsonInsertNullableValue),
			expected: false,
		},
		{
			name:     "json_replace_nullable_value_becomes_json_null",
			expr:     newNullRejectNotNull(t, exprCtx, jsonReplaceNullableValue),
			expected: false,
		},
		{
			name:     "json_array_append_nullable_value_becomes_json_null",
			expr:     newNullRejectNotNull(t, exprCtx, jsonArrayAppendNullableValue),
			expected: false,
		},
		{
			name:     "json_array_insert_nullable_value_becomes_json_null",
			expr:     newNullRejectNotNull(t, exprCtx, jsonArrayInsertNullableValue),
			expected: false,
		},
		{
			name:     "json_merge_patch_nullable_argument_can_still_return_document",
			expr:     newNullRejectNotNull(t, exprCtx, jsonMergePatchNullableDoc),
			expected: false,
		},
		{
			name:     "json_search_nullable_escape_falls_back_to_default_escape",
			expr:     newNullRejectNotNull(t, exprCtx, jsonSearchNullableEscape),
			expected: false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, IsNullRejected(sctx, innerSchema, tt.expr, true))
		})
	}
}

func newNullRejectIntColumn(id int64) *expression.Column {
	return &expression.Column{
		UniqueID: id,
		ID:       id,
		Index:    int(id),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
}

func newNullRejectStringColumn(id int64) *expression.Column {
	return &expression.Column{
		UniqueID: id,
		ID:       id,
		Index:    int(id),
		RetType:  types.NewFieldType(mysql.TypeVarString),
	}
}

func newNullRejectUintColumn(id int64) *expression.Column {
	return &expression.Column{
		UniqueID: id,
		ID:       id,
		Index:    int(id),
		RetType:  newNullRejectUintFieldType(mysql.TypeLonglong),
	}
}

func newNullRejectStringConst(value string) *expression.Constant {
	return &expression.Constant{
		Value:   types.NewStringDatum(value),
		RetType: types.NewFieldType(mysql.TypeVarString),
	}
}

func newNullRejectIntConst(value int64) *expression.Constant {
	return &expression.Constant{
		Value:   types.NewIntDatum(value),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func newNullRejectUintConst(value uint64) *expression.Constant {
	return &expression.Constant{
		Value:   types.NewUintDatum(value),
		RetType: newNullRejectUintFieldType(mysql.TypeLonglong),
	}
}

func newNullRejectUintFieldType(tp byte) *types.FieldType {
	fieldType := types.NewFieldType(tp)
	fieldType.AddFlag(mysql.UnsignedFlag)
	return fieldType
}

func newNullRejectFunc(t *testing.T, ctx expression.BuildContext, name string, retType *types.FieldType, args ...expression.Expression) expression.Expression {
	expr, err := expression.NewFunction(ctx, name, retType, args...)
	require.NoError(t, err)
	return expr
}

func newNullRejectJSONConst(t *testing.T, value string) *expression.Constant {
	jsonValue, err := types.ParseBinaryJSONFromString(value)
	require.NoError(t, err)
	return &expression.Constant{
		Value:   types.NewJSONDatum(jsonValue),
		RetType: types.NewFieldType(mysql.TypeJSON),
	}
}

func newNullRejectNotNull(t *testing.T, ctx expression.BuildContext, arg expression.Expression) expression.Expression {
	return newNullRejectFunc(
		t,
		ctx,
		ast.UnaryNot,
		types.NewFieldType(mysql.TypeTiny),
		newNullRejectFunc(t, ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), arg),
	)
}

func newNullRejectLike(t *testing.T, ctx expression.BuildContext, arg expression.Expression) expression.Expression {
	return newNullRejectFunc(
		t,
		ctx,
		ast.Like,
		types.NewFieldType(mysql.TypeTiny),
		newNullRejectFunc(t, ctx, ast.Trim, types.NewFieldType(mysql.TypeVarString), arg),
		newNullRejectStringConst("1%"),
		newNullRejectIntConst(92),
	)
}
