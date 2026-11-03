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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/inference"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestEmbedTextBuiltin(t *testing.T) {
	if !enableStarterDeployModeForEmbedTest(t) {
		t.Skip("EMBED_TEXT is only supported in starter deployment mode")
	}
	ctx := mock.NewContext()
	withMockDefaultEmbedFn(t)

	for _, tc := range []struct {
		options string
		want    string
	}{
		{want: "[1,2,3]"},
		{options: `{"plus":1}`, want: "[2,3,4]"},
		{options: `{"plus":1,"plus@search":10}`, want: "[2,3,4]"},
	} {
		args := []Expression{stringConst("mock/json"), stringConst("[1,2,3]")}
		if tc.options != "" {
			args = append(args, stringConst(tc.options))
		}
		fn, err := NewFunction(ctx, ast.EmbedText, types.NewFieldType(mysql.TypeTiDBVectorFloat32), args...)
		require.NoError(t, err)
		vec, isNull, err := fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, tc.want, vec.String())
	}

	// EMBED_TEXT obtains its session context through optional properties, so an
	// EvalContext wrapper must not make the session/domain runtime unavailable.
	wrappedCtx := struct{ EvalContext }{EvalContext: ctx.GetExprCtx().GetEvalCtx()}
	fn, err := NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
	)
	require.NoError(t, err)
	vec, isNull, err := fn.EvalVectorFloat32(wrappedCtx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[1,2,3]", vec.String())
}

func TestEmbedTextBuiltinNullAndErrors(t *testing.T) {
	ctx := mock.NewContext()
	withMockDefaultEmbedFn(t)
	enableNonStarterDeployModeForEmbedTest(t)
	evalCtx := ctx.GetExprCtx().GetEvalCtx()

	_, _, err := EvalEmbedTextArgs(evalCtx, chunk.Row{}, nil)
	require.ErrorContains(t, err, "invalid EMBED_TEXT() usage")
	_, _, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"), stringConst("[1,2,3]"), stringConst("{}"), stringConst("extra"),
	})
	require.ErrorContains(t, err, "invalid EMBED_TEXT() usage")

	embedArgs, isNull, err := EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
		stringConst(`{"plus":1,"plus@search":10}`),
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, &EmbedTextArgs{
		Model: "mock/json",
		Text:  "[1,2,3]",
		Opts:  map[string]any{"plus": float64(1)},
	}, embedArgs)

	embedArgs, isNull, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"), stringConst("[1,2,3]"), nullStringConst(),
	})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Nil(t, embedArgs.Opts)

	_, isNull, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		nullStringConst(), stringConst("[1,2,3]"),
	})
	require.NoError(t, err)
	require.True(t, isNull)
	_, isNull, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"), nullStringConst(),
	})
	require.NoError(t, err)
	require.True(t, isNull)

	_, _, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		&MockExpr{i: "mock/json", err: types.ErrOverflow}, stringConst("[1,2,3]"),
	})
	require.ErrorIs(t, err, types.ErrOverflow)
	_, _, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"), &MockExpr{i: "[1,2,3]", err: types.ErrOverflow},
	})
	require.ErrorIs(t, err, types.ErrOverflow)
	_, _, err = EvalEmbedTextArgs(evalCtx, chunk.Row{}, []Expression{
		stringConst("mock/json"), stringConst("[1,2,3]"), &MockExpr{i: "{}", err: types.ErrOverflow},
	})
	require.ErrorIs(t, err, types.ErrOverflow)

	_, _, err = EvalEmbedTextArgsFromExpr(evalCtx, chunk.Row{}, stringConst("not a function"))
	require.ErrorContains(t, err, "expects EMBED_TEXT()")
	_, _, err = EvalEmbedTextArgsFromExpr(evalCtx, chunk.Row{}, &ScalarFunction{})
	require.ErrorContains(t, err, "expects EMBED_TEXT()")

	_, err = EvalEmbedTextArgsToDatum(nil, nil, &EmbedTextArgs{})
	require.ErrorContains(t, err, "requires session context")

	fn, err := NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
	)
	require.NoError(t, err)
	parsedArgs, isNull, err := EvalEmbedTextArgsFromExpr(evalCtx, chunk.Row{}, fn)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "mock/json", parsedArgs.Model)
	require.Equal(t, "[1,2,3]", parsedArgs.Text)

	_, _, err = fn.EvalVectorFloat32(evalCtx, chunk.Row{})
	require.ErrorContains(t, err, "EMBED_TEXT is only supported in starter deployment mode")
	_, err = EvalEmbedTextArgsToDatum(nil, ctx, parsedArgs)
	require.ErrorContains(t, err, "EMBED_TEXT is only supported in starter deployment mode")

	if !enableStarterDeployModeForEmbedTest(t) {
		return
	}
	_, err = EvalEmbedTextArgsToDatum(nil, ctx, nil)
	require.ErrorContains(t, err, "invalid EMBED_TEXT() usage")
	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		nullStringConst(),
		stringConst("[1,2,3]"),
	)
	require.NoError(t, err)
	_, isNull, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		nullStringConst(),
	)
	require.NoError(t, err)
	_, isNull, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

	for _, options := range []*Constant{nullStringConst(), stringConst("")} {
		fn, err = NewFunction(
			ctx,
			ast.EmbedText,
			types.NewFieldType(mysql.TypeTiDBVectorFloat32),
			stringConst("mock/json"),
			stringConst("[1,2,3]"),
			options,
		)
		require.NoError(t, err)
		vec, isNull, err := fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, "[1,2,3]", vec.String())
	}

	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
		stringConst(`{invalid_json}`),
	)
	require.NoError(t, err)
	_, _, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.ErrorContains(t, err, "EMBED_TEXT expects options in JSON format")

	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
		stringConst(`null`),
	)
	require.NoError(t, err)
	_, _, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.ErrorContains(t, err, "EMBED_TEXT expects options in JSON format")

	oversizedVector := "[" + strings.Repeat("0,", 16383) + "0]"
	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst(oversizedVector),
	)
	require.NoError(t, err)
	_, _, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.ErrorContains(t, err, "vector cannot have more than 16383 dimensions")
}

func stringConst(value string) *Constant {
	return &Constant{
		Value:   types.NewDatum(value),
		RetType: types.NewFieldType(mysql.TypeString),
	}
}

func nullStringConst() *Constant {
	return &Constant{
		Value:   types.NewDatum(nil),
		RetType: types.NewFieldType(mysql.TypeString),
	}
}

func withMockDefaultEmbedFn(t *testing.T) {
	t.Helper()
	embedFn := inference.NewEmbedFn()
	if !embedFn.HasEmbedder("mock") {
		embedFn.MustRegisterEmbedder("mock", inference.NewMockEmbedder())
	}
	t.Cleanup(inference.SetDefaultEmbedFnForTest(embedFn))
}

func enableStarterDeployModeForEmbedTest(t *testing.T) bool {
	t.Helper()
	if !kerneltype.IsNextGen() {
		return false
	}
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})
	return true
}

func enableNonStarterDeployModeForEmbedTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		return
	}
	originalMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Premium))
	t.Cleanup(func() {
		require.NoError(t, deploymode.Set(originalMode))
	})
}
