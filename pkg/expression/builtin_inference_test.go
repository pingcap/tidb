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
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/inference"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type embedTextTestEmbedder struct {
	embeddings [][]float32
}

func (e embedTextTestEmbedder) CreateEmbeddings(context.Context, string, []string, map[string]any) ([][]float32, error) {
	return e.embeddings, nil
}

func TestEmbedTextBuiltin(t *testing.T) {
	ctx := createContext(t)
	registerEmbedTextTestEmbedder(t, "mock", inference.NewMockEmbedder())

	f := buildEmbedTextFuncForTest(t, ctx, "mock/json", "[1,2,3]", `{"plus":2}`)
	require.Equal(t, types.ETVectorFloat32, f.getRetTp().EvalType())
	require.Equal(t, exprctx.OptPropSessionVars.AsPropKeySet(), f.RequiredOptionalEvalProps())
	require.IsType(t, &builtinEmbedTextSig{}, f.Clone())

	vec, isNull, err := f.evalVectorFloat32(wrapEvalAssert(ctx.GetEvalCtx(), f), chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[3,4,5]", vec.String())

	vec, isNull, err = evalEmbedTextFuncForTest(t, ctx, "mock/json", "[4,5]")
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[4,5]", vec.String())

	vec, isNull, err = evalEmbedTextFuncForTest(t, ctx, "mock/json", "[6]", nil)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[6]", vec.String())

	vec, isNull, err = evalEmbedTextFuncForTest(t, ctx, "mock/json", "[7]", "")
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[7]", vec.String())

	_, isNull, err = evalEmbedTextFuncForTest(t, ctx, nil, "[1]")
	require.NoError(t, err)
	require.True(t, isNull)

	_, isNull, err = evalEmbedTextFuncForTest(t, ctx, "mock/json", nil)
	require.NoError(t, err)
	require.True(t, isNull)

	_, err = funcs[ast.EmbedText].getFunction(ctx, primitiveValsToConstants(ctx, []any{"mock/json"}))
	require.ErrorContains(t, err, "Incorrect parameter count")

	_, err = funcs[ast.EmbedText].getFunction(ctx, primitiveValsToConstants(ctx, []any{"mock/json", "[1]", `{}`, "extra"}))
	require.ErrorContains(t, err, "Incorrect parameter count")
}

func TestEmbedTextBuiltinOptionErrors(t *testing.T) {
	ctx := createContext(t)
	registerEmbedTextTestEmbedder(t, "mock", inference.NewMockEmbedder())

	tests := []struct {
		option  string
		wantErr string
	}{
		{option: "not-json", wantErr: "EMBED_TEXT expects options to be a JSON object"},
		{option: "null", wantErr: "EMBED_TEXT expects options to be a JSON object, got null"},
		{option: "[]", wantErr: "EMBED_TEXT expects options to be a JSON object, got array"},
		{option: `"x"`, wantErr: "EMBED_TEXT expects options to be a JSON object, got string"},
		{option: "1", wantErr: "EMBED_TEXT expects options to be a JSON object, got number"},
		{option: "true", wantErr: "EMBED_TEXT expects options to be a JSON object, got boolean"},
	}
	for _, tt := range tests {
		_, _, err := evalEmbedTextFuncForTest(t, ctx, "mock/json", "[1]", tt.option)
		require.ErrorContains(t, err, tt.wantErr)
	}

	require.Equal(t, "object", jsonValueType(map[string]any{}))
	require.Equal(t, "struct {}", jsonValueType(struct{}{}))
}

func TestEmbedTextBuiltinEmbeddingErrors(t *testing.T) {
	ctx := createContext(t)
	registerEmbedTextTestEmbedder(t, "exprtest_nan", embedTextTestEmbedder{
		embeddings: [][]float32{{float32(math.NaN())}},
	})

	_, _, err := evalEmbedTextFuncForTest(t, ctx, "exprtest_nan/model", "hello")
	require.ErrorContains(t, err, "NaN not allowed in vector")
}

func buildEmbedTextFuncForTest(t *testing.T, ctx BuildContext, args ...any) builtinFunc {
	t.Helper()
	f, err := funcs[ast.EmbedText].getFunction(ctx, primitiveValsToConstants(ctx, args))
	require.NoError(t, err)
	return f
}

func evalEmbedTextFuncForTest(t *testing.T, ctx BuildContext, args ...any) (types.VectorFloat32, bool, error) {
	t.Helper()
	f := buildEmbedTextFuncForTest(t, ctx, args...)
	return f.evalVectorFloat32(wrapEvalAssert(ctx.GetEvalCtx(), f), chunk.Row{})
}

func registerEmbedTextTestEmbedder(t *testing.T, provider string, embedder inference.Embedder) {
	t.Helper()
	if _, ok := inference.DefaultRegistry().GetEmbedder(provider); ok {
		return
	}
	require.NoError(t, inference.DefaultRegistry().RegisterEmbedder(provider, embedder))
}
