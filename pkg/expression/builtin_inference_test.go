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
	if !enableStarterDeployModeForTest(t) {
		t.Skip("EMBED_TEXT is only supported in starter deployment mode")
	}
	ctx := mock.NewContext()
	withMockDefaultEmbedFn(t)

	fn, err := NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
	)
	require.NoError(t, err)
	vec, isNull, err := fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[1,2,3]", vec.String())

	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
		stringConst(`{"plus":1}`),
	)
	require.NoError(t, err)
	vec, isNull, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "[2,3,4]", vec.String())
}

func TestEmbedTextBuiltinNullAndErrors(t *testing.T) {
	ctx := mock.NewContext()
	withMockDefaultEmbedFn(t)
	enableNonStarterDeployModeForTest(t)

	fn, err := NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		stringConst("[1,2,3]"),
	)
	require.NoError(t, err)
	_, _, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.ErrorContains(t, err, "EMBED_TEXT is only supported in starter deployment mode")

	if !enableStarterDeployModeForTest(t) {
		return
	}

	fn, err = NewFunction(
		ctx,
		ast.EmbedText,
		types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		stringConst("mock/json"),
		nullStringConst(),
	)
	require.NoError(t, err)
	_, isNull, err := fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.NoError(t, err)
	require.True(t, isNull)

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
		stringConst(`[1,"bad"]`),
	)
	require.NoError(t, err)
	_, _, err = fn.EvalVectorFloat32(ctx.GetExprCtx().GetEvalCtx(), chunk.Row{})
	require.Error(t, err)
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

func enableStarterDeployModeForTest(t *testing.T) bool {
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

func enableNonStarterDeployModeForTest(t *testing.T) {
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
