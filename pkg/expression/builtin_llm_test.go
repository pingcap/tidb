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
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/llm"
	"github.com/stretchr/testify/require"
)

func TestLLMCompleteRequiresDefaultModel(t *testing.T) {
	ctx := createContext(t)
	restoreEnable := vardef.EnableLLMInference.Load()
	restoreModel := vardef.LLMDefaultModel.Load()
	t.Cleanup(func() {
		vardef.EnableLLMInference.Store(restoreEnable)
		vardef.LLMDefaultModel.Store(restoreModel)
	})

	vardef.EnableLLMInference.Store(true)
	vardef.LLMDefaultModel.Store("")

	arg := &Constant{Value: types.NewDatum("hi"), RetType: types.NewFieldType(mysql.TypeString)}
	fn, err := NewFunction(ctx, ast.LLMComplete, types.NewFieldType(mysql.TypeString), arg)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)

	_, _, err = sf.EvalString(ctx, chunk.Row{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tidb_llm_default_model")
}

func TestLLMCompleteHookReturnsText(t *testing.T) {
	ctx := createContext(t)
	restoreEnable := vardef.EnableLLMInference.Load()
	restoreModel := vardef.LLMDefaultModel.Load()
	restoreMaxTokens := vardef.LLMMaxTokens.Load()
	restoreTemperature := vardef.LLMTemperature.Load()
	restoreTopP := vardef.LLMTopP.Load()
	restoreHook := SetLLMCompleteHookForTest(func(model, prompt string, opts llm.CompleteOptions) (string, error) {
		if model != "m1" || prompt != "hi" {
			return "", errors.New("unexpected prompt")
		}
		require.Equal(t, 256, opts.MaxTokens)
		require.NotNil(t, opts.Temperature)
		require.NotNil(t, opts.TopP)
		require.Equal(t, 0.2, *opts.Temperature)
		require.Equal(t, 0.9, *opts.TopP)
		return "ok", nil
	})
	t.Cleanup(func() {
		vardef.EnableLLMInference.Store(restoreEnable)
		vardef.LLMDefaultModel.Store(restoreModel)
		vardef.LLMMaxTokens.Store(restoreMaxTokens)
		vardef.LLMTemperature.Store(restoreTemperature)
		vardef.LLMTopP.Store(restoreTopP)
		restoreHook()
	})

	vardef.EnableLLMInference.Store(true)
	vardef.LLMDefaultModel.Store("m1")
	vardef.LLMMaxTokens.Store(256)
	vardef.LLMTemperature.Store(0.2)
	vardef.LLMTopP.Store(0.9)

	arg := &Constant{Value: types.NewDatum("hi"), RetType: types.NewFieldType(mysql.TypeString)}
	fn, err := NewFunction(ctx, ast.LLMComplete, types.NewFieldType(mysql.TypeString), arg)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)

	val, isNull, err := sf.EvalString(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, "ok", val)
}

func TestLLMEmbedHookReturnsVector(t *testing.T) {
	ctx := createContext(t)
	restoreEnable := vardef.EnableLLMInference.Load()
	restoreModel := vardef.LLMDefaultModel.Load()
	restoreHook := SetLLMEmbedHookForTest(func(model, text string) ([]float32, error) {
		if model != "m1" || text != "hi" {
			return nil, errors.New("unexpected text")
		}
		return []float32{0.1, 0.2}, nil
	})
	t.Cleanup(func() {
		vardef.EnableLLMInference.Store(restoreEnable)
		vardef.LLMDefaultModel.Store(restoreModel)
		restoreHook()
	})

	vardef.EnableLLMInference.Store(true)
	vardef.LLMDefaultModel.Store("m1")

	arg := &Constant{Value: types.NewDatum("hi"), RetType: types.NewFieldType(mysql.TypeString)}
	fn, err := NewFunction(ctx, ast.LLMEmbedText, types.NewFieldType(mysql.TypeTiDBVectorFloat32), arg)
	require.NoError(t, err)
	sf := fn.(*ScalarFunction)

	vec, isNull, err := sf.EvalVectorFloat32(ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, []float32{0.1, 0.2}, vec.Elements())
}
