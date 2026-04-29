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
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/inference"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &embedTextFunctionClass{}
	_ builtinFunc   = &builtinEmbedTextSig{}
)

type embedTextFunctionClass struct {
	baseFunctionClass
}

type builtinEmbedTextSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

func (b *builtinEmbedTextSig) Clone() builtinFunc {
	newSig := &builtinEmbedTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *embedTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, argTps...)
	if err != nil {
		return nil, err
	}
	return &builtinEmbedTextSig{baseBuiltinFunc: bf}, nil
}

func (b *builtinEmbedTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	model, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	text, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}

	var opts map[string]any
	if len(b.args) == 3 {
		optText, isNull, err := b.args[2].EvalString(ctx, row)
		if err != nil {
			return types.ZeroVectorFloat32, false, err
		}
		if !isNull && len(optText) > 0 {
			var parsed any
			if err := json.Unmarshal([]byte(optText), &parsed); err != nil {
				return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT expects options to be a JSON object: %w", err)
			}
			parsedOpts, ok := parsed.(map[string]any)
			if !ok {
				return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT expects options to be a JSON object, got %s", jsonValueType(parsed))
			}
			opts = parsedOpts
		}
	}

	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}

	var shouldCancel func() bool
	if sessionVars != nil {
		shouldCancel = func() bool {
			return sessionVars.SQLKiller.GetKillSignal() > 0
		}
	}

	embedding, err := inference.DefaultRegistry().Embed(shouldCancel, model, text, opts)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}

	embeddingVec, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	return embeddingVec, false, nil
}

func jsonValueType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	default:
		return fmt.Sprintf("%T", value)
	}
}

func (b *builtinEmbedTextSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}
