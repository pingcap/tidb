// Copyright 2025 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/inference/domainadaptor"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &embedTextFunctionClass{}
)

var (
	_ builtinFunc = &BuiltinEmbedTextSig{}
)

type embedTextFunctionClass struct {
	baseFunctionClass
}

// BuiltinEmbedTextSig is the signature for the `EMBED_TEXT` function.
type BuiltinEmbedTextSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader

	// IsFromVecSearch indicates whether this function is used with a vector search
	// (comes from VEC_EMBED_XXX_DISTANCE).
	// If true, the function will respect json options with @search suffix.
	// If false, json options with @search suffix will be ignored.
	IsFromVecSearch bool
}

// Clone implements builtinFunc interface.
func (b *BuiltinEmbedTextSig) Clone() builtinFunc {
	newSig := &BuiltinEmbedTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.IsFromVecSearch = b.IsFromVecSearch
	return newSig
}

func (c *embedTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	// args[0]: model name
	// args[1]: text to embed
	// args[2]: [optional] json options

	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETString, types.ETString)
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, argTps...)
	if err != nil {
		return nil, err
	}
	sig := &BuiltinEmbedTextSig{
		baseBuiltinFunc: bf,
		IsFromVecSearch: false,
	}
	// sig.setPbCode(tipb.ScalarFuncSig_EmbedTextSig)
	return sig, nil
}

func (b *BuiltinEmbedTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	model, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	text, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	var opts map[string]any = nil
	if len(b.args) == 3 {
		options, isNull, err := b.args[2].EvalString(ctx, row)
		if err != nil {
			return types.ZeroVectorFloat32, false, err
		}
		if !isNull && len(options) > 0 {
			// Parse the options (which must be a JSON string) into a map
			err := json.Unmarshal([]byte(options), &opts)
			if err != nil {
				return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT expects options in JSON format")
			}
			// Special treatment for options with @search suffix:
			// - If this function is used in a vector search context (i.e., VEC_EMBED_XXX_DISTANCE),
			//   @search options will take effect.
			// - Otherwise, @search options will be ignored.
			//
			// To avoid mutating the map while iterating, we collect the keys first.
			searchOpts := make([]string, 0, len(opts))
			for k := range opts {
				if strings.HasSuffix(k, "@search") {
					searchOpts = append(searchOpts, k)
				}
			}
			for _, k := range searchOpts {
				if b.IsFromVecSearch {
					opts[strings.TrimSuffix(k, "@search")] = opts[k]
				}
				delete(opts, k)
			}
		}
	}
	sessionEvalCtx, ok := ctx.(*sessionexpr.EvalContext)
	if !ok {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	sessionCtx := sessionEvalCtx.Sctx()

	embedding, err := domainadaptor.GetEmbedFn(sessionCtx).Embed(func() bool {
		// any kill signal should be handled.
		return sessionCtx.GetSessionVars().SQLKiller.GetKillSignal() > 0
	}, model, text, opts)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	embeddingVec, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	return embeddingVec, false, nil
}

// RequiredOptionalEvalProps implements RequiredOptionalEvalProps interface.
func (b *BuiltinEmbedTextSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}
