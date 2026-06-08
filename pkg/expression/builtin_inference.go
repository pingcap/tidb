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
	"strings"

	"github.com/pingcap/tidb/pkg/config/deploymode"
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

// EmbedTextArgs contains the evaluated arguments for an EMBED_TEXT() invocation.
type EmbedTextArgs struct {
	Model string
	Text  string
	Opts  map[string]any
}

// Clone implements builtinFunc interface.
func (b *BuiltinEmbedTextSig) Clone() builtinFunc {
	newSig := &BuiltinEmbedTextSig{IsFromVecSearch: b.IsFromVecSearch}
	newSig.cloneFrom(&b.baseBuiltinFunc)
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
	sessionEvalCtx, ok := unwrapSessionEvalContext(ctx)
	if !ok {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	sessionCtx := sessionEvalCtx.Sctx()
	if !deploymode.IsStarter() {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT is only supported in starter deployment mode")
	}

	embedArgs, isNull, err := EvalEmbedTextArgs(ctx, row, b.args, b.IsFromVecSearch)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}

	embedding, err := domainadaptor.GetEmbedFn(sessionCtx).Embed(func() bool {
		// any kill signal should be handled.
		return sessionCtx.GetSessionVars().SQLKiller.GetKillSignal() > 0
	}, embedArgs.Model, embedArgs.Text, embedArgs.Opts)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	embeddingVec, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	return embeddingVec, false, nil
}

// EvalEmbedTextArgs evaluates EMBED_TEXT() arguments without calling the remote embedding provider.
func EvalEmbedTextArgs(ctx EvalContext, row chunk.Row, args []Expression, isFromVecSearch bool) (*EmbedTextArgs, bool, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, false, fmt.Errorf("invalid EMBED_TEXT() usage")
	}
	model, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	text, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	var opts map[string]any
	if len(args) == 3 {
		options, isNull, err := args[2].EvalString(ctx, row)
		if err != nil {
			return nil, false, err
		}
		if !isNull && len(options) > 0 {
			if err := json.Unmarshal([]byte(options), &opts); err != nil {
				return nil, false, fmt.Errorf("EMBED_TEXT expects options in JSON format")
			}
			searchOpts := make([]string, 0, len(opts))
			for k := range opts {
				if strings.HasSuffix(k, "@search") {
					searchOpts = append(searchOpts, k)
				}
			}
			for _, k := range searchOpts {
				if isFromVecSearch {
					opts[strings.TrimSuffix(k, "@search")] = opts[k]
				}
				delete(opts, k)
			}
		}
	}
	return &EmbedTextArgs{Model: model, Text: text, Opts: opts}, false, nil
}

// RequiredOptionalEvalProps implements RequiredOptionalEvalProps interface.
func (b *BuiltinEmbedTextSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func unwrapSessionEvalContext(ctx EvalContext) (*sessionexpr.EvalContext, bool) {
	if assertionCtx, ok := ctx.(*assertionEvalContext); ok {
		ctx = assertionCtx.EvalContext
	}
	sessionEvalCtx, ok := ctx.(*sessionexpr.EvalContext)
	return sessionEvalCtx, ok
}
