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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/inference/domainadaptor"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

var (
	_ functionClass = &embedTextFunctionClass{}
)

var (
	_ builtinFunc = &builtinEmbedTextSig{}
)

type embedTextFunctionClass struct {
	baseFunctionClass
}

type builtinEmbedTextSig struct {
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
func (b *builtinEmbedTextSig) Clone() builtinFunc {
	newSig := &builtinEmbedTextSig{IsFromVecSearch: b.IsFromVecSearch}
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
	sig := &builtinEmbedTextSig{
		baseBuiltinFunc: bf,
		IsFromVecSearch: false,
	}
	// sig.setPbCode(tipb.ScalarFuncSig_EmbedTextSig)
	return sig, nil
}

func (b *builtinEmbedTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (res types.VectorFloat32, isNull bool, err error) {
	sessionEvalCtx, ok := unwrapSessionEvalContext(ctx)
	if !ok {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	sessionCtx := sessionEvalCtx.Sctx()
	datum, isNull, err := EvalEmbedTextDatum(sessionCtx.GetTraceCtx(), sessionCtx, ctx, row, b.args, b.IsFromVecSearch)
	if err != nil || isNull {
		return types.ZeroVectorFloat32, isNull, err
	}
	return datum.GetVectorFloat32(), false, nil
}

// MarkEmbedTextFromVecSearch marks an EMBED_TEXT() scalar function synthesized
// from a VEC_EMBED_*_DISTANCE() expression.
func MarkEmbedTextFromVecSearch(sf *ScalarFunction) error {
	if sf == nil {
		return fmt.Errorf("unexpected nil function for embed_text")
	}
	embedSig, ok := sf.Function.(*builtinEmbedTextSig)
	if !ok {
		return fmt.Errorf("unexpected function signature for embed_text: %T", sf.Function)
	}
	embedSig.IsFromVecSearch = true
	return nil
}

// EvalEmbedTextArgsFromExpression evaluates arguments from a direct EMBED_TEXT()
// scalar expression without calling the remote embedding provider.
func EvalEmbedTextArgsFromExpression(ctx EvalContext, row chunk.Row, expr Expression) (*EmbedTextArgs, bool, error) {
	sf, ok := expr.(*ScalarFunction)
	if !ok {
		return nil, false, fmt.Errorf("auto-embedding generated column expects EMBED_TEXT()")
	}
	embedSig, ok := sf.Function.(*builtinEmbedTextSig)
	if !ok {
		return nil, false, fmt.Errorf("auto-embedding generated column expects EMBED_TEXT()")
	}
	return EvalEmbedTextArgs(ctx, row, sf.GetArgs(), embedSig.IsFromVecSearch)
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

// CheckEmbedTextAllowed validates deployment-level EMBED_TEXT() availability.
func CheckEmbedTextAllowed() error {
	if !deploymode.IsStarter() {
		return fmt.Errorf("EMBED_TEXT is only supported in starter deployment mode")
	}
	return nil
}

// EvalEmbedTextDatum evaluates EMBED_TEXT() to a vector datum using the same runtime contract
// for direct expression evaluation and generated-column materialization.
func EvalEmbedTextDatum(ctx context.Context, sctx sessionctx.Context, evalCtx EvalContext, row chunk.Row, args []Expression, isFromVecSearch bool) (types.Datum, bool, error) {
	if sctx == nil {
		return types.Datum{}, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	if err := CheckEmbedTextAllowed(); err != nil {
		return types.Datum{}, false, err
	}
	embedArgs, isNull, err := EvalEmbedTextArgs(evalCtx, row, args, isFromVecSearch)
	if isNull || err != nil {
		return types.Datum{}, isNull, err
	}
	datum, err := EvalEmbedTextArgsToDatum(ctx, sctx, embedArgs)
	return datum, false, err
}

// EvalEmbedTextArgsToDatum materializes already-evaluated EMBED_TEXT() arguments to a vector datum.
func EvalEmbedTextArgsToDatum(ctx context.Context, sctx sessionctx.Context, embedArgs *EmbedTextArgs) (types.Datum, error) {
	if sctx == nil {
		return types.Datum{}, fmt.Errorf("EMBED_TEXT requires session context")
	}
	if err := CheckEmbedTextAllowed(); err != nil {
		return types.Datum{}, err
	}
	if embedArgs == nil {
		return types.Datum{}, fmt.Errorf("invalid EMBED_TEXT() usage")
	}
	embedding, err := domainadaptor.GetEmbedFn(sctx).EmbedWithContext(ctx, func() bool {
		return sctx.GetSessionVars().SQLKiller.GetKillSignal() > 0
	}, embedArgs.Model, embedArgs.Text, embedArgs.Opts)
	if err != nil {
		return types.Datum{}, err
	}
	embeddingVec, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.Datum{}, err
	}
	return types.NewVectorFloat32Datum(embeddingVec), nil
}

// RequiredOptionalEvalProps implements RequiredOptionalEvalProps interface.
func (b *builtinEmbedTextSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

func unwrapSessionEvalContext(ctx EvalContext) (*sessionexpr.EvalContext, bool) {
	if assertionCtx, ok := ctx.(*assertionEvalContext); ok {
		ctx = assertionCtx.EvalContext
	}
	sessionEvalCtx, ok := ctx.(*sessionexpr.EvalContext)
	return sessionEvalCtx, ok
}
