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
	_ builtinFunc   = &builtinEmbedTextSig{}
)

type embedTextFunctionClass struct {
	baseFunctionClass
}

type builtinEmbedTextSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
}

// Clone implements builtinFunc.Clone.
func (b *builtinEmbedTextSig) Clone() builtinFunc {
	newSig := &builtinEmbedTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (c *embedTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTypes := []types.EvalType{types.ETString, types.ETString}
	if len(args) == 3 {
		argTypes = append(argTypes, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, argTypes...)
	if err != nil {
		return nil, err
	}
	return &builtinEmbedTextSig{baseBuiltinFunc: bf}, nil
}

func (b *builtinEmbedTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	// Check the deployment mode before evaluating any argument. In unsupported
	// deployments EMBED_TEXT must not trigger argument side effects or external calls.
	if !deploymode.IsStarter() {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT is only supported in starter deployment mode")
	}

	sessionEvalCtx, ok := unwrapSessionEvalContext(ctx)
	if !ok {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	model, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	text, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	opts, err := evalEmbedTextOptions(ctx, row, b.args)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}

	sctx := sessionEvalCtx.Sctx()
	embedFn := domainadaptor.GetEmbedFn(sctx)
	if embedFn == nil {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires an initialized Domain embedding runtime")
	}
	embedding, err := embedFn.EmbedWithContext(
		sctx.GetTraceCtx(),
		func() bool { return sctx.GetSessionVars().SQLKiller.GetKillSignal() > 0 },
		model,
		text,
		opts,
	)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	if err := types.CheckVectorDimValid(len(embedding)); err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	vector, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	return vector, false, nil
}

func evalEmbedTextOptions(ctx EvalContext, row chunk.Row, args []Expression) (map[string]any, error) {
	if len(args) != 3 {
		return nil, nil
	}
	options, isNull, err := args[2].EvalString(ctx, row)
	if err != nil {
		return nil, err
	}
	if isNull || options == "" {
		return nil, nil
	}

	var opts map[string]any
	if err := json.Unmarshal([]byte(options), &opts); err != nil {
		return nil, fmt.Errorf("EMBED_TEXT expects options in JSON format")
	}
	if opts == nil {
		return nil, fmt.Errorf("EMBED_TEXT expects options in JSON format")
	}
	// Keys with the @search suffix are reserved for VEC_EMBED_* rewrites and
	// must not affect a direct EMBED_TEXT call.
	for key := range opts {
		if strings.HasSuffix(key, "@search") {
			delete(opts, key)
		}
	}
	return opts, nil
}

// RequiredOptionalEvalProps implements RequiredOptionalEvalProps.
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
