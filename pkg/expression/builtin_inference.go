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
	expropt.SessionContextPropReader
}

// EmbedTextArgs contains evaluated arguments for one EMBED_TEXT invocation.
// Evaluating arguments separately lets generated-column execution batch the
// remote calls without evaluating ordinary SQL expressions concurrently.
type EmbedTextArgs struct {
	Model string
	Text  string
	Opts  map[string]any
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
	if err := CheckEmbedTextAllowed(); err != nil {
		return types.ZeroVectorFloat32, false, err
	}

	sctx, err := b.GetSessionContext(ctx)
	if err != nil {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context: %w", err)
	}
	if sctx == nil {
		return types.ZeroVectorFloat32, false, fmt.Errorf("EMBED_TEXT requires session context")
	}
	embedArgs, isNull, err := EvalEmbedTextArgs(ctx, row, b.args)
	if isNull || err != nil {
		return types.ZeroVectorFloat32, isNull, err
	}
	datum, err := EvalEmbedTextArgsToDatum(sctx.GetTraceCtx(), sctx, embedArgs)
	if err != nil {
		return types.ZeroVectorFloat32, false, err
	}
	return datum.GetVectorFloat32(), false, nil
}

// EvalEmbedTextArgsFromExpr evaluates the arguments of a direct
// EMBED_TEXT scalar expression without calling the embedding provider.
func EvalEmbedTextArgsFromExpr(ctx EvalContext, row chunk.Row, expr Expression) (*EmbedTextArgs, bool, error) {
	sf, ok := expr.(*ScalarFunction)
	if !ok {
		return nil, false, fmt.Errorf("generated-column evaluation expects EMBED_TEXT()")
	}
	if _, ok := sf.Function.(*builtinEmbedTextSig); !ok {
		return nil, false, fmt.Errorf("generated-column evaluation expects EMBED_TEXT()")
	}
	return EvalEmbedTextArgs(ctx, row, sf.GetArgs())
}

// EvalEmbedTextArgs evaluates EMBED_TEXT arguments without calling the provider.
func EvalEmbedTextArgs(ctx EvalContext, row chunk.Row, args []Expression) (*EmbedTextArgs, bool, error) {
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
	opts, err := evalEmbedTextOptions(ctx, row, args)
	if err != nil {
		return nil, false, err
	}
	return &EmbedTextArgs{Model: model, Text: text, Opts: opts}, false, nil
}

// CheckEmbedTextAllowed validates deployment-level EMBED_TEXT availability.
func CheckEmbedTextAllowed() error {
	if !deploymode.IsStarter() {
		return fmt.Errorf("EMBED_TEXT is only supported in starter deployment mode")
	}
	return nil
}

// EvalEmbedTextArgsToDatum materializes evaluated EMBED_TEXT arguments as a vector datum.
func EvalEmbedTextArgsToDatum(ctx context.Context, sctx expropt.SessionContext, embedArgs *EmbedTextArgs) (types.Datum, error) {
	if sctx == nil {
		return types.Datum{}, fmt.Errorf("EMBED_TEXT requires session context")
	}
	if err := CheckEmbedTextAllowed(); err != nil {
		return types.Datum{}, err
	}
	if embedArgs == nil {
		return types.Datum{}, fmt.Errorf("invalid EMBED_TEXT() usage")
	}
	embedFn := domainadaptor.GetEmbedFn(sctx)
	if embedFn == nil {
		return types.Datum{}, fmt.Errorf("EMBED_TEXT requires an initialized Domain embedding runtime")
	}
	embedding, err := embedFn.EmbedWithContext(
		ctx,
		func() bool { return sctx.GetSessionVars().SQLKiller.GetKillSignal() > 0 },
		embedArgs.Model,
		embedArgs.Text,
		embedArgs.Opts,
	)
	if err != nil {
		return types.Datum{}, err
	}
	if err := types.CheckVectorDimValid(len(embedding)); err != nil {
		return types.Datum{}, err
	}
	vector, err := types.CreateVectorFloat32(embedding)
	if err != nil {
		return types.Datum{}, err
	}
	return types.NewVectorFloat32Datum(vector), nil
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
	return b.SessionContextPropReader.RequiredOptionalEvalProps()
}
