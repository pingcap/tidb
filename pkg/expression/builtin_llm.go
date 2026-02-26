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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/llm"
	"github.com/pingcap/tidb/pkg/util/llm/bedrock"
	"github.com/pingcap/tidb/pkg/util/llm/vertex"
)

var (
	_ functionClass = &llmCompleteFunctionClass{}
	_ functionClass = &llmEmbedTextFunctionClass{}

	_ builtinFunc = &builtinLLMCompleteSig{}
	_ builtinFunc = &builtinLLMEmbedTextSig{}
)

var (
	llmCompleteHook func(model, prompt string, opts llm.CompleteOptions) (string, error)
	llmEmbedHook    func(model, text string) ([]float32, error)
)

type llmClient interface {
	Complete(ctx context.Context, model, prompt string, opts llm.CompleteOptions) (string, error)
	EmbedText(ctx context.Context, model, text string) ([]float32, error)
}

type llmCompleteFunctionClass struct {
	baseFunctionClass
}

func (c *llmCompleteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	ctx.SetSkipPlanCache("llm_complete is not cacheable")
	return &builtinLLMCompleteSig{baseBuiltinFunc: bf}, nil
}

type llmEmbedTextFunctionClass struct {
	baseFunctionClass
}

func (c *llmEmbedTextFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, types.ETString)
	if err != nil {
		return nil, err
	}
	ctx.SetSkipPlanCache("llm_embed_text is not cacheable")
	return &builtinLLMEmbedTextSig{baseBuiltinFunc: bf}, nil
}

type builtinLLMCompleteSig struct {
	baseBuiltinFunc
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinLLMCompleteSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinLLMCompleteSig) Clone() builtinFunc {
	newSig := &builtinLLMCompleteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLLMCompleteSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	if err := checkLLMEnabled(ctx, b.PrivilegeCheckerPropReader); err != nil {
		return "", true, err
	}
	prompt, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}
	model := vardef.LLMDefaultModel.Load()
	out, err := runLLMComplete(model, prompt)
	if err != nil {
		return "", true, err
	}
	return out, false, nil
}

type builtinLLMEmbedTextSig struct {
	baseBuiltinFunc
	expropt.PrivilegeCheckerPropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinLLMEmbedTextSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.PrivilegeCheckerPropReader.RequiredOptionalEvalProps()
}

func (b *builtinLLMEmbedTextSig) Clone() builtinFunc {
	newSig := &builtinLLMEmbedTextSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLLMEmbedTextSig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	if err := checkLLMEnabled(ctx, b.PrivilegeCheckerPropReader); err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	text, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	if isNull {
		return types.ZeroVectorFloat32, true, nil
	}
	model := vardef.LLMDefaultModel.Load()
	values, err := runLLMEmbed(model, text)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	vec, err := types.CreateVectorFloat32(values)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	return vec, false, nil
}

func checkLLMEnabled(ctx EvalContext, privReader expropt.PrivilegeCheckerPropReader) error {
	if !vardef.EnableLLMInference.Load() {
		return errLLMInferenceDisabled
	}
	if vardef.LLMDefaultModel.Load() == "" {
		return errLLMDefaultModelUnset
	}
	checker, err := privReader.GetPrivilegeChecker(ctx)
	if err != nil {
		return err
	}
	if !checker.RequestDynamicVerification("LLM_EXECUTE", false) {
		return errSpecificAccessDenied.GenWithStackByArgs("LLM_EXECUTE")
	}
	return nil
}

func runLLMComplete(model, prompt string) (string, error) {
	opts := llmCompleteOptions()
	if llmCompleteHook != nil {
		return llmCompleteHook(model, prompt, opts)
	}
	client, ctx, cancel, err := newLLMClient()
	if err != nil {
		return "", err
	}
	if cancel != nil {
		defer cancel()
	}
	return client.Complete(ctx, model, prompt, opts)
}

func runLLMEmbed(model, text string) ([]float32, error) {
	if llmEmbedHook != nil {
		return llmEmbedHook(model, text)
	}
	client, ctx, cancel, err := newLLMClient()
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}
	return client.EmbedText(ctx, model, text)
}

func newLLMClient() (llmClient, context.Context, context.CancelFunc, error) {
	cfg := config.GetGlobalConfig().LLM
	provider := strings.ToLower(cfg.Provider)
	if provider == "" {
		provider = "vertex"
	}
	if provider != "vertex" && provider != "bedrock" {
		return nil, nil, nil, errors.Errorf("unsupported llm provider: %s", cfg.Provider)
	}

	timeout := vardef.LLMTimeout.Load()
	if timeout <= 0 {
		timeout = cfg.RequestTimeout
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}

	var (
		client llmClient
		err    error
	)
	switch provider {
	case "vertex":
		client, err = newVertexClient(ctx, cfg, timeout)
	case "bedrock":
		client, err = newBedrockClient(ctx, cfg, timeout)
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, err
	}
	return client, ctx, cancel, nil
}

func newVertexClient(ctx context.Context, cfg config.LLMConfig, timeout time.Duration) (llmClient, error) {
	if cfg.VertexProject == "" || cfg.VertexLocation == "" {
		return nil, errors.New("llm config requires vertex_project and vertex_location")
	}
	tokenSource, err := vertex.NewTokenSource(ctx, cfg.CredentialFile)
	if err != nil {
		return nil, err
	}
	return vertex.NewClient(vertex.Config{
		Project:     cfg.VertexProject,
		Location:    cfg.VertexLocation,
		TokenSource: tokenSource,
		Timeout:     timeout,
	})
}

func newBedrockClient(ctx context.Context, cfg config.LLMConfig, timeout time.Duration) (llmClient, error) {
	if cfg.BedrockRegion == "" {
		return nil, errors.New("llm config requires bedrock_region")
	}
	return bedrock.NewClient(ctx, bedrock.Config{
		Region:   cfg.BedrockRegion,
		Endpoint: cfg.BedrockEndpoint,
		Timeout:  timeout,
	})
}

func llmCompleteOptions() llm.CompleteOptions {
	opts := llm.CompleteOptions{}
	if maxTokens := vardef.LLMMaxTokens.Load(); maxTokens > 0 {
		opts.MaxTokens = int(maxTokens)
	}
	if temp := vardef.LLMTemperature.Load(); temp >= 0 {
		val := temp
		opts.Temperature = &val
	}
	if topP := vardef.LLMTopP.Load(); topP >= 0 {
		val := topP
		opts.TopP = &val
	}
	return opts
}
