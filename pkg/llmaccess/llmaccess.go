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

package llmaccess

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)

const (
	OpenAI = "openai"
)

type LLMAccessor interface {
	// ChatCompletion calls the specified LLM to complete the chat based on input prompt.
	ChatCompletion(platform, model, prompt string) (response string, err error)

	AlterPlatform(sctx sessionctx.Context, platform, key, val string) error
}

type llmAccessorImpl struct {
	sPool util.DestroyableSessionPool
}

func NewLLMAccessor(sPool util.DestroyableSessionPool) LLMAccessor {
	return &llmAccessorImpl{sPool: sPool}
}

func (llm *llmAccessorImpl) AlterPlatform(sctx sessionctx.Context, platform, key, val string) error {
	platform, err := formatPlatform(platform)
	if err != nil {
		return err
	}
	switch strings.ToUpper(key) {
	case "KEY", "TIMEOUT":
	case "ENABLED", "DISABLED":
		val = key
		key = "status"
	default:
		return fmt.Errorf("unsupported key: %s", key)
	}
	updateStmt := "update mysql.llm_platform set `" + key + "` = %? where `name` = %?"
	return callWithSCtx(llm.sPool, true, func(tmpCtx sessionctx.Context) error {
		_, err := exec(tmpCtx, updateStmt, val, platform)
		sctx.GetSessionVars().StmtCtx.SetAffectedRows(tmpCtx.GetSessionVars().StmtCtx.AffectedRows())
		return err
	})
}

// ChatCompletion calls the specified LLM to complete the chat based on input prompt.
func (llm *llmAccessorImpl) ChatCompletion(platform, model, prompt string) (response string, err error) {
	platform, err = formatPlatform(platform)
	if err != nil {
		return "", err
	}

	switch platform {
	case OpenAI:
		return llm.chatCompletionOpenAI(model, prompt, "")
	default:
		return "", fmt.Errorf("unsupported platform: %d", platform)
	}
}

func (*llmAccessorImpl) chatCompletionOpenAI(model, prompt, key string) (response string, err error) {
	if key == "" {
		key, _ = os.LookupEnv("OPENAI_API_KEY")
	}
	client := openai.NewClient(
		option.WithAPIKey(key), // defaults to os.LookupEnv("OPENAI_API_KEY")
	)
	chatCompletion, err := client.Chat.Completions.New(context.TODO(), openai.ChatCompletionNewParams{
		Messages: openai.F([]openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		}),
		Model: openai.F(model),
	})
	if err != nil {
		llmLogger().Error("failed to call OpenAI",
			zap.String("platform", OpenAI), zap.String("model", model),
			zap.String("prompt", prompt), zap.Error(err))
		return "", err
	}
	return chatCompletion.Choices[0].Message.Content, nil
}

func formatPlatform(platform string) (string, error) {
	switch strings.ToLower(platform) {
	case OpenAI:
		return OpenAI, nil
	}
	return "", fmt.Errorf("unsupported platform: %s", platform)
}
