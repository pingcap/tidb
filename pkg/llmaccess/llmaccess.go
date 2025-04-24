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

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

// LLMPlatform is the platform of LLM.
type LLMPlatform int

const (
	// OpenAI is the OpenAI platform.
	OpenAI LLMPlatform = iota
)

// ChatCompletion calls the specified LLM to complete the chat based on input prompt.
func ChatCompletion(platform LLMPlatform, model, prompt string) (response string, err error) {
	switch platform {
	case OpenAI:
		return chatCompletionOpenAI(model, prompt, "")
	default:
		return "", fmt.Errorf("unsupported platform: %d", platform)
	}
}

func chatCompletionOpenAI(model, prompt, key string) (response string, err error) {
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
		return "", err
	}
	return chatCompletion.Choices[0].Message.Content, nil
}
