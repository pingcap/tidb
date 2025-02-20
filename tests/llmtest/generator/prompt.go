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

package generator

import (
	"github.com/openai/openai-go"
	"github.com/pingcap/tidb/tests/llmtest/testcase"
)

// PromptGenerator is the interface for prompt generator.
type PromptGenerator interface {
	Name() string
	Groups() []string

	GeneratePrompt(group string, count int, existCases []*testcase.Case) []openai.ChatCompletionMessageParamUnion
	Unmarshal(response string) []testcase.Case
}

// generators is a map from generator name to generator.
var generators map[string]PromptGenerator

// AllPromptGenerators returns all the registered generators.
func AllPromptGenerators() []PromptGenerator {
	result := make([]PromptGenerator, 0, len(generators))
	for _, g := range generators {
		result = append(result, g)
	}
	return result
}

// GetPromptGenerator returns the generator by name.
func GetPromptGenerator(name string) PromptGenerator {
	return generators[name]
}

func registerPromptGenerator(g PromptGenerator) {
	if generators == nil {
		generators = make(map[string]PromptGenerator)
	}
	generators[g.Name()] = g
}
