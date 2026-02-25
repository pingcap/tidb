//go:build intest

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

// SetLLMCompleteHookForTest overrides LLM completion for tests.
func SetLLMCompleteHookForTest(fn func(model, prompt string) (string, error)) func() {
	old := llmCompleteHook
	llmCompleteHook = fn
	return func() {
		llmCompleteHook = old
	}
}

// SetLLMEmbedHookForTest overrides LLM embedding for tests.
func SetLLMEmbedHookForTest(fn func(model, text string) ([]float32, error)) func() {
	old := llmEmbedHook
	llmEmbedHook = fn
	return func() {
		llmEmbedHook = old
	}
}
