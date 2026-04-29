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

package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// MockEmbedder is only meant for tests. It accepts the "mock/json" model and
// interprets each input text as a JSON array of float32 numbers.
type MockEmbedder struct{}

// NewMockEmbedder creates the test embedder used by EMBED_TEXT() integration tests.
func NewMockEmbedder() *MockEmbedder {
	return &MockEmbedder{}
}

// CreateEmbeddings implements Embedder.
func (m *MockEmbedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	if model != "json" {
		return nil, fmt.Errorf("unknown model %s", model)
	}

	allowedOptions := map[string]struct{}{
		"plus":  {},
		"delay": {},
	}
	for opt := range opts {
		if _, ok := allowedOptions[opt]; !ok {
			return nil, fmt.Errorf("unknown option %s", opt)
		}
	}

	plus := float32(0)
	if v, ok := opts["plus"]; ok {
		plusFloat, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid type for 'plus' option: %T", v)
		}
		plus = float32(plusFloat)
	}
	if v, ok := opts["delay"]; ok {
		delayText, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("invalid type for 'delay' option: %T", v)
		}
		delay, err := time.ParseDuration(delayText)
		if err != nil {
			return nil, fmt.Errorf("invalid delay duration: %s", delayText)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}

	embeddings := make([][]float32, len(texts))
	for i, text := range texts {
		var embedding []float32
		if err := json.Unmarshal([]byte(text), &embedding); err != nil {
			return nil, err
		}
		if plus != 0 {
			for j := range embedding {
				embedding[j] += plus
			}
		}
		embeddings[i] = embedding
	}
	return embeddings, nil
}
