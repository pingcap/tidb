// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// Embedder allows returning desired embedding according to a failpoint.
// Only enabled in tests.
// It always returns an embedding which is exactly the same as json_parse(input texts).
// For example, if the texts is ["[1,2,3]"], the embedding will be [[1,2,3]].
// It also accepts a opt "plus=N", which plus N to each element of the embedding.
type Embedder struct {
}

// NewMockEmbedder creates a new MockEmbedder instance.
// It always returns an embedding which is exactly the same as json_parse(input texts).
// For example, if the texts is ["[1,2,3]"], the embedding will be [[1,2,3]].
// It also accepts a opt "plus=N", which plus N to each element of the embedding.
func NewMockEmbedder() *Embedder {
	return &Embedder{}
}

var _ base.Embedder = (*Embedder)(nil)

// CreateEmbeddings implements base.Embedder.
func (m *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	if model != "json" {
		// Also act as an error trigger.
		return nil, fmt.Errorf("unknown model %s", model)
	}

	// Let's just be strict about the options passed in.
	allowedOptions := map[string]bool{
		"plus":  true,
		"delay": true,
	}
	for opt := range opts {
		if !allowedOptions[opt] {
			return nil, fmt.Errorf("unknown option %s", opt)
		}
	}

	plus := 0.0
	if opts != nil {
		if p, ok := opts["plus"]; ok {
			if plusVal, ok := p.(float64); ok {
				plus = plusVal
			} else {
				return nil, fmt.Errorf("invalid type for 'plus' option: %T", p)
			}
		}

		// Simulate remote call delays.
		if delay, ok := opts["delay"]; ok {
			if delayVal, ok := delay.(string); ok {
				dur, err := time.ParseDuration(delayVal)
				if err != nil {
					return nil, fmt.Errorf("invalid delay duration: %s", delayVal)
				}
				time.Sleep(dur)
			} else {
				return nil, fmt.Errorf("invalid type for 'delay' option: %T", delay)
			}
		}
	}

	embeddings := make([][]float32, len(texts))
	for i, text := range texts {
		var embedding []float32
		err := json.Unmarshal([]byte(text), &embedding)
		if err != nil {
			return nil, err
		}
		if plus != 0 {
			for j := range embedding {
				embedding[j] += float32(plus)
			}
		}
		embeddings[i] = embedding
	}
	return embeddings, nil
}
