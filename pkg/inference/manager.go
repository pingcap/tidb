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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// Embedder defines the minimal contract that an embedding provider must satisfy.
// The provider name is managed by Registry, while model is the provider-specific
// model identifier after the leading "<provider>/" prefix has been stripped.
type Embedder interface {
	CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error)
}

// Registry keeps the available embedding providers.
type Registry struct {
	mu        sync.RWMutex
	embedders map[string]Embedder
}

var defaultRegistry = newRegistry(true)

// DefaultRegistry returns the process-wide embedding registry used by EMBED_TEXT().
func DefaultRegistry() *Registry {
	return defaultRegistry
}

// NewRegistry creates a new embedding registry.
func NewRegistry() *Registry {
	return newRegistry(false)
}

func newRegistry(registerMock bool) *Registry {
	r := &Registry{
		embedders: make(map[string]Embedder),
	}
	r.MustRegisterEmbedder("openai", NewOpenAIEmbedder())
	if registerMock && intest.InTest {
		r.MustRegisterEmbedder("mock", NewMockEmbedder())
	}
	return r
}

// RegisterEmbedder registers a provider under the given name.
func (r *Registry) RegisterEmbedder(name string, embedder Embedder) error {
	name = normalizeProviderName(name)
	if name == "" {
		return errors.New("embedding provider name cannot be empty")
	}
	if embedder == nil {
		return errors.Errorf("embedding provider %q is nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.embedders[name]; exists {
		return errors.Errorf("embedding provider %q is already registered", name)
	}
	r.embedders[name] = embedder
	return nil
}

// MustRegisterEmbedder is like RegisterEmbedder but panics on error.
func (r *Registry) MustRegisterEmbedder(name string, embedder Embedder) {
	if err := r.RegisterEmbedder(name, embedder); err != nil {
		panic(err)
	}
}

// GetEmbedder returns a registered provider by name.
func (r *Registry) GetEmbedder(name string) (Embedder, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	embedder, ok := r.embedders[normalizeProviderName(name)]
	return embedder, ok
}

// Embed produces a single embedding for one input text.
func (r *Registry) Embed(shouldCancel func() bool, modelWithProvider, text string, opts map[string]any) ([]float32, error) {
	provider, model, err := parseModelIdentifier(modelWithProvider)
	if err != nil {
		return nil, err
	}
	embedder, ok := r.GetEmbedder(provider)
	if !ok {
		return nil, errors.Errorf("embedding provider %q is not registered", provider)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if shouldCancel != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				if shouldCancel() {
					cancel()
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}

	if opts == nil {
		opts = map[string]any{}
	}
	embeddings, err := embedder.CreateEmbeddings(ctx, model, []string{text}, opts)
	if err != nil {
		return nil, err
	}
	switch len(embeddings) {
	case 0:
		return nil, errors.Errorf("embedding provider %q returned no embeddings", provider)
	case 1:
		return embeddings[0], nil
	default:
		return nil, errors.Errorf("embedding provider %q returned %d embeddings for a single input", provider, len(embeddings))
	}
}

func normalizeProviderName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func parseModelIdentifier(modelWithProvider string) (provider string, model string, err error) {
	modelWithProvider = strings.TrimSpace(modelWithProvider)
	provider, model, ok := strings.Cut(modelWithProvider, "/")
	if !ok || normalizeProviderName(provider) == "" || strings.TrimSpace(model) == "" {
		return "", "", fmt.Errorf("EMBED_TEXT expects model name in '<provider>/<model>' format")
	}
	return normalizeProviderName(provider), strings.TrimSpace(model), nil
}
