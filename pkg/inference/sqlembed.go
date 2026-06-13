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

package inference

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/inference/embedding/batcher"
	"github.com/pingcap/tidb/pkg/inference/embedding/cohere"
	"github.com/pingcap/tidb/pkg/inference/embedding/gemini"
	"github.com/pingcap/tidb/pkg/inference/embedding/huggingface"
	"github.com/pingcap/tidb/pkg/inference/embedding/jina"
	"github.com/pingcap/tidb/pkg/inference/embedding/mock"
	"github.com/pingcap/tidb/pkg/inference/embedding/nvidia"
	"github.com/pingcap/tidb/pkg/inference/embedding/openai"
	"github.com/pingcap/tidb/pkg/inference/embedding/tidbcloud"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// Embedder is an interface for embedding providers.
type Embedder = base.Embedder

const (
	// EmbeddingCacheSize is the number of entries in the embedding cache.
	// This cache is mainly used for deduplicating embedding requests when user
	// calls the same SQL embed function in multiple places. It does also served
	// as a normal cache to reduce embedding API calls.
	EmbeddingCacheSize = 10000
)

// EmbedFn provides SQL adaptor for Embedding models. This is supposed to be put inside a
// Domain so that it can be shared across sessions.
type EmbedFn struct {
	embedder *batcher.BatchEmbedder
	sf       singleflight.Group
	cache    *ristretto.Cache
}

const (
	errMissingAPI   = "%s API key is not configured, to configure the API key: SET @@GLOBAL.%s='<API_KEY>'"
	errUnauthorized = "%s returns status unauthorized, check your API key. To reconfigure a new API key: SET @@GLOBAL.%s='<API_KEY>'"
)

// NewEmbedFn creates a new EmbedFn instance.
func NewEmbedFn() *EmbedFn {
	embedder := batcher.NewBatchEmbedder()
	embedder.RegisterEmbedder("jina_ai", jina.NewJinaEmbedder(jina.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedJinaAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "JinaAI", strings.ToUpper(vardef.TiDBExpEmbedJinaAIAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "JinaAI", strings.ToUpper(vardef.TiDBExpEmbedJinaAIAPIKey)),
	}))
	embedder.RegisterEmbedder("openai", openai.NewOpenAIEmbedder(openai.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedOpenAIAPIKey.Load() },
		GetBaseURL:       variable.GetOpenAIEmbeddingBaseURL,
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "OpenAI", strings.ToUpper(vardef.TiDBExpEmbedOpenAIAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "OpenAI", strings.ToUpper(vardef.TiDBExpEmbedOpenAIAPIKey)),
	}))
	embedder.RegisterEmbedder("cohere", cohere.NewCohereEmbedder(cohere.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedCohereAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "Cohere", strings.ToUpper(vardef.TiDBExpEmbedCohereAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "Cohere", strings.ToUpper(vardef.TiDBExpEmbedCohereAPIKey)),
	}))
	embedder.RegisterEmbedder("huggingface", huggingface.NewHuggingFaceEmbedder(huggingface.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedHuggingFaceAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "HuggingFace", strings.ToUpper(vardef.TiDBExpEmbedHuggingFaceAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "HuggingFace", strings.ToUpper(vardef.TiDBExpEmbedHuggingFaceAPIKey)),
	}))
	embedder.RegisterEmbedder("nvidia_nim", nvidia.NewNvidiaEmbedder(nvidia.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedNvidiaNIMAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "NVIDIA NIM", strings.ToUpper(vardef.TiDBExpEmbedNvidiaNIMAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "NVIDIA NIM", strings.ToUpper(vardef.TiDBExpEmbedNvidiaNIMAPIKey)),
	}))
	embedder.RegisterEmbedder("gemini", gemini.NewGeminiEmbedder(gemini.EmbedderConfig{
		GetAPIKey:        func() string { return vardef.EmbedGeminiAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "Gemini", strings.ToUpper(vardef.TiDBExpEmbedGeminiAPIKey)),
		// ErrUnauthorized is not provided in gemini. The error message provided by Gemini API is sufficient enough.
	}))
	if isHostedEmbeddingEnabled() {
		embedder.RegisterEmbedder("tidbcloud_free", tidbcloud.NewTiDBCloudFreeEmbedder(tidbcloud.EmbedderConfig{
			GetBillingID: func() string {
				if config.GetGlobalConfig().AutoScalerClusterID == "" {
					return ""
				}
				return fmt.Sprintf("cluster_%s", config.GetGlobalConfig().AutoScalerClusterID)
			},
			GetAPIKey:  getHostedEmbeddingAPIKey,
			GetBaseURL: func() string { return config.GetGlobalConfig().HostedEmbedding.APIEndpoint },
		}))
	}
	if intest.InTest {
		embedder.RegisterEmbedder("mock", mock.NewMockEmbedder())
	}
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        EmbeddingCacheSize,
		MaxCost:            EmbeddingCacheSize,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(err)
	}
	return &EmbedFn{
		embedder: embedder,
		cache:    cache,
	}
}

// HasEmbedder returns whether a provider is registered.
func (e *EmbedFn) HasEmbedder(provider string) bool {
	return e.embedder.HasEmbedder(provider)
}

// MustRegisterEmbedder registers an embedder for tests and panics on invalid input.
func (e *EmbedFn) MustRegisterEmbedder(provider string, embedder Embedder) {
	e.embedder.MustRegisterEmbedder(provider, embedder)
}

func isHostedEmbeddingEnabled() bool {
	return kerneltype.IsNextGen() && config.GetGlobalConfig().HostedEmbedding.Enabled
}

func getHostedEmbeddingAPIKey() string {
	apiKeyPath := config.GetGlobalConfig().HostedEmbedding.APIKeyPath
	if apiKeyPath == "" {
		return ""
	}
	d, err := os.ReadFile(apiKeyPath)
	if err != nil {
		logutil.BgLogger().Error("Failed to read specified API key file for hosted embedding service, API key will not be attached",
			zap.String("api-key-path", apiKeyPath),
			zap.Error(err))
		return ""
	}
	return strings.TrimSpace(string(d))
}

// Embed generates embeddings for the given text. It handles with cache and batching.
func (e *EmbedFn) Embed(shouldCancel func() bool, modelWithProvider string, text string, opts map[string]any) ([]float32, error) {
	return e.EmbedWithContext(context.Background(), shouldCancel, modelWithProvider, text, opts)
}

// EmbedWithContext generates embeddings for the given text. It handles cache and batching.
func (e *EmbedFn) EmbedWithContext(ctx context.Context, shouldCancel func() bool, modelWithProvider string, text string, opts map[string]any) ([]float32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// In TiDB expression execution, usually only a Killed flag can be retrieved.
	// This creates the context from the Killed flag which checks at 1s interval.
	if shouldCancel != nil {
		go func() {
			for {
				if shouldCancel() {
					cancel()
					return
				}
				select {
				case <-time.After(1 * time.Second):
					// Check the shouldCancel condition every second.
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	if opts == nil {
		opts = make(map[string]any)
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize opts: %w", err)
	}
	cacheKey := embeddingCacheKey(modelWithProvider, text, optsJSON)
	if cached, found := e.cache.Get(cacheKey); found {
		return cached.([]float32), nil
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	resultCh := e.sf.DoChan(cacheKey, func() (any, error) {
		embeddings, err := e.embedder.CreateEmbeddings(ctx, modelWithProvider, []string{text}, opts)
		if err != nil {
			return nil, err
		}
		if len(embeddings) == 0 {
			return nil, fmt.Errorf("no embeddings returned for model %s and text %s", modelWithProvider, text)
		}
		e.cache.Set(cacheKey, embeddings[0], 1)
		e.cache.Wait()
		return embeddings[0], nil
	})
	select {
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.([]float32), nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func embeddingCacheKey(modelWithProvider, text string, optsJSON []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(modelWithProvider))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(text))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(optsJSON)
	return string(hash.Sum(nil))
}

// Close releases resources held by the EmbedFn.
func (e *EmbedFn) Close() {
	e.cache.Close()
}

// NewMockEmbedder creates the test embedder used by EMBED_TEXT() integration tests.
func NewMockEmbedder() *mock.Embedder {
	return mock.NewMockEmbedder()
}

// ResetDefaultEmbedFnForTest rebuilds the process-wide embedding function for tests.
func ResetDefaultEmbedFnForTest() {
	defaultEmbedFnMu.Lock()
	defer defaultEmbedFnMu.Unlock()
	if defaultEmbedFn != nil {
		defaultEmbedFn.Close()
	}
	defaultEmbedFn = NewEmbedFn()
}

// SetDefaultEmbedFnForTest replaces the process-wide embedding function and returns a cleanup callback.
func SetDefaultEmbedFnForTest(embedFn *EmbedFn) func() {
	defaultEmbedFnMu.Lock()
	original := defaultEmbedFn
	defaultEmbedFn = embedFn
	defaultEmbedFnMu.Unlock()
	return func() {
		defaultEmbedFnMu.Lock()
		defer defaultEmbedFnMu.Unlock()
		if defaultEmbedFn != nil && defaultEmbedFn != original {
			defaultEmbedFn.Close()
		}
		defaultEmbedFn = original
	}
}

var (
	defaultEmbedFnMu sync.Mutex
	defaultEmbedFn   *EmbedFn
)

// DefaultEmbedFn returns the process-wide embedding function used by EMBED_TEXT().
func DefaultEmbedFn() *EmbedFn {
	defaultEmbedFnMu.Lock()
	defer defaultEmbedFnMu.Unlock()
	if defaultEmbedFn == nil {
		defaultEmbedFn = NewEmbedFn()
	}
	return defaultEmbedFn
}
