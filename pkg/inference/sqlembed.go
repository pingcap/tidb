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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/inference/embedding/batcher"
	"github.com/pingcap/tidb/pkg/inference/embedding/cohere"
	"github.com/pingcap/tidb/pkg/inference/embedding/gemini"
	"github.com/pingcap/tidb/pkg/inference/embedding/huggingface"
	"github.com/pingcap/tidb/pkg/inference/embedding/jina"
	"github.com/pingcap/tidb/pkg/inference/embedding/mock"
	"github.com/pingcap/tidb/pkg/inference/embedding/nvidia"
	"github.com/pingcap/tidb/pkg/inference/embedding/openai"
	"github.com/pingcap/tidb/pkg/inference/embedding/tidbcloud"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

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
		GetAPIKey:        func() string { return variable.EmbedJinaAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "JinaAI", strings.ToUpper(variable.TiDBExpEmbedJinaAIAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "JinaAI", strings.ToUpper(variable.TiDBExpEmbedJinaAIAPIKey)),
	}))
	embedder.RegisterEmbedder("openai", openai.NewOpenAIEmbedder(openai.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedOpenAIAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "OpenAI", strings.ToUpper(variable.TiDBExpEmbedOpenAIAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "OpenAI", strings.ToUpper(variable.TiDBExpEmbedOpenAIAPIKey)),
	}))
	embedder.RegisterEmbedder("cohere", cohere.NewCohereEmbedder(cohere.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedCohereAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "Cohere", strings.ToUpper(variable.TiDBExpEmbedCohereAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "Cohere", strings.ToUpper(variable.TiDBExpEmbedCohereAPIKey)),
	}))
	embedder.RegisterEmbedder("huggingface", huggingface.NewHuggingFaceEmbedder(huggingface.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedHuggingFaceAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "HuggingFace", strings.ToUpper(variable.TiDBExpEmbedHuggingFaceAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "HuggingFace", strings.ToUpper(variable.TiDBExpEmbedHuggingFaceAPIKey)),
	}))
	embedder.RegisterEmbedder("nvidia_nim", nvidia.NewNvidiaEmbedder(nvidia.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedNvidiaNIMAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "NVIDIA NIM", strings.ToUpper(variable.TiDBExpEmbedNvidiaNIMAPIKey)),
		ErrUnauthorized:  fmt.Errorf(errUnauthorized, "NVIDIA NIM", strings.ToUpper(variable.TiDBExpEmbedNvidiaNIMAPIKey)),
	}))
	embedder.RegisterEmbedder("gemini", gemini.NewGeminiEmbedder(gemini.EmbedderConfig{
		GetAPIKey:        func() string { return variable.EmbedGeminiAPIKey.Load() },
		ErrMissingAPIKey: fmt.Errorf(errMissingAPI, "Gemini", strings.ToUpper(variable.TiDBExpEmbedGeminiAPIKey)),
		// ErrUnauthorized is not provided in gemini. The error message provided by Gemini API is sufficient enough.
	}))
	if config.GetGlobalConfig().HostedEmbedding.Enabled {
		var apiKey string
		if config.GetGlobalConfig().HostedEmbedding.APIKeyPath != "" {
			d, err := os.ReadFile(config.GetGlobalConfig().HostedEmbedding.APIKeyPath)
			if err != nil {
				logutil.BgLogger().Error("Failed to read specified API key file for hosted embedding service, API key will not be attached",
					zap.String("api-key-path", config.GetGlobalConfig().HostedEmbedding.APIKeyPath),
					zap.Error(err))
			} else {
				apiKey = strings.TrimSpace(string(d))
			}
		}
		embedder.RegisterEmbedder("tidbcloud_free", tidbcloud.NewTiDBCloudFreeEmbedder(tidbcloud.EmbedderConfig{
			GetBillingID: func() string {
				if metrics.ServerlessClusterID == "" {
					return ""
				}
				return fmt.Sprintf("cluster_%s", metrics.ServerlessClusterID)
			},
			GetAPIKey:  func() string { return apiKey },
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

// Embed generates embeddings for the given text. It handles with cache and batching.
func (e *EmbedFn) Embed(shouldCancel func() bool, modelWithProvider string, text string, opts map[string]any) ([]float32, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// In TiDB expression execution, usually only a Killed flag can be retrieved.
	// This creates the context from the Killed flag which checks at 1s interval.
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

	if opts == nil {
		opts = make(map[string]any)
	}
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize opts: %w", err)
	}
	cacheKey := fmt.Sprintf("%s/%s/%s", modelWithProvider, text, string(optsJSON))
	if cached, found := e.cache.Get(cacheKey); found {
		return cached.([]float32), nil
	}
	result, err, _ := e.sf.Do(cacheKey, func() (any, error) {
		embeddings, err := e.embedder.CreateEmbeddings(ctx, modelWithProvider, []string{text}, opts)
		if err != nil {
			return nil, err
		}
		if len(embeddings) == 0 {
			return nil, fmt.Errorf("no embeddings returned for model %s and text %s", modelWithProvider, text)
		}
		e.cache.Set(cacheKey, embeddings[0], 1)
		return embeddings[0], nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]float32), err
}

// Close releases resources held by the EmbedFn.
func (e *EmbedFn) Close() {
	e.cache.Close()
}
