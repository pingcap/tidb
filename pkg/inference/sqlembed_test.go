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
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

const embeddingTestTimeout = 10 * time.Second

type staticEmbedder struct {
	embeddings [][]float32
	err        error
	calls      atomic.Int64
}

func (s *staticEmbedder) CreateEmbeddings(context.Context, string, []string, map[string]any) ([][]float32, error) {
	s.calls.Add(1)
	return s.embeddings, s.err
}

type controlledEmbedder struct {
	started     chan struct{}
	startedOnce sync.Once
	release     chan struct{}
	canceled    chan struct{}
	cancelOnce  sync.Once
	contextVals chan any
	calls       atomic.Int64
}

type embeddingTestContextKey struct{}

func (c *controlledEmbedder) CreateEmbeddings(ctx context.Context, _ string, _ []string, _ map[string]any) ([][]float32, error) {
	c.calls.Add(1)
	c.startedOnce.Do(func() { close(c.started) })
	if c.contextVals != nil {
		c.contextVals <- ctx.Value(embeddingTestContextKey{})
	}
	select {
	case <-c.release:
		return [][]float32{{1, 2, 3}}, nil
	case <-ctx.Done():
		c.cancelOnce.Do(func() { close(c.canceled) })
		return nil, context.Cause(ctx)
	}
}

func TestEmbedFnProvidersAndErrors(t *testing.T) {
	embedFn := NewEmbedFn()
	t.Cleanup(embedFn.Close)

	for _, provider := range []string{"openai", "jina_ai", "cohere", "huggingface", "nvidia_nim", "gemini"} {
		require.True(t, embedFn.HasEmbedder(provider), provider)
	}
	require.True(t, embedFn.HasEmbedder(" OPENAI "))
	if !embedFn.HasEmbedder("mock") {
		embedFn.MustRegisterEmbedder("mock", NewMockEmbedder())
	}

	embedding, err := embedFn.Embed(nil, "mock/json", "[1,2,3]", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)
	_, err = embedFn.Embed(func() bool { return true }, "mock/json", "[1,2,3]", nil)
	require.ErrorIs(t, err, context.Canceled)

	_, err = embedFn.Embed(nil, "model-without-provider", "hello", nil)
	require.ErrorContains(t, err, "model name must be in format")
	_, err = embedFn.Embed(nil, "unknown/model", "hello", nil)
	require.ErrorContains(t, err, "unknown embedding provider")

	embedFn.MustRegisterEmbedder("empty", &staticEmbedder{})
	_, err = embedFn.Embed(nil, "empty/model", "hello", nil)
	require.ErrorContains(t, err, "no embeddings returned")

	embedFn.MustRegisterEmbedder("fail", &staticEmbedder{err: errors.New("embed failed")})
	_, err = embedFn.Embed(nil, "fail/model", "hello", nil)
	require.ErrorContains(t, err, "embed failed")

	oversized := &staticEmbedder{embeddings: [][]float32{make([]float32, 16384)}}
	embedFn.MustRegisterEmbedder("oversized", oversized)
	for range 2 {
		_, err = embedFn.Embed(nil, "oversized/model", "hello", nil)
		require.ErrorContains(t, err, "vector cannot have more than 16383 dimensions")
	}
	require.Equal(t, int64(2), oversized.calls.Load(), "invalid vectors must not be cached")
}

func TestHostedEmbeddingConfigHelpers(t *testing.T) {
	cfg := config.GetGlobalConfig()
	originalClusterID := cfg.AutoScalerClusterID
	originalAPIKeyPath := cfg.HostedEmbedding.APIKeyPath
	t.Cleanup(func() {
		cfg.AutoScalerClusterID = originalClusterID
		cfg.HostedEmbedding.APIKeyPath = originalAPIKeyPath
	})

	t.Run("billing ID", func(t *testing.T) {
		cfg.AutoScalerClusterID = ""
		require.Empty(t, hostedEmbeddingBillingID())

		cfg.AutoScalerClusterID = "cluster-123"
		require.Equal(t, "cluster_cluster-123", hostedEmbeddingBillingID())
	})

	t.Run("API key path", func(t *testing.T) {
		cfg.HostedEmbedding.APIKeyPath = ""
		require.Empty(t, getHostedEmbeddingAPIKey())

		apiKeyPath := filepath.Join(t.TempDir(), "api-key")
		require.NoError(t, os.WriteFile(apiKeyPath, []byte("  test-api-key\n"), 0o600))
		cfg.HostedEmbedding.APIKeyPath = apiKeyPath
		require.Equal(t, "test-api-key", getHostedEmbeddingAPIKey())

		cfg.HostedEmbedding.APIKeyPath = filepath.Join(t.TempDir(), "missing-api-key")
		require.Empty(t, getHostedEmbeddingAPIKey())
	})
}

func TestContextWithCancelCheck(t *testing.T) {
	t.Run("nil callback", func(t *testing.T) {
		ctx, cancel := contextWithCancelCheck(context.Background(), nil)
		cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := contextWithCancelCheck(context.Background(), func() bool { return true })
		defer cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("polls callback", func(t *testing.T) {
		var shouldCancel atomic.Bool
		ctx, cancel := contextWithCancelCheck(context.Background(), shouldCancel.Load)
		defer cancel()
		require.NoError(t, ctx.Err())

		shouldCancel.Store(true)
		require.Eventually(t, func() bool {
			return errors.Is(ctx.Err(), context.Canceled)
		}, 2*embedCancelCheckInterval+time.Second, 10*time.Millisecond)
	})
}

func TestEmbedFnCacheIsolationAndInvalidation(t *testing.T) {
	originalVersion := vardef.EmbeddingConfigVersion.Load()
	t.Cleanup(func() {
		vardef.EmbeddingConfigVersion.Store(originalVersion)
	})
	embedFn := NewEmbedFn()
	t.Cleanup(embedFn.Close)

	provider := &staticEmbedder{embeddings: [][]float32{{1, 2, 3}}}
	embedFn.MustRegisterEmbedder("static", provider)

	embedding, err := embedFn.Embed(nil, "static/model", "hello", nil)
	require.NoError(t, err)
	embedding[0] = 99

	embedding, err = embedFn.Embed(nil, "static/model", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)
	require.Equal(t, int64(1), provider.calls.Load())

	// Dynamic API-key and endpoint updates advance this version, so cached
	// results from the previous provider configuration are not reused.
	vardef.EmbeddingConfigVersion.Inc()
	embedding, err = embedFn.Embed(nil, "static/model", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)
	require.Equal(t, int64(2), provider.calls.Load())

	opts := map[string]any{}
	optsJSON, err := json.Marshal(opts)
	require.NoError(t, err)
	cacheKey := makeCacheKey("static/model", "already cached", opts, optsJSON, vardef.EmbeddingConfigVersion.Load())
	require.True(t, embedFn.cache.Set(cacheKey, []float32{4, 5, 6}, 1))
	embedFn.cache.Wait()
	call, cached, cacheHit, err := embedFn.acquireCall(context.Background(), cacheKey, "static/model", "already cached", opts)
	require.NoError(t, err)
	require.Nil(t, call)
	require.True(t, cacheHit)
	require.Equal(t, []float32{4, 5, 6}, cached)
	require.Equal(t, int64(2), provider.calls.Load())
}

func TestEmbeddingCacheKeyAndOptionsSnapshot(t *testing.T) {
	intOpts := map[string]any{
		"plus":   int(1),
		"nested": map[string]any{"dimensions": int(128)},
	}
	floatOpts := map[string]any{
		"plus":   float64(1),
		"nested": map[string]any{"dimensions": float64(128)},
	}
	intJSON, err := json.Marshal(intOpts)
	require.NoError(t, err)
	floatJSON, err := json.Marshal(floatOpts)
	require.NoError(t, err)
	require.JSONEq(t, string(intJSON), string(floatJSON))
	require.NotEqual(t,
		makeCacheKey("provider/model", "text", intOpts, intJSON, 1),
		makeCacheKey("provider/model", "text", floatOpts, floatJSON, 1),
	)

	// Length-prefixing keeps component boundaries unambiguous even when model
	// names and input text contain NUL bytes.
	require.NotEqual(t,
		makeCacheKey("a", "b\x00c", nil, nil, 1),
		makeCacheKey("a\x00b", "c", nil, nil, 1),
	)

	type directOption struct {
		Value any `json:"value"`
	}
	structIntOpts := map[string]any{"nested": directOption{Value: int(1)}}
	structFloatOpts := map[string]any{"nested": directOption{Value: float64(1)}}
	structIntJSON, err := json.Marshal(structIntOpts)
	require.NoError(t, err)
	structFloatJSON, err := json.Marshal(structFloatOpts)
	require.NoError(t, err)
	require.JSONEq(t, string(structIntJSON), string(structFloatJSON))
	require.NotEqual(t,
		makeCacheKey("provider/model", "text", structIntOpts, structIntJSON, 1),
		makeCacheKey("provider/model", "text", structFloatOpts, structFloatJSON, 1),
	)

	mapIntOpts := map[string]any{"nested": map[int]any{1: int(1)}}
	mapFloatOpts := map[string]any{"nested": map[int]any{1: float64(1)}}
	mapIntJSON, err := json.Marshal(mapIntOpts)
	require.NoError(t, err)
	mapFloatJSON, err := json.Marshal(mapFloatOpts)
	require.NoError(t, err)
	require.JSONEq(t, string(mapIntJSON), string(mapFloatJSON))
	require.NotEqual(t,
		makeCacheKey("provider/model", "text", mapIntOpts, mapIntJSON, 1),
		makeCacheKey("provider/model", "text", mapFloatOpts, mapFloatJSON, 1),
	)

	mixedMapOpts := map[string]any{"nested": map[int]any{1: int(1), 2: float64(2)}}
	mixedMapJSON, err := json.Marshal(mixedMapOpts)
	require.NoError(t, err)
	mixedMapKey := makeCacheKey("provider/model", "text", mixedMapOpts, mixedMapJSON, 1)
	for range 10 {
		require.Equal(t, mixedMapKey, makeCacheKey("provider/model", "text", mixedMapOpts, mixedMapJSON, 1))
	}

	snapshot, err := snapshotOptions(intOpts)
	require.NoError(t, err)
	intOpts["nested"].(map[string]any)["dimensions"] = int(512)
	require.Equal(t, int(128), snapshot["nested"].(map[string]any)["dimensions"])
}

func TestEmbedFnCloseWaitsForInFlightCall(t *testing.T) {
	embedFn := NewEmbedFn()
	t.Cleanup(embedFn.Close)
	provider := &controlledEmbedder{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	embedFn.MustRegisterEmbedder("controlled", provider)

	result := make(chan error, 1)
	go func() {
		_, err := embedFn.Embed(nil, "controlled/model", "hello", nil)
		result <- err
	}()
	waitForChannel(t, provider.started, "provider request to start")

	embedFn.Close()
	require.ErrorIs(t, receiveFromChannel(t, result, "embedding request to finish after close"), context.Canceled)
}

func TestEmbedFnSharedCallCancellation(t *testing.T) {
	embedFn := NewEmbedFn()
	t.Cleanup(embedFn.Close)
	provider := &controlledEmbedder{
		started:     make(chan struct{}),
		release:     make(chan struct{}),
		canceled:    make(chan struct{}),
		contextVals: make(chan any, 1),
	}
	embedFn.MustRegisterEmbedder("controlled", provider)

	ctx1, cancel1 := context.WithCancelCause(context.WithValue(
		context.Background(),
		embeddingTestContextKey{},
		"first-caller-trace",
	))
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	t.Cleanup(func() {
		cancel1(context.Canceled)
		cancel2(context.Canceled)
	})
	type result struct {
		embedding []float32
		err       error
	}
	result1 := make(chan result, 1)
	result2 := make(chan result, 1)
	go func() {
		embedding, err := embedFn.EmbedWithContext(ctx1, nil, "controlled/model", "hello", nil)
		result1 <- result{embedding: embedding, err: err}
	}()
	waitForChannel(t, provider.started, "provider request to start")
	require.Equal(t, "first-caller-trace", receiveFromChannel(t, provider.contextVals, "provider context value"))
	go func() {
		embedding, err := embedFn.EmbedWithContext(ctx2, nil, "controlled/model", "hello", nil)
		result2 <- result{embedding: embedding, err: err}
	}()
	require.Eventually(t, func() bool {
		return hasSingleInFlightCallWithWaiters(embedFn, 2)
	}, embeddingTestTimeout, 10*time.Millisecond)

	firstCause := errors.New("first caller canceled")
	cancel1(firstCause)
	require.ErrorIs(t, receiveFromChannel(t, result1, "first caller result").err, firstCause)
	select {
	case <-provider.canceled:
		t.Fatal("provider request was canceled while another caller was still waiting")
	default:
	}

	close(provider.release)
	second := receiveFromChannel(t, result2, "second caller result")
	require.NoError(t, second.err)
	require.Equal(t, []float32{1, 2, 3}, second.embedding)
	require.Equal(t, int64(1), provider.calls.Load())
}

func TestEmbedFnCancelsProviderAfterAllCallersCancel(t *testing.T) {
	embedFn := NewEmbedFn()
	t.Cleanup(embedFn.Close)
	provider := &controlledEmbedder{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	embedFn.MustRegisterEmbedder("controlled", provider)

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	err1 := make(chan error, 1)
	err2 := make(chan error, 1)
	go func() {
		_, err := embedFn.EmbedWithContext(ctx1, nil, "controlled/model", "hello", nil)
		err1 <- err
	}()
	waitForChannel(t, provider.started, "provider request to start")
	go func() {
		_, err := embedFn.EmbedWithContext(ctx2, nil, "controlled/model", "hello", nil)
		err2 <- err
	}()
	require.Eventually(t, func() bool {
		return hasSingleInFlightCallWithWaiters(embedFn, 2)
	}, embeddingTestTimeout, 10*time.Millisecond)

	cancel1()
	cancel2()
	require.ErrorIs(t, receiveFromChannel(t, err1, "first caller cancellation"), context.Canceled)
	require.ErrorIs(t, receiveFromChannel(t, err2, "second caller cancellation"), context.Canceled)
	select {
	case <-provider.canceled:
	case <-time.After(embeddingTestTimeout):
		t.Fatal("provider request was not canceled after all callers canceled")
	}
}

func TestSetDefaultEmbedFnForTest(t *testing.T) {
	original := DefaultEmbedFn()
	replacement := NewEmbedFn()
	restore := SetDefaultEmbedFnForTest(replacement)
	require.Same(t, replacement, DefaultEmbedFn())
	restore()
	require.Same(t, original, DefaultEmbedFn())
}

func waitForChannel(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	receiveFromChannel(t, ch, description)
}

func hasSingleInFlightCallWithWaiters(embedFn *EmbedFn, waiters int) bool {
	embedFn.mu.Lock()
	defer embedFn.mu.Unlock()
	if len(embedFn.inFlight) != 1 {
		return false
	}
	for _, call := range embedFn.inFlight {
		return call.waiters == waiters
	}
	return false
}

func receiveFromChannel[T any](t *testing.T, ch <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(embeddingTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}
