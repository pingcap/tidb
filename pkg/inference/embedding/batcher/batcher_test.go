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

package batcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/stretchr/testify/require"
)

// mockEmbedder is a mock implementation of the base.Embedder interface
type mockEmbedder struct {
	callCount int64
	mu        sync.Mutex
	calls     []mockCall
	delay     time.Duration
	err       error
	started   chan struct{}
	startOnce sync.Once
}

var _ base.Embedder = (*mockEmbedder)(nil)

type mockCall struct {
	model string
	texts []string
	opts  map[string]any
}

func newMockEmbedder() *mockEmbedder {
	return &mockEmbedder{
		calls:   make([]mockCall, 0),
		started: make(chan struct{}),
	}
}

func (m *mockEmbedder) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *mockEmbedder) setDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.delay = delay
}

func (m *mockEmbedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	atomic.AddInt64(&m.callCount, 1)
	m.startOnce.Do(func() { close(m.started) })

	m.mu.Lock()
	m.calls = append(m.calls, mockCall{
		model: model,
		texts: append([]string{}, texts...), // copy slice
		opts:  opts,
	})
	delay := m.delay
	err := m.err
	m.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if err != nil {
		return nil, err
	}

	// Generate mock embeddings (each text gets a simple embedding based on its length)
	embeddings := make([][]float32, len(texts))
	for i, text := range texts {
		embedding := make([]float32, 10) // 10-dimensional embeddings
		for j := range embedding {
			embedding[j] = float32(len(text) + i + j)
		}
		embeddings[i] = embedding
	}

	return embeddings, nil
}

func (m *mockEmbedder) getCallCount() int64 {
	return atomic.LoadInt64(&m.callCount)
}

func (m *mockEmbedder) getCalls() []mockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]mockCall{}, m.calls...)
}

type blockingEmbedder struct {
	started     chan struct{}
	release     chan struct{}
	canceled    chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
	cancelOnce  sync.Once
	mu          sync.Mutex
	texts       []string
}

type panicDoneContext struct {
	context.Context
	firstDone     chan struct{}
	firstDoneOnce sync.Once
	panicOnDone   atomic.Bool
}

type observedDoneContext struct {
	context.Context
	targetCalls int32
	doneCalls   atomic.Int32
	observed    chan struct{}
	observeOnce sync.Once
}

func newPanicDoneContext() *panicDoneContext {
	return &panicDoneContext{
		Context:   context.Background(),
		firstDone: make(chan struct{}),
	}
}

func newObservedDoneContext(ctx context.Context, targetCalls int32) *observedDoneContext {
	return &observedDoneContext{
		Context:     ctx,
		targetCalls: targetCalls,
		observed:    make(chan struct{}),
	}
}

func (c *panicDoneContext) Done() <-chan struct{} {
	if c.panicOnDone.Swap(false) {
		panic("injected context Done panic")
	}
	c.firstDoneOnce.Do(func() { close(c.firstDone) })
	return c.Context.Done()
}

func (c *observedDoneContext) Done() <-chan struct{} {
	if c.doneCalls.Add(1) >= c.targetCalls {
		c.observeOnce.Do(func() { close(c.observed) })
	}
	return c.Context.Done()
}

type panickingEmbedder struct{}

func (*panickingEmbedder) CreateEmbeddings(context.Context, string, []string, map[string]any) ([][]float32, error) {
	panic("injected embedder panic")
}

func newBlockingEmbedder() *blockingEmbedder {
	return &blockingEmbedder{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
}

func (b *blockingEmbedder) CreateEmbeddings(ctx context.Context, _ string, texts []string, _ map[string]any) ([][]float32, error) {
	b.mu.Lock()
	b.texts = append([]string(nil), texts...)
	b.mu.Unlock()
	b.startedOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		embeddings := make([][]float32, len(texts))
		for i, text := range texts {
			embeddings[i] = []float32{float32(len(text))}
		}
		return embeddings, nil
	case <-ctx.Done():
		b.cancelOnce.Do(func() { close(b.canceled) })
		return nil, context.Cause(ctx)
	}
}

func (b *blockingEmbedder) finish() {
	b.releaseOnce.Do(func() { close(b.release) })
}

func (b *blockingEmbedder) getTexts() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.texts...)
}

type asyncEmbeddingResult struct {
	embeddings [][]float32
	err        error
}

func createEmbeddingsAsync(ctx context.Context, batcher *Batch, text string) <-chan asyncEmbeddingResult {
	return createEmbeddingBatchAsync(ctx, batcher, []string{text})
}

func createEmbeddingBatchAsync(ctx context.Context, batcher *Batch, texts []string) <-chan asyncEmbeddingResult {
	return createEmbeddingBatchWithOptsAsync(ctx, batcher, texts, nil)
}

func createEmbeddingBatchWithOptsAsync(
	ctx context.Context,
	batcher *Batch,
	texts []string,
	opts map[string]any,
) <-chan asyncEmbeddingResult {
	resultCh := make(chan asyncEmbeddingResult, 1)
	go func() {
		embeddings, err := batcher.CreateEmbeddings(ctx, "test/model", texts, opts)
		resultCh <- asyncEmbeddingResult{embeddings: embeddings, err: err}
	}()
	return resultCh
}

func waitForPendingCalls(t *testing.T, batcher *Batch, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		batcher.mu.Lock()
		defer batcher.mu.Unlock()
		for _, batches := range batcher.m {
			for _, batch := range batches {
				if len(batch.calls) == expected {
					return true
				}
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func waitForTotalPendingCalls(t *testing.T, batcher *Batch, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		batcher.mu.Lock()
		defer batcher.mu.Unlock()
		count := 0
		for _, batches := range batcher.m {
			for _, batch := range batches {
				count += len(batch.calls)
			}
		}
		return count == expected
	}, time.Second, time.Millisecond)
}

type pendingBatch struct {
	key   batchKey
	batch *batchedCalls
}

func takePendingBatches(t *testing.T, batcher *Batch) []pendingBatch {
	t.Helper()
	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	var pending []pendingBatch
	for key, batches := range batcher.m {
		for _, batch := range batches {
			require.True(t, batch.timer.Stop())
			pending = append(pending, pendingBatch{key: key, batch: batch})
		}
	}
	return pending
}

func takePendingBatch(t *testing.T, batcher *Batch) (batchKey, *batchedCalls) {
	t.Helper()
	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	require.Len(t, batcher.m, 1)
	for key, batches := range batcher.m {
		require.Len(t, batches, 1)
		batch := batches[0]
		require.True(t, batch.timer.Stop())
		return key, batch
	}
	panic("unreachable")
}

func TestBatchKeyUsesFixedSizeOptionsDigest(t *testing.T) {
	first, err := newBatchKey("test", "model", map[string]any{"a": 1, "b": 2})
	require.NoError(t, err)
	second, err := newBatchKey("test", "model", map[string]any{"b": 2, "a": 1})
	require.NoError(t, err)
	require.Equal(t, first, second)

	different, err := newBatchKey("test", "model", map[string]any{"a": 1, "b": 3})
	require.NoError(t, err)
	require.NotEqual(t, first, different)

	large, err := newBatchKey("test", "model", map[string]any{"value": strings.Repeat("x", 64*1024)})
	require.NoError(t, err)
	require.Len(t, large.optsDigest, 32)

	_, err = newBatchKey("test", "model", map[string]any{"unsupported": make(chan struct{})})
	require.ErrorContains(t, err, "failed to serialize opts")
}

func TestBatchRegistrationRejectsNil(t *testing.T) {
	batcher := New()
	require.EqualError(t, batcher.Register("nil", nil), `embedding provider "nil" is nil`)
	require.False(t, batcher.Has("nil"))

	var typedNil *mockEmbedder
	require.EqualError(t, batcher.Register("typed-nil", typedNil), `embedding provider "typed-nil" is nil`)
	require.False(t, batcher.Has("typed-nil"))

	embedder := newMockEmbedder()
	require.EqualError(t, batcher.Register("", embedder), `invalid embedding provider: ""`)
	require.EqualError(t, batcher.Register("invalid/name", embedder), `invalid embedding provider: "invalid/name"`)

	require.NoError(t, batcher.Register("test", embedder))
	require.True(t, batcher.Has(" TEST "))
	replacement := newMockEmbedder()
	require.EqualError(t, batcher.Register(" TEST ", replacement), `embedding provider "test" is already registered`)
	require.Same(t, embedder, batcher.embedders["test"])
	require.PanicsWithError(t, `embedding provider "test" is already registered`, func() {
		batcher.MustRegister("test", replacement)
	})
}

func TestBatch_SingleRequest(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()
	texts := []string{"hello", "world"}

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 2)
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, "model1", calls[0].model)
	require.Equal(t, texts, calls[0].texts)
	require.Nil(t, calls[0].opts)
}

func TestBatch_BatchingSameModel(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewWithConfig(time.Hour, 3)
	batcher.MustRegister("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Launch multiple concurrent requests with the same model
	var wg sync.WaitGroup
	results := make([][]float32, 3)
	errors := make([]error, 3)

	texts := [][]string{
		{"hello"},
		{"world"},
		{"test"},
	}

	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(len(texts))
	for i := range texts {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ready.Done()
			<-start
			embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", texts[idx], nil)
			errors[idx] = err
			if err == nil {
				results[idx] = embeddings[0] // each request has 1 text
			}
		}(i)
	}

	ready.Wait()
	close(start)
	wg.Wait()

	// Check that all requests succeeded
	for i := range errors {
		require.NoError(t, errors[i])
		require.NotNil(t, results[i])
	}

	// Should have made only 1 API call due to batching
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, "model1", calls[0].model)
	// Check that all texts are present, but don't check order since goroutines can execute in any order
	require.ElementsMatch(t, []string{"hello", "world", "test"}, calls[0].texts)
}

func TestBatch_ConcurrentRequests(t *testing.T) {
	const requestCount = 128

	mockEmb := newMockEmbedder()
	batcher := NewWithConfig(5*time.Second, requestCount)
	batcher.MustRegister("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make([][][]float32, requestCount)
	errs := make([]error, requestCount)
	var wg sync.WaitGroup
	for i := range requestCount {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = batcher.CreateEmbeddings(ctx, "test/model", []string{fmt.Sprintf("text-%d", idx)}, nil)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range requestCount {
		require.NoError(t, errs[i])
		require.Len(t, results[i], 1)
	}
	require.Equal(t, int64(1), mockEmb.getCallCount())
}

func TestBatchCancellationAcrossCallers(t *testing.T) {
	t.Run("caller can reuse texts after cancellation", func(t *testing.T) {
		embedder := newMockEmbedder()
		batcher := NewWithConfig(time.Hour, 2)
		batcher.MustRegister("test", embedder)

		ctx, cancel := context.WithCancel(context.Background())
		texts := []string{"original"}
		resultCh := createEmbeddingBatchAsync(ctx, batcher, texts)
		waitForPendingCalls(t, batcher, 1)

		cancel()
		require.ErrorIs(t, (<-resultCh).err, context.Canceled)
		texts[0] = "reused"

		key, pendingBatch := takePendingBatch(t, batcher)
		require.Equal(t, []string{"original"}, pendingBatch.calls[0].texts)
		batcher.processBatch(key, pendingBatch, embedder, "model")
		require.Equal(t, int64(0), embedder.getCallCount())
	})

	t.Run("caller canceled before dispatch is excluded", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(time.Hour, 2)
		batcher.MustRegister("test", embedder)

		canceledCtx, cancel := context.WithCancel(context.Background())
		canceledResult := createEmbeddingsAsync(canceledCtx, batcher, "private")
		waitForPendingCalls(t, batcher, 1)
		cancel()
		require.ErrorIs(t, (<-canceledResult).err, context.Canceled)

		activeResult := createEmbeddingsAsync(context.Background(), batcher, "active")
		select {
		case <-embedder.started:
		case <-time.After(time.Second):
			require.FailNow(t, "provider request did not start")
		}
		require.Equal(t, []string{"active"}, embedder.getTexts())

		embedder.finish()
		result := <-activeResult
		require.NoError(t, result.err)
		require.Equal(t, [][]float32{{6}}, result.embeddings)
	})

	t.Run("all callers canceled before dispatch skip provider", func(t *testing.T) {
		embedder := newMockEmbedder()
		batcher := NewWithConfig(time.Hour, 3)
		batcher.MustRegister("test", embedder)

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		firstResult := createEmbeddingsAsync(firstCtx, batcher, "first")
		secondResult := createEmbeddingsAsync(secondCtx, batcher, "second")
		waitForPendingCalls(t, batcher, 2)

		cancelFirst()
		cancelSecond()
		require.ErrorIs(t, (<-firstResult).err, context.Canceled)
		require.ErrorIs(t, (<-secondResult).err, context.Canceled)

		key, pendingBatch := takePendingBatch(t, batcher)
		batcher.processBatch(key, pendingBatch, embedder, "model")
		require.Equal(t, int64(0), embedder.getCallCount())
	})

	t.Run("watcher panic cancels provider request", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(time.Hour, 2)
		batcher.MustRegister("test", embedder)

		ctx := newPanicDoneContext()
		resultCh := createEmbeddingsAsync(ctx, batcher, "test")
		waitForPendingCalls(t, batcher, 1)
		select {
		case <-ctx.firstDone:
		case <-time.After(time.Second):
			require.FailNow(t, "caller did not start waiting for its result")
		}

		ctx.panicOnDone.Store(true)
		key, pendingBatch := takePendingBatch(t, batcher)
		batcher.processBatch(key, pendingBatch, embedder, "model")

		result := <-resultCh
		require.ErrorIs(t, result.err, context.Canceled)
		select {
		case <-embedder.canceled:
		case <-time.After(time.Second):
			require.FailNow(t, "provider request was not canceled after the watcher panic")
		}
	})

	t.Run("one active caller keeps provider request alive", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(5*time.Second, 2)
		batcher.MustRegister("test", embedder)

		canceledCtx, cancel := context.WithCancel(context.Background())
		canceledResult := createEmbeddingsAsync(canceledCtx, batcher, "first")
		waitForPendingCalls(t, batcher, 1)
		activeCtx := newObservedDoneContext(context.Background(), 2)
		activeResult := createEmbeddingsAsync(activeCtx, batcher, "second")
		<-embedder.started

		cancel()
		require.ErrorIs(t, (<-canceledResult).err, context.Canceled)
		select {
		case <-activeCtx.observed:
		case <-time.After(time.Second):
			require.FailNow(t, "watcher did not advance to the active caller")
		}
		select {
		case <-embedder.canceled:
			require.FailNow(t, "provider request was canceled while one caller remained active")
		default:
		}

		embedder.finish()
		result := <-activeResult
		require.NoError(t, result.err)
		require.Equal(t, [][]float32{{6}}, result.embeddings)
	})

	t.Run("all canceled callers cancel provider request", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(5*time.Second, 2)
		batcher.MustRegister("test", embedder)

		firstCtx, cancelFirst := context.WithCancel(context.Background())
		secondCtx, cancelSecond := context.WithCancel(context.Background())
		firstResult := createEmbeddingsAsync(firstCtx, batcher, "first")
		secondResult := createEmbeddingsAsync(secondCtx, batcher, "second")
		<-embedder.started

		cancelSecond()
		cancelFirst()
		require.ErrorIs(t, (<-firstResult).err, context.Canceled)
		require.ErrorIs(t, (<-secondResult).err, context.Canceled)
		select {
		case <-embedder.canceled:
		case <-time.After(time.Second):
			require.Fail(t, "provider request was not canceled after all callers canceled")
		}
	})
}

func TestBatch_DifferentModelsNotBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model2", []string{"world"}, nil)
		require.NoError(t, err)
	}()

	wg.Wait()

	// Should have made 2 API calls since models are different
	require.Equal(t, int64(2), mockEmb.getCallCount())
}

func TestBatch_DifferentOptionsNotBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, map[string]any{"task": "task1"})
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"world"}, map[string]any{"task": "task2"})
		require.NoError(t, err)
	}()

	wg.Wait()

	// Should have made 2 API calls since options are different
	require.Equal(t, int64(2), mockEmb.getCallCount())

	// JSON-equivalent values with different Go types must not be merged. The
	// provider receives the original options, so merging them would make behavior
	// depend on which goroutine created the batch first.
	intOpts := map[string]any{"plus": int(1)}
	floatOpts := map[string]any{"plus": float64(1)}
	intKey, err := newBatchKey("test", "model", intOpts)
	require.NoError(t, err)
	floatKey, err := newBatchKey("test", "model", floatOpts)
	require.NoError(t, err)
	require.Equal(t, intKey, floatKey)

	typedMockEmb := newMockEmbedder()
	typedBatcher := NewWithConfig(time.Hour, 10)
	typedBatcher.MustRegister("test", typedMockEmb)
	intResult := createEmbeddingBatchWithOptsAsync(context.Background(), typedBatcher, []string{"int"}, intOpts)
	floatResult := createEmbeddingBatchWithOptsAsync(context.Background(), typedBatcher, []string{"float"}, floatOpts)
	waitForTotalPendingCalls(t, typedBatcher, 2)
	pending := takePendingBatches(t, typedBatcher)
	require.Len(t, pending, 2)
	for _, item := range pending {
		typedBatcher.processBatch(item.key, item.batch, typedMockEmb, "model")
	}
	require.NoError(t, (<-intResult).err)
	require.NoError(t, (<-floatResult).err)

	calls := typedMockEmb.getCalls()
	require.Len(t, calls, 2)
	seenInt := false
	seenFloat := false
	for _, call := range calls {
		switch call.opts["plus"].(type) {
		case int:
			seenInt = true
		case float64:
			seenFloat = true
		}
	}
	require.True(t, seenInt)
	require.True(t, seenFloat)
}

func TestBatchOptionsAreSnapshotted(t *testing.T) {
	embedder := newMockEmbedder()
	batcher := NewWithConfig(time.Hour, 3)
	batcher.MustRegister("test", embedder)

	firstOpts := map[string]any{
		"nested": map[string]any{"dimensions": int(128)},
	}
	secondOpts := map[string]any{
		"nested": map[string]any{"dimensions": int(128)},
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult := createEmbeddingBatchWithOptsAsync(firstCtx, batcher, []string{"first"}, firstOpts)
	waitForPendingCalls(t, batcher, 1)
	secondResult := createEmbeddingBatchWithOptsAsync(context.Background(), batcher, []string{"second"}, secondOpts)
	waitForPendingCalls(t, batcher, 2)

	cancelFirst()
	require.ErrorIs(t, (<-firstResult).err, context.Canceled)
	firstOpts["nested"].(map[string]any)["dimensions"] = int(512)

	key, pendingBatch := takePendingBatch(t, batcher)
	batcher.processBatch(key, pendingBatch, embedder, "model")
	result := <-secondResult
	require.NoError(t, result.err)

	calls := embedder.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, []string{"second"}, calls[0].texts)
	require.Equal(t, secondOpts, calls[0].opts)
	require.IsType(t, int(0), calls[0].opts["nested"].(map[string]any)["dimensions"])
}

func TestBatchCallerResultsAreIsolated(t *testing.T) {
	embedder := newMockEmbedder()
	batcher := NewWithConfig(time.Hour, 2)
	batcher.MustRegister("test", embedder)

	firstResultCh := createEmbeddingsAsync(context.Background(), batcher, "first")
	waitForPendingCalls(t, batcher, 1)
	secondResultCh := createEmbeddingsAsync(context.Background(), batcher, "second")
	firstResult := <-firstResultCh
	secondResult := <-secondResultCh
	require.NoError(t, firstResult.err)
	require.NoError(t, secondResult.err)
	require.Equal(t, len(firstResult.embeddings), cap(firstResult.embeddings))

	secondEmbedding := append([]float32(nil), secondResult.embeddings[0]...)
	_ = append(firstResult.embeddings, []float32{999})
	require.Equal(t, secondEmbedding, secondResult.embeddings[0])
}

func TestBatchPanickingEmbedderReturnsError(t *testing.T) {
	batcher := NewWithConfig(time.Hour, 2)
	batcher.MustRegister("test", &panickingEmbedder{})

	firstResultCh := createEmbeddingsAsync(context.Background(), batcher, "first")
	waitForPendingCalls(t, batcher, 1)
	secondResultCh := createEmbeddingsAsync(context.Background(), batcher, "second")
	for _, resultCh := range []<-chan asyncEmbeddingResult{firstResultCh, secondResultCh} {
		select {
		case result := <-resultCh:
			require.Nil(t, result.embeddings)
			require.EqualError(t, result.err, "embedding batch processing panicked")
		case <-time.After(time.Second):
			require.FailNow(t, "panicking provider did not produce a terminal result for every caller")
		}
	}
}

func TestBatch_SameOptionsAreBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, map[string]any{"task": "task1"})
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"world"}, map[string]any{"task": "task1"})
		require.NoError(t, err)
	}()

	wg.Wait()

	// Should have made 1 API call since model and options are the same
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.ElementsMatch(t, []string{"hello", "world"}, calls[0].texts)
}

func TestBatch_ErrorPropagation(t *testing.T) {
	mockEmb := newMockEmbedder()
	mockEmb.setError(fmt.Errorf("API error"))

	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	var wg sync.WaitGroup
	errors := make([]error, 2)

	for i := range errors {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)
			errors[idx] = err
		}(i)
	}

	wg.Wait()

	// Both requests should get the same error
	for _, err := range errors {
		require.Error(t, err)
		require.Contains(t, err.Error(), "API error")
	}

	require.Equal(t, int64(1), mockEmb.getCallCount())
}

func TestBatch_UnknownPrefix(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("zeta", mockEmb)
	batcher.MustRegister("alpha", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "unknown/model1", []string{"hello"}, nil)

	require.EqualError(t, err, "unknown embedding provider 'unknown', available providers: alpha, zeta")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_InvalidModelFormat(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "invalidmodel", []string{"hello"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "model name must be in format")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_EmptyTexts(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{}, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 0)
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_ContextCancellation1(t *testing.T) {
	mockEmb := newMockEmbedder()

	batcher := NewWithConfig(time.Hour, DefaultMaxBatchSize)
	batcher.MustRegister("test", mockEmb)

	customCause := errors.New("custom cancellation cause")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(customCause) // make sure ctx is cancelled before making the request

	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)

	require.ErrorIs(t, err, customCause)
	require.Equal(t, int64(0), mockEmb.getCallCount())
	batcher.mu.Lock()
	require.Empty(t, batcher.m)
	batcher.mu.Unlock()

	// Directly resolve a ready provider result to deterministically verify that
	// its generic context error cannot replace the caller's custom cause.
	_, err = resolveBatchResult(ctx, &batchResult{err: context.Canceled})
	require.ErrorIs(t, err, customCause)
}

func TestBatch_ContextCancellation2(t *testing.T) {
	mockEmb := newMockEmbedder()
	mockEmb.setDelay(200 * time.Millisecond) // simulate a slow embedder

	batcher := New()
	batcher.MustRegister("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestBatch_MultipleEmbedders(t *testing.T) {
	mockEmb1 := newMockEmbedder()
	mockEmb2 := newMockEmbedder()

	batcher := New()
	batcher.MustRegister("test1", mockEmb1)
	batcher.MustRegister("test2", mockEmb2)

	ctx := context.Background()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test1/model1", []string{"hello"}, nil)
		require.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := batcher.CreateEmbeddings(ctx, "test2/model1", []string{"world"}, nil)
		require.NoError(t, err)
	}()

	wg.Wait()

	require.Equal(t, int64(1), mockEmb1.getCallCount())
	require.Equal(t, int64(1), mockEmb2.getCallCount())
}

func TestBatch_BatchWindow(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewWithConfig(100*time.Millisecond, 10)
	batcher.MustRegister("test", mockEmb)

	ctx := context.Background()

	// First request
	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)
	require.NoError(t, err)

	// Second request should create a new batch
	// because the first one is returned only after batch is processed
	_, err = batcher.CreateEmbeddings(ctx, "test/model1", []string{"world"}, nil)
	require.NoError(t, err)

	// Should have made 2 API calls since they were in different batch windows
	require.Equal(t, int64(2), mockEmb.getCallCount())
}

func TestBatch_MaxBatchSize_ExactLimit(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewWithConfig(time.Hour, 3)
	batcher.MustRegister("test", mockEmb)

	// Create exactly maxBatchSize texts in one request
	texts := make([]string, 3)
	for i := range texts {
		texts[i] = fmt.Sprintf("text_%d", i)
	}

	resultCh := createEmbeddingBatchAsync(context.Background(), batcher, texts)
	select {
	case <-mockEmb.started:
	case <-time.After(time.Second):
		require.FailNow(t, "provider request did not start after the batch reached its exact limit")
	}
	result := <-resultCh

	require.NoError(t, result.err)
	require.Len(t, result.embeddings, 3)
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, texts, calls[0].texts)
}

func TestBatch_MaxBatchSize_ChunksProviderRequests(t *testing.T) {
	t.Run("single caller exceeds the limit", func(t *testing.T) {
		mockEmb := newMockEmbedder()
		batcher := NewWithConfig(time.Hour, 3)
		batcher.MustRegister("test", mockEmb)

		texts := []string{"text1", "text2", "text3", "text4", "text5"}
		result := <-createEmbeddingBatchAsync(context.Background(), batcher, texts)

		require.NoError(t, result.err)
		require.Len(t, result.embeddings, 5)
		require.Equal(t, int64(2), mockEmb.getCallCount())

		calls := mockEmb.getCalls()
		require.Len(t, calls, 2)
		require.Equal(t, texts[:3], calls[0].texts)
		require.Equal(t, texts[3:], calls[1].texts)
	})

	t.Run("batched callers cross a chunk boundary", func(t *testing.T) {
		mockEmb := newMockEmbedder()
		batcher := NewWithConfig(time.Hour, 3)
		batcher.MustRegister("test", mockEmb)

		firstTexts := []string{"first1", "first2"}
		secondTexts := []string{"second1", "second2"}
		firstResult := createEmbeddingBatchAsync(context.Background(), batcher, firstTexts)
		waitForPendingCalls(t, batcher, 1)
		secondResult := createEmbeddingBatchAsync(context.Background(), batcher, secondTexts)

		first := <-firstResult
		second := <-secondResult
		require.NoError(t, first.err)
		require.NoError(t, second.err)
		require.Len(t, first.embeddings, len(firstTexts))
		require.Len(t, second.embeddings, len(secondTexts))

		calls := mockEmb.getCalls()
		require.Len(t, calls, 2)
		require.Equal(t, []string{"first1", "first2", "second1"}, calls[0].texts)
		require.Equal(t, []string{"second2"}, calls[1].texts)
	})
}
