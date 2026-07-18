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

func newPanicDoneContext() *panicDoneContext {
	return &panicDoneContext{
		Context:   context.Background(),
		firstDone: make(chan struct{}),
	}
}

func (c *panicDoneContext) Done() <-chan struct{} {
	if c.panicOnDone.Swap(false) {
		panic("injected context Done panic")
	}
	c.firstDoneOnce.Do(func() { close(c.firstDone) })
	return c.Context.Done()
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
	resultCh := make(chan asyncEmbeddingResult, 1)
	go func() {
		embeddings, err := batcher.CreateEmbeddings(ctx, "test/model", texts, nil)
		resultCh <- asyncEmbeddingResult{embeddings: embeddings, err: err}
	}()
	return resultCh
}

func waitForPendingCalls(t *testing.T, batcher *Batch, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		batcher.mu.Lock()
		defer batcher.mu.Unlock()
		for _, batch := range batcher.m {
			if len(batch.calls) == expected {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond)
}

func takePendingBatch(t *testing.T, batcher *Batch) (batchKey, *batchedCalls) {
	t.Helper()
	batcher.mu.Lock()
	defer batcher.mu.Unlock()
	require.Len(t, batcher.m, 1)
	for key, batch := range batcher.m {
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
	require.Panics(t, func() {
		batcher.Register("nil", nil)
	})
	require.False(t, batcher.Has("nil"))

	var typedNil *mockEmbedder
	require.Panics(t, func() {
		batcher.MustRegister("typed-nil", typedNil)
	})
	require.False(t, batcher.Has("typed-nil"))
}

func TestBatch_SingleRequest(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

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
	t.Run("caller canceled before dispatch is excluded", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(time.Hour, 2)
		batcher.Register("test", embedder)

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
		batcher.Register("test", embedder)

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
		batcher.processBatch(key, pendingBatch, embedder, "model", nil)
		require.Equal(t, int64(0), embedder.getCallCount())
	})

	t.Run("watcher panic cancels provider request", func(t *testing.T) {
		embedder := newBlockingEmbedder()
		t.Cleanup(embedder.finish)
		batcher := NewWithConfig(time.Hour, 2)
		batcher.Register("test", embedder)

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
		batcher.processBatch(key, pendingBatch, embedder, "model", nil)

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
		batcher.Register("test", embedder)

		canceledCtx, cancel := context.WithCancel(context.Background())
		canceledResult := createEmbeddingsAsync(canceledCtx, batcher, "first")
		activeResult := createEmbeddingsAsync(context.Background(), batcher, "second")
		<-embedder.started

		cancel()
		require.ErrorIs(t, (<-canceledResult).err, context.Canceled)
		select {
		case <-embedder.canceled:
			require.Fail(t, "provider request was canceled while one caller remained active")
		case <-time.After(20 * time.Millisecond):
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
		batcher.Register("test", embedder)

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
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

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
}

func TestBatch_SameOptionsAreBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "unknown/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown embedding provider")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_InvalidModelFormat(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.Register("test", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "invalidmodel", []string{"hello"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "model name must be in format")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_EmptyTexts(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := New()
	batcher.Register("test", mockEmb)

	ctx := context.Background()

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{}, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 0)
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatch_ContextCancellation1(t *testing.T) {
	mockEmb := newMockEmbedder()

	batcher := New()
	batcher.Register("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	cancel() // make sure ctx is cancelled before making the request

	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestBatch_ContextCancellation2(t *testing.T) {
	mockEmb := newMockEmbedder()
	mockEmb.setDelay(200 * time.Millisecond) // simulate a slow embedder

	batcher := New()
	batcher.Register("test", mockEmb)

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
	batcher.Register("test1", mockEmb1)
	batcher.Register("test2", mockEmb2)

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
	batcher.Register("test", mockEmb)

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
	batcher.Register("test", mockEmb)

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

func TestBatch_MaxBatchSize_ExceedsInSingleRequest(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewWithConfig(time.Hour, 3)
	batcher.Register("test", mockEmb)

	// Create a request with more texts than maxBatchSize
	texts := []string{"text1", "text2", "text3", "text4", "text5"} // 5 texts > maxBatchSize(3)

	resultCh := createEmbeddingBatchAsync(context.Background(), batcher, texts)
	select {
	case <-mockEmb.started:
	case <-time.After(time.Second):
		require.FailNow(t, "provider request did not start after the batch exceeded its limit")
	}
	result := <-resultCh

	require.NoError(t, result.err)
	require.Len(t, result.embeddings, 5)
	// Should still make only 1 call because it's a single request
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, texts, calls[0].texts)
}
