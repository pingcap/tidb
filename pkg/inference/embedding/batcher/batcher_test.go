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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// mockEmbedder is a mock implementation of the base.Embedder interface
type mockEmbedder struct {
	callCount int64
	mu        sync.Mutex
	calls     []mockCall
	delay     time.Duration
	err       error
}

var _ base.Embedder = (*mockEmbedder)(nil)

type mockCall struct {
	model string
	texts []string
	opts  map[string]any
}

func newMockEmbedder() *mockEmbedder {
	return &mockEmbedder{
		calls: make([]mockCall, 0),
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

func TestBatchEmbedder_SingleRequest(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_BatchingSameModel(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx := context.Background()

	// Launch multiple concurrent requests with the same model
	var wg sync.WaitGroup
	results := make([][]float32, 3)
	errors := make([]error, 3)

	texts := [][]string{
		{"hello"},
		{"world"},
		{"test"},
	}

	for i := range texts {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", texts[idx], nil)
			results[idx] = embeddings[0] // each request has 1 text
			errors[idx] = err
		}(i)
	}

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

func TestBatchEmbedder_DifferentModelsNotBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_DifferentOptionsNotBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_SameOptionsAreBatched(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_ErrorPropagation(t *testing.T) {
	mockEmb := newMockEmbedder()
	mockEmb.setError(fmt.Errorf("API error"))

	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_UnknownPrefix(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "unknown/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown embedding provider")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatchEmbedder_InvalidModelFormat(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx := context.Background()

	_, err := batcher.CreateEmbeddings(ctx, "invalidmodel", []string{"hello"}, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(), "model name must be in format")
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatchEmbedder_EmptyTexts(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx := context.Background()

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{}, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 0)
	require.Equal(t, int64(0), mockEmb.getCallCount())
}

func TestBatchEmbedder_ContextCancellation1(t *testing.T) {
	mockEmb := newMockEmbedder()

	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	cancel() // make sure ctx is cancelled before making the request

	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestBatchEmbedder_ContextCancellation2(t *testing.T) {
	mockEmb := newMockEmbedder()
	mockEmb.setDelay(200 * time.Millisecond) // simulate a slow embedder

	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test", mockEmb)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := batcher.CreateEmbeddings(ctx, "test/model1", []string{"hello"}, nil)

	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestBatchEmbedder_MultipleEmbedders(t *testing.T) {
	mockEmb1 := newMockEmbedder()
	mockEmb2 := newMockEmbedder()

	batcher := NewBatchEmbedder()
	batcher.RegisterEmbedder("test1", mockEmb1)
	batcher.RegisterEmbedder("test2", mockEmb2)

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

func TestBatchEmbedder_BatchWindow(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedderWithConfig(100*time.Millisecond, 10)
	batcher.RegisterEmbedder("test", mockEmb)

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

func TestBatchEmbedder_MaxBatchSize_ExactLimit(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedderWithConfig(5*time.Second, 3) // Long window to avoid time-based batching
	batcher.RegisterEmbedder("test", mockEmb)

	start := time.Now()
	ctx := context.Background()

	// Create exactly maxBatchSize texts in one request
	texts := make([]string, 3)
	for i := range texts {
		texts[i] = fmt.Sprintf("text_%d", i)
	}

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 3)
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, texts, calls[0].texts)

	// Ensure the batch was processed immediately for exact maxBatchSize
	require.LessOrEqual(t, time.Since(start), 100*time.Millisecond)
}

func TestBatchEmbedder_MaxBatchSize_ExceedsInSingleRequest(t *testing.T) {
	mockEmb := newMockEmbedder()
	batcher := NewBatchEmbedderWithConfig(5*time.Second, 3)
	batcher.RegisterEmbedder("test", mockEmb)

	start := time.Now()
	ctx := context.Background()

	// Create a request with more texts than maxBatchSize
	texts := []string{"text1", "text2", "text3", "text4", "text5"} // 5 texts > maxBatchSize(3)

	embeddings, err := batcher.CreateEmbeddings(ctx, "test/model1", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 5)
	// Should still make only 1 call because it's a single request
	require.Equal(t, int64(1), mockEmb.getCallCount())

	calls := mockEmb.getCalls()
	require.Len(t, calls, 1)
	require.Equal(t, texts, calls[0].texts)

	// Ensure the batch was processed immediately for exact maxBatchSize
	require.LessOrEqual(t, time.Since(start), 100*time.Millisecond)
}
