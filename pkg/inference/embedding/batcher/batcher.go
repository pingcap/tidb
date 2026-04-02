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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

const (
	// DefaultBatchWindow is the default time window for batching requests.
	DefaultBatchWindow = 100 * time.Millisecond

	// DefaultMaxBatchSize is the default maximum number of requests to batch together.
	DefaultMaxBatchSize = 16
)

// BatchEmbedder batches requests to underlying embedders within a time window
// to reduce the number of API calls.
type BatchEmbedder struct {
	embedders map[string]base.Embedder // name (jina/openai/...) -> embedder

	batchWindow  time.Duration
	maxBatchSize int

	mu sync.Mutex               // protects m
	m  map[string]*batchedCalls // key -> call
}

var _ base.Embedder = (*BatchEmbedder)(nil)

// batchedCalls represents a collection of requests that should be batched together.
type batchedCalls struct {
	calls         []*call
	timer         *time.Timer // For early trigger
	batchedTextsN int
}

// call represents a single request waiting to be batched.
// (i.e., a single call to CreateEmbeddings())
type call struct {
	ctx      context.Context
	texts    []string
	resultCh chan *batchResult
}

// batchResult represents the result of a batched operation.
type batchResult struct {
	embeddings [][]float32
	err        error
}

// NewBatchEmbedder creates a new batch embedder with the given batch window.
func NewBatchEmbedder() *BatchEmbedder {
	return NewBatchEmbedderWithConfig(DefaultBatchWindow, DefaultMaxBatchSize)
}

// NewBatchEmbedderWithConfig creates a new batch embedder with the given configuration.
// batchWindow: how long to wait for more requests before processing the batch
// maxBatchSize: maximum number of requests to batch together
func NewBatchEmbedderWithConfig(batchWindow time.Duration, maxBatchSize int) *BatchEmbedder {
	if batchWindow <= 0 {
		batchWindow = DefaultBatchWindow
	}
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	return &BatchEmbedder{
		embedders:    make(map[string]base.Embedder),
		m:            make(map[string]*batchedCalls),
		batchWindow:  batchWindow,
		maxBatchSize: maxBatchSize,
	}
}

// RegisterEmbedder registers an embedder with the given provider name as the prefix.
// This is not concurrent-safe. It must be called before any CreateEmbeddings calls.
func (b *BatchEmbedder) RegisterEmbedder(provider string, embedder base.Embedder) {
	b.embedders[provider] = embedder
}

func (b *BatchEmbedder) parseModelWithProvider(modelWithProvider string) (string, string, error) {
	parts := strings.SplitN(modelWithProvider, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("model name must be in format 'provider/model', got: %s", modelWithProvider)
	}
	embedderName := parts[0]
	actualModel := parts[1]
	_, exists := b.embedders[embedderName]
	if !exists {
		p := []string{}
		for key := range b.embedders {
			p = append(p, key)
		}
		availableProviders := strings.Join(p, ", ")
		return "", "", fmt.Errorf(
			"unknown embedding provider '%s', available providers: %s",
			embedderName,
			availableProviders)
	}
	return embedderName, actualModel, nil
}

// CreateEmbeddings batches requests with the same model/opts combination within the batch window.
func (b *BatchEmbedder) CreateEmbeddings(ctx context.Context, modelWithProvider string, texts []string, opts map[string]any) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	embedderName, actualModel, err := b.parseModelWithProvider(modelWithProvider)
	if err != nil {
		return nil, err
	}
	embedder := b.embedders[embedderName]

	// A batch key includes the actual model and serialized options, so that
	// different models or options create different batches.
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize opts: %w", err)
	}
	batchKey := fmt.Sprintf("%s/%s/%s", embedderName, actualModel, string(optsJSON))

	thisCall := &call{
		ctx:      ctx,
		texts:    texts,
		resultCh: make(chan *batchResult, 1),
	}

	createNewBatch := func() *batchedCalls {
		bc := &batchedCalls{
			calls:         []*call{thisCall},
			batchedTextsN: len(texts),
		}
		bc.timer = time.AfterFunc(b.batchWindow, func() {
			b.processBatch(batchKey, bc, embedder, actualModel, opts)
		})
		return bc
	}

	// Add to batch
	b.mu.Lock()
	currentBatch, exists := b.m[batchKey]
	if !exists {
		currentBatch = createNewBatch()
		b.m[batchKey] = currentBatch
	} else {
		currentBatch.calls = append(currentBatch.calls, thisCall)
		currentBatch.batchedTextsN += len(texts)
	}
	// If the batch is full, seal this batch.
	// Sealing is done by simply dropping the reference to the current batch
	// so that no one else can modify it any more.
	if currentBatch.batchedTextsN >= b.maxBatchSize {
		delete(b.m, batchKey)
		// currentBatch is now sealed, let's trigger it immediately without waiting for the timer.
		if currentBatch.timer.Stop() {
			currentBatch.timer.Reset(0)
		}
	}
	b.mu.Unlock()

	// Wait result
	select {
	case result := <-thisCall.resultCh:
		if result.err != nil {
			return nil, result.err
		}
		return result.embeddings, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// processBatch processes a batch of requests.
func (b *BatchEmbedder) processBatch(batchKey string, thisBatch *batchedCalls, embedder base.Embedder, model string, opts map[string]any) {
	// Take out the current batch from the map to gain ownership. After this point,
	// no other goroutine can modify this batch.
	// Note that the current batch may be already taken out if there are too many requests.
	{
		b.mu.Lock()
		activeBatch, exists := b.m[batchKey]
		if exists && thisBatch == activeBatch {
			delete(b.m, batchKey)
		}
		b.mu.Unlock()
	}

	// Now we are the owner of the current batch, no synchronization is needed.

	// Collect all texts and track start indices for each request
	var allTexts []string
	startIndices := make([]int, len(thisBatch.calls))

	for i, call := range thisBatch.calls {
		startIndices[i] = len(allTexts)
		allTexts = append(allTexts, call.texts...)
	}

	// Make the actual embeddings call
	// The embedding call will be cancelled only if all calls in this batch are cancelled.
	reqCtx, cancelReq := context.WithCancel(context.Background())
	go func() {
		for _, call := range thisBatch.calls {
			select {
			case <-call.ctx.Done():
				// One context is cancelled:
				// Do nothing, continue the loop and wait for the next context's cancellation
			case <-reqCtx.Done():
				// The req context has already been cancelled, for example, the request
				// is finished. No need to check further.
				return
			}
		}
		cancelReq()
	}()
	embeddings, err := embedder.CreateEmbeddings(reqCtx, model, allTexts, opts)
	cancelReq()

	// Send results back to all requests

	if err != nil {
		for _, call := range thisBatch.calls {
			select {
			case call.resultCh <- &batchResult{err: err}:
			case <-call.ctx.Done():
				// Request was cancelled, don't block
			}
		}
		return
	}
	for i, call := range thisBatch.calls {
		startIdx := startIndices[i]
		result := &batchResult{
			embeddings: embeddings[startIdx : startIdx+len(call.texts)],
		}
		select {
		case call.resultCh <- result:
		case <-call.ctx.Done():
			// Request was cancelled, don't block
		}
	}
}
