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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util"
)

const (
	// DefaultBatchWindow is the default time window for batching requests.
	DefaultBatchWindow = 100 * time.Millisecond

	// DefaultMaxBatchSize is the default maximum number of requests to batch together.
	DefaultMaxBatchSize = 16
)

// Batch batches requests to underlying embedders within a time window
// to reduce the number of API calls.
type Batch struct {
	embedders map[string]base.Embedder // name (jina/openai/...) -> embedder

	batchWindow  time.Duration
	maxBatchSize int

	mu sync.Mutex                 // protects m
	m  map[batchKey]*batchedCalls // key -> call
}

var _ base.Embedder = (*Batch)(nil)

type batchKey struct {
	provider   string
	model      string
	optsDigest [sha256.Size]byte
}

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

// New creates a batch embedder with the default configuration.
func New() *Batch {
	return NewWithConfig(DefaultBatchWindow, DefaultMaxBatchSize)
}

// NewWithConfig creates a batch embedder with the given configuration.
// batchWindow: how long to wait for more requests before processing the batch
// maxBatchSize: maximum number of requests to batch together
func NewWithConfig(batchWindow time.Duration, maxBatchSize int) *Batch {
	if batchWindow <= 0 {
		batchWindow = DefaultBatchWindow
	}
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	return &Batch{
		embedders:    make(map[string]base.Embedder),
		m:            make(map[batchKey]*batchedCalls),
		batchWindow:  batchWindow,
		maxBatchSize: maxBatchSize,
	}
}

// Register registers an embedder with the given provider name as the prefix.
// This is not concurrent-safe. It must be called before any CreateEmbeddings calls.
func (b *Batch) Register(provider string, embedder base.Embedder) {
	provider = normalizeProviderName(provider)
	panicIfNilEmbedder(provider, embedder)
	b.embedders[provider] = embedder
}

// MustRegister registers an embedder and panics if the provider is invalid or duplicated.
func (b *Batch) MustRegister(provider string, embedder base.Embedder) {
	provider = normalizeProviderName(provider)
	if provider == "" || strings.Contains(provider, "/") {
		panic(fmt.Sprintf("invalid embedding provider: %q", provider))
	}
	if _, ok := b.embedders[provider]; ok {
		panic(fmt.Sprintf("embedding provider %q is already registered", provider))
	}
	b.Register(provider, embedder)
}

// Has returns whether a provider is registered.
func (b *Batch) Has(provider string) bool {
	provider = normalizeProviderName(provider)
	_, ok := b.embedders[provider]
	return ok
}

func (b *Batch) parseModelWithProvider(modelWithProvider string) (string, string, error) {
	parts := strings.SplitN(modelWithProvider, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("model name must be in format 'provider/model', got: %s", modelWithProvider)
	}
	embedderName := normalizeProviderName(parts[0])
	actualModel := strings.TrimSpace(parts[1])
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

func normalizeProviderName(provider string) string {
	return strings.ToLower(strings.TrimSpace(provider))
}

func panicIfNilEmbedder(provider string, embedder base.Embedder) {
	if embedder == nil {
		panic(fmt.Sprintf("embedding provider %q is nil", provider))
	}
	value := reflect.ValueOf(embedder)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if value.IsNil() {
			panic(fmt.Sprintf("embedding provider %q is nil", provider))
		}
	}
}

func newBatchKey(provider, model string, opts map[string]any) (batchKey, error) {
	optsJSON, err := json.Marshal(opts)
	if err != nil {
		return batchKey{}, fmt.Errorf("failed to serialize opts: %w", err)
	}
	return batchKey{
		provider:   provider,
		model:      model,
		optsDigest: sha256.Sum256(optsJSON),
	}, nil
}

// CreateEmbeddings batches requests with the same model/opts combination within the batch window.
func (b *Batch) CreateEmbeddings(ctx context.Context, modelWithProvider string, texts []string, opts map[string]any) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}

	embedderName, actualModel, err := b.parseModelWithProvider(modelWithProvider)
	if err != nil {
		return nil, err
	}
	embedder := b.embedders[embedderName]

	// A batch key includes the provider, actual model, and a fixed-size digest
	// of the serialized options, so different inputs create different batches
	// without retaining potentially large option strings in the batch map.
	key, err := newBatchKey(embedderName, actualModel, opts)
	if err != nil {
		return nil, err
	}

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
			b.processBatch(key, bc, embedder, actualModel, opts)
		})
		return bc
	}

	// Add to batch
	b.mu.Lock()
	currentBatch, exists := b.m[key]
	if !exists {
		currentBatch = createNewBatch()
		b.m[key] = currentBatch
	} else {
		currentBatch.calls = append(currentBatch.calls, thisCall)
		currentBatch.batchedTextsN += len(texts)
	}
	// If the batch is full, seal this batch.
	// Sealing is done by simply dropping the reference to the current batch
	// so that no one else can modify it any more.
	if currentBatch.batchedTextsN >= b.maxBatchSize {
		delete(b.m, key)
		// currentBatch is now sealed, let's trigger it immediately without waiting for the timer.
		// For an AfterFunc timer, Stop returning false means its callback has already started.
		// Resetting in that case could schedule a second concurrent processBatch call.
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
		return nil, ctx.Err()
	}
}

// processBatch processes a batch of requests.
func (b *Batch) processBatch(key batchKey, thisBatch *batchedCalls, embedder base.Embedder, model string, opts map[string]any) {
	// Take out the current batch from the map to gain ownership. After this point,
	// no other goroutine can modify this batch.
	// Note that the current batch may be already taken out if there are too many requests.
	{
		b.mu.Lock()
		activeBatch, exists := b.m[key]
		if exists && thisBatch == activeBatch {
			delete(b.m, key)
		}
		b.mu.Unlock()
	}

	// Now we are the owner of the current batch, no synchronization is needed.

	// Exclude calls that were canceled before provider dispatch. Their callers
	// return independently through ctx.Done(), and sending their text would
	// waste provider capacity and violate the cancellation expectation.
	activeCalls := make([]*call, 0, len(thisBatch.calls))
	allTexts := make([]string, 0, thisBatch.batchedTextsN)
	startIndices := make([]int, 0, len(thisBatch.calls))
	for _, call := range thisBatch.calls {
		if call.ctx.Err() != nil {
			continue
		}
		activeCalls = append(activeCalls, call)
		startIndices = append(startIndices, len(allTexts))
		allTexts = append(allTexts, call.texts...)
	}
	if len(activeCalls) == 0 {
		return
	}

	// Make the actual embeddings call
	// The embedding call will be cancelled only if all calls in this batch are cancelled.
	reqCtx, cancelReq := context.WithCancel(context.Background())
	var watcherWG util.WaitGroupWrapper
	watcherWG.RunWithRecover(func() {
		// Waiting sequentially is intentional: the loop can finish only after
		// every caller context is canceled. If the provider returns first,
		// cancelReq releases this watcher through reqCtx.Done().
		for _, call := range activeCalls {
			select {
			case <-call.ctx.Done():
			case <-reqCtx.Done():
				// The req context has already been cancelled, for example, the request
				// is finished. No need to check further.
				return
			}
		}
		cancelReq()
	}, func(r any) {
		if r != nil {
			cancelReq()
		}
	})
	embeddings, err := func() ([][]float32, error) {
		defer func() {
			cancelReq()
			watcherWG.Wait()
		}()
		return embedder.CreateEmbeddings(reqCtx, model, allTexts, opts)
	}()

	// Send results back to all requests

	if err != nil {
		for _, call := range activeCalls {
			select {
			case call.resultCh <- &batchResult{err: err}:
			case <-call.ctx.Done():
				// Request was cancelled, don't block
			}
		}
		return
	}
	if len(embeddings) != len(allTexts) {
		if len(embeddings) == 0 {
			err = fmt.Errorf("no embeddings returned for model %s", model)
		} else {
			err = fmt.Errorf("embedding provider returned %d embeddings for %d texts", len(embeddings), len(allTexts))
		}
		for _, call := range activeCalls {
			select {
			case call.resultCh <- &batchResult{err: err}:
			case <-call.ctx.Done():
				// Request was cancelled, don't block
			}
		}
		return
	}
	for i, call := range activeCalls {
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
