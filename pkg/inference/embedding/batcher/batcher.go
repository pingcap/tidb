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
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/copystructure"
	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util"
)

const (
	// DefaultBatchWindow is the default time window for batching requests.
	DefaultBatchWindow = 100 * time.Millisecond

	// DefaultMaxBatchSize is the default maximum number of texts sent in each
	// request to an underlying embedding provider.
	DefaultMaxBatchSize = 16
)

// Batch batches requests to underlying embedders within a time window
// to reduce the number of API calls.
type Batch struct {
	embedders map[string]base.Embedder // name (jina/openai/...) -> embedder

	batchWindow  time.Duration
	maxBatchSize int

	mu sync.Mutex                   // protects m
	m  map[batchKey][]*batchedCalls // key -> batches with the same JSON options digest
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
	opts          map[string]any
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
// maxBatchSize: maximum number of texts in each underlying provider request
func NewWithConfig(batchWindow time.Duration, maxBatchSize int) *Batch {
	if batchWindow <= 0 {
		batchWindow = DefaultBatchWindow
	}
	if maxBatchSize <= 0 {
		maxBatchSize = DefaultMaxBatchSize
	}
	return &Batch{
		embedders:    make(map[string]base.Embedder),
		m:            make(map[batchKey][]*batchedCalls),
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
	provider := normalizeProviderName(parts[0])
	actualModel := strings.TrimSpace(parts[1])
	_, exists := b.embedders[provider]
	if !exists {
		p := []string{}
		for key := range b.embedders {
			p = append(p, key)
		}
		sort.Strings(p)
		availableProviders := strings.Join(p, ", ")
		return "", "", fmt.Errorf(
			"unknown embedding provider '%s', available providers: %s",
			provider,
			availableProviders)
	}
	return provider, actualModel, nil
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

// snapshotOptions detaches a batch from caller-owned mutable option values.
// A canceled caller can return before the shared provider request is dispatched.
func snapshotOptions(opts map[string]any) (map[string]any, error) {
	if opts == nil {
		return nil, nil
	}
	snapshot, err := copystructure.Copy(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to snapshot opts: %w", err)
	}
	clonedOpts, ok := snapshot.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("failed to snapshot opts: unexpected copy type %T", snapshot)
	}
	return clonedOpts, nil
}

func (b *Batch) removeBatchLocked(key batchKey, target *batchedCalls) {
	batches := b.m[key]
	for i, batch := range batches {
		if batch != target {
			continue
		}
		last := len(batches) - 1
		batches[i] = batches[last]
		batches[last] = nil
		batches = batches[:last]
		if len(batches) == 0 {
			delete(b.m, key)
		} else {
			b.m[key] = batches
		}
		return
	}
}

func resolveBatchResult(ctx context.Context, result *batchResult) ([][]float32, error) {
	// Once the caller context is canceled, preserve its cause even if a provider
	// result is also ready. The aggregated provider context cannot preserve each
	// caller's custom cause.
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if result.err != nil {
		return nil, result.err
	}
	return result.embeddings, nil
}

func waitForBatchResult(ctx context.Context, resultCh <-chan *batchResult) ([][]float32, error) {
	select {
	case result := <-resultCh:
		return resolveBatchResult(ctx, result)
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func trySendBatchResult(call *call, result *batchResult) {
	select {
	case call.resultCh <- result:
	default:
	}
}

func (b *Batch) processBatchWithRecovery(key batchKey, thisBatch *batchedCalls, embedder base.Embedder, model string) {
	util.WithRecovery(func() {
		b.processBatch(key, thisBatch, embedder, model)
	}, func(r any) {
		if r == nil {
			return
		}
		result := &batchResult{
			err: errors.New("embedding batch processing panicked"),
		}
		for _, call := range thisBatch.calls {
			// A panic can occur after another result was delivered. Never block or
			// overwrite that result while completing callers that are still waiting.
			trySendBatchResult(call, result)
		}
	})
}

// CreateEmbeddings batches requests with the same model/opts combination within the batch window.
func (b *Batch) CreateEmbeddings(ctx context.Context, modelWithProvider string, texts []string, opts map[string]any) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}

	provider, actualModel, err := b.parseModelWithProvider(modelWithProvider)
	if err != nil {
		return nil, err
	}
	embedder := b.embedders[provider]

	// A batch key includes the provider, actual model, and a fixed-size digest
	// of the serialized options, so different inputs create different batches
	// without embedding potentially large serialized options in the map key.
	key, err := newBatchKey(provider, actualModel, opts)
	if err != nil {
		return nil, err
	}
	optsSnapshot, err := snapshotOptions(opts)
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
			opts:          optsSnapshot,
		}
		bc.timer = time.AfterFunc(b.batchWindow, func() {
			b.processBatchWithRecovery(key, bc, embedder, actualModel)
		})
		return bc
	}

	// Add to batch
	b.mu.Lock()
	var currentBatch *batchedCalls
	for _, candidate := range b.m[key] {
		// JSON serialization intentionally provides a stable, fixed-size first-level
		// key. It is not sufficient for semantic equality because values with
		// different Go types can have the same JSON representation.
		if reflect.DeepEqual(candidate.opts, optsSnapshot) {
			currentBatch = candidate
			break
		}
	}
	if currentBatch == nil {
		currentBatch = createNewBatch()
		b.m[key] = append(b.m[key], currentBatch)
	} else {
		currentBatch.calls = append(currentBatch.calls, thisCall)
		currentBatch.batchedTextsN += len(texts)
	}
	// If the batch is full, seal this batch.
	// Sealing is done by simply dropping the reference to the current batch
	// so that no one else can modify it any more.
	if currentBatch.batchedTextsN >= b.maxBatchSize {
		b.removeBatchLocked(key, currentBatch)
		// currentBatch is now sealed, let's trigger it immediately without waiting for the timer.
		// For an AfterFunc timer, Stop returning false means its callback has already started.
		// Resetting in that case could schedule a second concurrent processBatch call.
		if currentBatch.timer.Stop() {
			currentBatch.timer.Reset(0)
		}
	}
	b.mu.Unlock()

	return waitForBatchResult(ctx, thisCall.resultCh)
}

// processBatch processes a batch of requests.
func (b *Batch) processBatch(key batchKey, thisBatch *batchedCalls, embedder base.Embedder, model string) {
	// Take out the current batch from the map to gain ownership. After this point,
	// no other goroutine can modify this batch.
	// Note that the current batch may be already taken out if there are too many requests.
	{
		b.mu.Lock()
		b.removeBatchLocked(key, thisBatch)
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

		embeddings := make([][]float32, 0, len(allTexts))
		for start := 0; start < len(allTexts); start += b.maxBatchSize {
			end := min(start+b.maxBatchSize, len(allTexts))
			chunkEmbeddings, err := embedder.CreateEmbeddings(reqCtx, model, allTexts[start:end], thisBatch.opts)
			if err != nil {
				return nil, err
			}
			if len(chunkEmbeddings) != end-start {
				if len(chunkEmbeddings) == 0 {
					return nil, fmt.Errorf("no embeddings returned for model %s", model)
				}
				return nil, fmt.Errorf("embedding provider returned %d embeddings for %d texts", len(chunkEmbeddings), end-start)
			}
			embeddings = append(embeddings, chunkEmbeddings...)
		}
		return embeddings, nil
	}()

	// Send results back to all requests

	if err != nil {
		for _, call := range activeCalls {
			// resultCh is buffered and receives exactly once, so delivery cannot
			// block even if the canceled caller has already returned.
			call.resultCh <- &batchResult{err: err}
		}
		return
	}
	for i, call := range activeCalls {
		startIdx := startIndices[i]
		endIdx := startIdx + len(call.texts)
		result := &batchResult{
			// Cap the outer slice so appending cannot overwrite the next caller's result.
			embeddings: embeddings[startIdx:endIdx:endIdx],
		}
		call.resultCh <- result
	}
}
