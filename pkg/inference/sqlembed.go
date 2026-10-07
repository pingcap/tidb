// Copyright 2026 PingCAP, Inc.
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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/mitchellh/copystructure"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Embedder is implemented by embedding providers.
type Embedder = base.Embedder

const (
	// EmbeddingCacheSize is the maximum number of entries retained in the
	// process-local embedding cache.
	EmbeddingCacheSize = 10000

	embedCancelCheckInterval = time.Second
	hostedKeyLogInterval     = time.Minute
)

const (
	errMissingAPI   = "%s API key is not configured, to configure the API key: SET @@GLOBAL.%s='<API_KEY>'"
	errUnauthorized = "%s returns status unauthorized, check your API key. To reconfigure a new API key: SET @@GLOBAL.%s='<API_KEY>'"
)

type embeddingCall struct {
	done      chan struct{}
	cancel    context.CancelFunc
	waiters   int
	completed bool

	embedding []float32
	err       error
}

// EmbedFn adapts embedding providers for SQL execution. It is owned by Domain
// so batching and cached results can be shared by sessions attached to that Domain.
type EmbedFn struct {
	embedder *batcher.Batch
	cache    *ristretto.Cache

	wg       util.WaitGroupWrapper
	mu       sync.Mutex
	inFlight map[string]*embeddingCall
	closed   bool
}

var hostedEmbeddingLogger = logutil.SampleLoggerFactory(hostedKeyLogInterval, 1)()

// NewEmbedFn creates an EmbedFn and registers all supported providers.
func NewEmbedFn() *EmbedFn {
	embedder := batcher.New()
	embedder.MustRegister("jina_ai", jina.NewJinaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedJinaAPIKey.Load,
		ErrMissingAPIKey: missingAPIKeyError("JinaAI", vardef.TiDBExpEmbedJinaAIAPIKey),
		ErrUnauthorized:  unauthorizedError("JinaAI", vardef.TiDBExpEmbedJinaAIAPIKey),
	}))
	embedder.MustRegister("openai", openai.NewOpenAIEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedOpenAIAPIKey.Load,
		GetBaseURL:       variable.GetOpenAIEmbeddingBaseURL,
		ErrMissingAPIKey: missingAPIKeyError("OpenAI", vardef.TiDBExpEmbedOpenAIAPIKey),
		ErrUnauthorized:  unauthorizedError("OpenAI", vardef.TiDBExpEmbedOpenAIAPIKey),
	}))
	embedder.MustRegister("cohere", cohere.NewCohereEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedCohereAPIKey.Load,
		ErrMissingAPIKey: missingAPIKeyError("Cohere", vardef.TiDBExpEmbedCohereAPIKey),
		ErrUnauthorized:  unauthorizedError("Cohere", vardef.TiDBExpEmbedCohereAPIKey),
	}))
	embedder.MustRegister("huggingface", huggingface.NewHuggingFaceEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedHuggingFaceAPIKey.Load,
		ErrMissingAPIKey: missingAPIKeyError("HuggingFace", vardef.TiDBExpEmbedHuggingFaceAPIKey),
		ErrUnauthorized:  unauthorizedError("HuggingFace", vardef.TiDBExpEmbedHuggingFaceAPIKey),
	}))
	embedder.MustRegister("nvidia_nim", nvidia.NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedNvidiaNIMAPIKey.Load,
		ErrMissingAPIKey: missingAPIKeyError("NVIDIA NIM", vardef.TiDBExpEmbedNvidiaNIMAPIKey),
		ErrUnauthorized:  unauthorizedError("NVIDIA NIM", vardef.TiDBExpEmbedNvidiaNIMAPIKey),
	}))
	embedder.MustRegister("gemini", gemini.NewGeminiEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        vardef.EmbedGeminiAPIKey.Load,
		ErrMissingAPIKey: missingAPIKeyError("Gemini", vardef.TiDBExpEmbedGeminiAPIKey),
		// Gemini's response body provides the useful authentication error, so no
		// custom unauthorized text is needed here.
	}))
	if isHostedEmbeddingEnabled() {
		embedder.MustRegister("tidbcloud_free", tidbcloud.NewTiDBCloudFreeEmbedder(tidbcloud.EmbedderConfig{
			GetBillingID: hostedEmbeddingBillingID,
			GetAPIKey:    getHostedEmbeddingAPIKey,
			GetBaseURL: func() string {
				return config.GetGlobalConfig().HostedEmbedding.APIEndpoint
			},
		}))
	}
	if intest.InTest {
		embedder.MustRegister("mock", mock.NewMockEmbedder())
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        EmbeddingCacheSize * 10,
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
		inFlight: make(map[string]*embeddingCall),
	}
}

func missingAPIKeyError(provider, variableName string) error {
	return fmt.Errorf(errMissingAPI, provider, strings.ToUpper(variableName))
}

func unauthorizedError(provider, variableName string) error {
	return fmt.Errorf(errUnauthorized, provider, strings.ToUpper(variableName))
}

// HasEmbedder returns whether a provider is registered.
func (e *EmbedFn) HasEmbedder(provider string) bool {
	return e.embedder.Has(provider)
}

// MustRegisterEmbedder registers an embedder for tests and panics on invalid input.
// It must be called before the EmbedFn starts serving requests.
func (e *EmbedFn) MustRegisterEmbedder(provider string, embedder Embedder) {
	e.embedder.MustRegister(provider, embedder)
}

func isHostedEmbeddingEnabled() bool {
	return kerneltype.IsNextGen() && deploymode.IsStarter() && config.GetGlobalConfig().HostedEmbedding.Enabled
}

func hostedEmbeddingBillingID() string {
	clusterID := config.GetGlobalConfig().AutoScalerClusterID
	if clusterID == "" {
		return ""
	}
	return "cluster_" + clusterID
}

func getHostedEmbeddingAPIKey() string {
	apiKeyPath := config.GetGlobalConfig().HostedEmbedding.APIKeyPath
	if apiKeyPath == "" {
		return ""
	}
	data, err := os.ReadFile(apiKeyPath)
	if err != nil {
		hostedEmbeddingLogger.Error(
			"failed to read API key file for hosted embedding service; request will be sent without the key",
			zap.String("api-key-path", apiKeyPath),
			zap.Error(err),
		)
		return ""
	}
	return strings.TrimSpace(string(data))
}

// Embed generates an embedding while adapting the SQL killer callback to a context.
func (e *EmbedFn) Embed(shouldCancel func() bool, modelWithProvider, text string, opts map[string]any) ([]float32, error) {
	return e.EmbedWithContext(context.Background(), shouldCancel, modelWithProvider, text, opts)
}

// EmbedWithContext generates an embedding with Domain-scoped batching and caching.
// Equal concurrent requests share one provider call, while each caller retains
// independent cancellation. The provider request is canceled only after all
// callers waiting on that shared request have canceled.
func (e *EmbedFn) EmbedWithContext(
	ctx context.Context,
	shouldCancel func() bool,
	modelWithProvider string,
	text string,
	opts map[string]any,
) ([]float32, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cleanup := contextWithCancelCheck(ctx, shouldCancel)
	defer cleanup()
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}

	if opts == nil {
		opts = map[string]any{}
	}
	optsSnapshot, err := snapshotOptions(opts)
	if err != nil {
		return nil, err
	}
	optsJSON, err := json.Marshal(optsSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize opts: %w", err)
	}
	cacheKey := makeCacheKey(
		modelWithProvider,
		text,
		optsSnapshot,
		optsJSON,
		vardef.EmbeddingConfigVersion.Load(),
	)
	call, cached, cacheHit, err := e.acquireCall(ctx, cacheKey, modelWithProvider, text, optsSnapshot)
	if err != nil {
		return nil, err
	}
	if cacheHit {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return cached, nil
	}
	select {
	case <-call.done:
		// Caller cancellation wins if completion and cancellation become visible
		// at the same time, preserving the caller's cancellation cause.
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if call.err != nil {
			return nil, call.err
		}
		return cloneEmbedding(call.embedding), nil
	case <-ctx.Done():
		e.releaseCall(cacheKey, call)
		return nil, context.Cause(ctx)
	}
}

func contextWithCancelCheck(parent context.Context, shouldCancel func() bool) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	if shouldCancel == nil {
		return ctx, cancel
	}
	if shouldCancel() {
		cancel()
		return ctx, cancel
	}
	var watcher util.WaitGroupWrapper
	watcher.RunWithRecover(func() {
		ticker := time.NewTicker(embedCancelCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if shouldCancel() {
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}, func(r any) {
		if r != nil {
			cancel()
		}
	})
	return ctx, func() {
		cancel()
		watcher.Wait()
	}
}

func (e *EmbedFn) acquireCall(
	ctx context.Context,
	key string,
	modelWithProvider string,
	text string,
	opts map[string]any,
) (*embeddingCall, []float32, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, nil, false, fmt.Errorf("embedding function is closed")
	}
	if cached, ok := e.cache.Get(key); ok {
		if embedding, ok := cached.([]float32); ok {
			return nil, cloneEmbedding(embedding), true, nil
		}
	}
	if call := e.inFlight[key]; call != nil {
		call.waiters++
		return call, nil, false, nil
	}

	// The shared request has its own cancellation lifecycle, but retaining the
	// first caller's context values keeps tracing and other request metadata.
	reqCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	call := &embeddingCall{
		done:    make(chan struct{}),
		cancel:  cancel,
		waiters: 1,
	}
	e.inFlight[key] = call
	e.wg.RunWithRecover(func() {
		e.runCall(reqCtx, key, call, modelWithProvider, text, opts)
	}, func(r any) {
		if r != nil {
			e.completeCall(key, call, nil, fmt.Errorf("embedding request panicked: %v", r))
		}
	})
	return call, nil, false, nil
}

func (e *EmbedFn) runCall(
	ctx context.Context,
	key string,
	call *embeddingCall,
	modelWithProvider string,
	text string,
	opts map[string]any,
) {
	embeddings, err := e.embedder.CreateEmbeddings(ctx, modelWithProvider, []string{text}, opts)
	var embedding []float32
	if err == nil {
		if len(embeddings) == 0 {
			err = fmt.Errorf("embedding provider returned no result for model %q", modelWithProvider)
		} else if validationErr := types.CheckVectorDimValid(len(embeddings[0])); validationErr != nil {
			// Reject values that TiDB cannot represent before they can consume
			// space in the process-local cache.
			err = validationErr
		} else {
			embedding = cloneEmbedding(embeddings[0])
		}
	}

	e.completeCall(key, call, embedding, err)
}

func (e *EmbedFn) completeCall(key string, call *embeddingCall, embedding []float32, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if call.completed {
		return
	}
	if err == nil && !e.closed && call.waiters > 0 && e.cache.Set(key, cloneEmbedding(embedding), 1) {
		e.cache.Wait()
	}
	call.embedding = embedding
	call.err = err
	if e.inFlight[key] == call {
		delete(e.inFlight, key)
	}
	call.completed = true
	close(call.done)
	call.cancel()
}

func (e *EmbedFn) releaseCall(key string, call *embeddingCall) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if call.waiters > 0 {
		call.waiters--
	}
	if call.waiters != 0 {
		return
	}
	if e.inFlight[key] == call {
		delete(e.inFlight, key)
	}
	call.cancel()
}

func makeCacheKey(modelWithProvider, text string, opts map[string]any, optsJSON []byte, configVersion uint64) string {
	hash := sha256.New()
	writeKeyPart(hash, []byte(modelWithProvider))
	writeKeyPart(hash, []byte(text))
	writeKeyPart(hash, optsJSON)
	writeKeyPart(hash, optionTypeSig(opts))
	var versionBytes [8]byte
	binary.LittleEndian.PutUint64(versionBytes[:], configVersion)
	_, _ = hash.Write(versionBytes[:])
	return string(hash.Sum(nil))
}

func writeKeyPart(writer interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func optionTypeSig(opts map[string]any) []byte {
	var signature bytes.Buffer
	appendOptionType(&signature, reflect.ValueOf(opts))
	return signature.Bytes()
}

func appendOptionType(signature *bytes.Buffer, value reflect.Value) {
	if !value.IsValid() {
		writeKeyPart(signature, nil)
		return
	}
	typeName := value.Type().PkgPath() + "/" + value.Type().String()
	writeKeyPart(signature, []byte(typeName))

	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			writeKeyPart(signature, nil)
			return
		}
		appendOptionType(signature, value.Elem())
	case reflect.Map:
		if value.IsNil() {
			writeKeyPart(signature, nil)
			return
		}
		type mapEntry struct {
			keySig []byte
			value  reflect.Value
		}
		entries := make([]mapEntry, 0, value.Len())
		for _, key := range value.MapKeys() {
			var keySig bytes.Buffer
			appendOptionType(&keySig, key)
			writeKeyPart(&keySig, []byte(fmt.Sprintf("%#v", key.Interface())))
			entries = append(entries, mapEntry{keySig: keySig.Bytes(), value: value.MapIndex(key)})
		}
		sort.Slice(entries, func(i, j int) bool {
			return bytes.Compare(entries[i].keySig, entries[j].keySig) < 0
		})
		for _, entry := range entries {
			writeKeyPart(signature, entry.keySig)
			appendOptionType(signature, entry.value)
		}
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			writeKeyPart(signature, nil)
			return
		}
		for i := range value.Len() {
			appendOptionType(signature, value.Index(i))
		}
	case reflect.Struct:
		for i := range value.NumField() {
			writeKeyPart(signature, []byte(value.Type().Field(i).Name))
			appendOptionType(signature, value.Field(i))
		}
	}
}

func snapshotOptions(opts map[string]any) (map[string]any, error) {
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

func cloneEmbedding(embedding []float32) []float32 {
	return append([]float32(nil), embedding...)
}

// Close releases resources and cancels in-flight provider requests.
func (e *EmbedFn) Close() {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closed = true
	for key, call := range e.inFlight {
		delete(e.inFlight, key)
		call.cancel()
	}
	e.mu.Unlock()
	e.wg.Wait()
	e.cache.Close()
}

// NewMockEmbedder creates the deterministic test embedder used by SQL tests.
func NewMockEmbedder() *mock.Embedder {
	return mock.NewMockEmbedder()
}

var (
	defaultEmbedFnMu sync.Mutex
	defaultEmbedFn   *EmbedFn
)

// DefaultEmbedFn returns the process-wide fallback used by tests without a Domain.
func DefaultEmbedFn() *EmbedFn {
	defaultEmbedFnMu.Lock()
	defer defaultEmbedFnMu.Unlock()
	if defaultEmbedFn == nil {
		defaultEmbedFn = NewEmbedFn()
	}
	return defaultEmbedFn
}

// SetDefaultEmbedFnForTest replaces the process-wide fallback and returns a cleanup callback.
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
