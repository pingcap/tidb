// Copyright 2025 PingCAP, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/inference/embedding/batcher"
	"github.com/stretchr/testify/require"
)

type blockingEmbedder struct {
	mu      sync.Mutex
	calls   []string
	entered chan struct{}
	release chan struct{}
}

var _ base.Embedder = (*blockingEmbedder)(nil)

func (e *blockingEmbedder) CreateEmbeddings(_ context.Context, _ string, _ []string, opts map[string]any) ([][]float32, error) {
	task := opts["task"].(string)

	e.mu.Lock()
	e.calls = append(e.calls, task)
	if len(e.calls) == 1 {
		close(e.entered)
	}
	e.mu.Unlock()

	<-e.release

	if task == "first" {
		return [][]float32{{1}}, nil
	}
	return [][]float32{{2}}, nil
}

func (e *blockingEmbedder) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}

func TestEmbedSingleflightKeyIncludesOpts(t *testing.T) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters:        100,
		MaxCost:            100,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer cache.Close()

	embedder := &blockingEmbedder{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	b := batcher.NewBatchEmbedderWithConfig(time.Millisecond, 1)
	b.RegisterEmbedder("test", embedder)

	fn := &EmbedFn{
		embedder: b,
		cache:    cache,
	}

	var wg sync.WaitGroup
	results := make([][]float32, 2)
	errs := make([]error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		results[0], errs[0] = fn.Embed(func() bool { return false }, "test/model", "same-text", map[string]any{"task": "first"})
	}()

	<-embedder.entered

	wg.Add(1)
	go func() {
		defer wg.Done()
		results[1], errs[1] = fn.Embed(func() bool { return false }, "test/model", "same-text", map[string]any{"task": "second"})
	}()

	time.Sleep(20 * time.Millisecond)
	close(embedder.release)
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, 2, embedder.callCount())
	require.Equal(t, []float32{1}, results[0])
	require.Equal(t, []float32{2}, results[1])
}
