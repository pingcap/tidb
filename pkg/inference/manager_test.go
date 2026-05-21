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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type staticEmbedder struct {
	embeddings [][]float32
	err        error
	calls      atomic.Int64
}

func (s *staticEmbedder) CreateEmbeddings(context.Context, string, []string, map[string]any) ([][]float32, error) {
	s.calls.Add(1)
	return s.embeddings, s.err
}

func TestEmbedFnProvidersAndMock(t *testing.T) {
	embedFn := NewEmbedFn()
	defer embedFn.Close()

	require.True(t, embedFn.HasEmbedder("openai"))
	require.True(t, embedFn.HasEmbedder(" OPENAI "))
	require.True(t, embedFn.HasEmbedder("jina_ai"))
	require.True(t, embedFn.HasEmbedder("cohere"))
	require.True(t, embedFn.HasEmbedder("huggingface"))
	require.True(t, embedFn.HasEmbedder("nvidia_nim"))
	require.True(t, embedFn.HasEmbedder("gemini"))
	require.False(t, embedFn.HasEmbedder("tidbcloud_free"))
	if !embedFn.HasEmbedder("mock") {
		embedFn.MustRegisterEmbedder("mock", NewMockEmbedder())
	}

	embedding, err := embedFn.Embed(nil, "mock/json", "[1,2,3]", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)

	embedding, err = embedFn.Embed(nil, " mock / json ", "[1,2,3]", map[string]any{"plus": float64(2)})
	require.NoError(t, err)
	require.Equal(t, []float32{3, 4, 5}, embedding)
}

func TestEmbedFnCacheAndErrors(t *testing.T) {
	embedFn := NewEmbedFn()
	defer embedFn.Close()

	static := &staticEmbedder{embeddings: [][]float32{{1, 2, 3}}}
	embedFn.MustRegisterEmbedder("static", static)

	embedding, err := embedFn.Embed(nil, "static/model", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)
	embedding, err = embedFn.Embed(nil, "static/model", "hello", nil)
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, embedding)
	require.Equal(t, int64(1), static.calls.Load())

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
}

func TestSetDefaultEmbedFnForTest(t *testing.T) {
	original := DefaultEmbedFn()
	replacement := NewEmbedFn()
	restore := SetDefaultEmbedFnForTest(replacement)
	require.Same(t, replacement, DefaultEmbedFn())
	restore()
	require.Same(t, original, DefaultEmbedFn())
}
