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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

type staticEmbedder struct {
	embeddings [][]float32
	err        error
}

func (s staticEmbedder) CreateEmbeddings(context.Context, string, []string, map[string]any) ([][]float32, error) {
	return s.embeddings, s.err
}

func TestRegistryEmbed(t *testing.T) {
	t.Run("default registry", func(t *testing.T) {
		_, ok := DefaultRegistry().GetEmbedder("openai")
		require.True(t, ok)
	})

	t.Run("mock", func(t *testing.T) {
		r := NewRegistry()
		r.MustRegisterEmbedder("mock", NewMockEmbedder())
		r.MustRegisterEmbedder(" MockCase ", NewMockEmbedder())

		embedding, err := r.Embed(nil, "mock/json", "[1,2,3]", nil)
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2, 3}, embedding)

		embedding, err = r.Embed(nil, "mock/json", "[1,2,3]", map[string]any{"plus": float64(2)})
		require.NoError(t, err)
		require.Equal(t, []float32{3, 4, 5}, embedding)

		embedding, err = r.Embed(nil, " MockCase / json ", "[4,5]", nil)
		require.NoError(t, err)
		require.Equal(t, []float32{4, 5}, embedding)
	})

	t.Run("openai", func(t *testing.T) {
		var req struct {
			Input          []string `json:"input"`
			Model          string   `json:"model"`
			EncodingFormat string   `json:"encoding_format"`
			Dimensions     int64    `json:"dimensions"`
			User           string   `json:"user"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/embeddings", r.URL.Path)
			require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"model":  req.Model,
				"data": []map[string]any{
					{
						"object":    "embedding",
						"index":     0,
						"embedding": []float64{1, 2, 3},
					},
				},
				"usage": map[string]any{
					"prompt_tokens": 1,
					"total_tokens":  1,
				},
			}))
		}))
		defer server.Close()

		t.Setenv(openAIAPIKeyEnv, "test-key")
		t.Setenv(openAIBaseURLEnv, server.URL+"/v1/embeddings/")

		r := NewRegistry()
		embedding, err := r.Embed(func() bool { return false }, "openai/text-embedding-3-small", "hello", map[string]any{
			"dimensions": float64(3),
			"user":       "test-user",
		})
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2, 3}, embedding)
		require.Equal(t, []string{"hello"}, req.Input)
		require.Equal(t, "text-embedding-3-small", req.Model)
		require.Equal(t, "float", req.EncodingFormat)
		require.Equal(t, int64(3), req.Dimensions)
		require.Equal(t, "test-user", req.User)
	})

	t.Run("openai base from sysvar", func(t *testing.T) {
		originalBase := vardef.EmbedOpenAIAPIBase.Load()
		t.Cleanup(func() {
			vardef.EmbedOpenAIAPIBase.Store(originalBase)
		})

		var req struct {
			Input []string `json:"input"`
			Model string   `json:"model"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/embeddings", r.URL.Path)
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"model":  req.Model,
				"data": []map[string]any{
					{
						"object":    "embedding",
						"index":     0,
						"embedding": []float64{4, 5, 6},
					},
				},
				"usage": map[string]any{
					"prompt_tokens": 1,
					"total_tokens":  1,
				},
			}))
		}))
		defer server.Close()

		t.Setenv(openAIAPIKeyEnv, "test-key")
		t.Setenv(openAIBaseURLEnv, "http://127.0.0.1:1/should-not-be-used")
		vardef.EmbedOpenAIAPIBase.Store(server.URL + "/v1")

		r := NewRegistry()
		embedding, err := r.Embed(nil, "openai/text-embedding-3-small", "hello", nil)
		require.NoError(t, err)
		require.Equal(t, []float32{4, 5, 6}, embedding)
		require.Equal(t, []string{"hello"}, req.Input)
		require.Equal(t, "text-embedding-3-small", req.Model)
	})
}

func TestRegistryErrors(t *testing.T) {
	r := NewRegistry()

	require.ErrorContains(t, r.RegisterEmbedder(" ", NewMockEmbedder()), "embedding provider name cannot be empty")
	require.ErrorContains(t, r.RegisterEmbedder("nil", nil), `embedding provider "nil" is nil`)

	_, err := r.Embed(nil, "json", "[1,2,3]", nil)
	require.ErrorContains(t, err, "EMBED_TEXT expects model name")

	_, err = r.Embed(nil, "unknown/json", "[1,2,3]", nil)
	require.ErrorContains(t, err, `embedding provider "unknown" is not registered`)

	require.NoError(t, r.RegisterEmbedder("mock", NewMockEmbedder()))
	require.ErrorContains(t, r.RegisterEmbedder("mock", NewMockEmbedder()), `embedding provider "mock" is already registered`)
	require.Panics(t, func() {
		r.MustRegisterEmbedder("mock", NewMockEmbedder())
	})

	r.MustRegisterEmbedder("empty", staticEmbedder{})
	_, err = r.Embed(nil, "empty/model", "hello", nil)
	require.ErrorContains(t, err, `embedding provider "empty" returned no embeddings`)

	r.MustRegisterEmbedder("multi", staticEmbedder{embeddings: [][]float32{{1}, {2}}})
	_, err = r.Embed(nil, "multi/model", "hello", nil)
	require.ErrorContains(t, err, `embedding provider "multi" returned 2 embeddings for a single input`)

	r.MustRegisterEmbedder("fail", staticEmbedder{err: errors.New("embed failed")})
	_, err = r.Embed(nil, "fail/model", "hello", nil)
	require.ErrorContains(t, err, "embed failed")

	t.Run("openai missing api key", func(t *testing.T) {
		t.Setenv(openAIAPIKeyEnv, "")
		t.Setenv(openAIBaseURLEnv, "")

		_, err := r.Embed(nil, "openai/text-embedding-3-small", "hello", nil)
		require.ErrorContains(t, err, openAIAPIKeyEnv)
	})

	t.Run("openai invalid options", func(t *testing.T) {
		t.Setenv(openAIAPIKeyEnv, "test-key")

		_, err := r.Embed(nil, "openai/text-embedding-3-small", "hello", map[string]any{"unknown": 1})
		require.ErrorContains(t, err, "unknown option unknown")

		_, err = r.Embed(nil, "openai/text-embedding-3-small", "hello", map[string]any{"dimensions": 1.5})
		require.ErrorContains(t, err, "invalid type for 'dimensions' option")

		_, err = r.Embed(nil, "openai/text-embedding-3-small", "hello", map[string]any{"user": 42})
		require.ErrorContains(t, err, "invalid type for 'user' option")
	})

	t.Run("mock invalid inputs", func(t *testing.T) {
		m := NewMockEmbedder()
		_, err := m.CreateEmbeddings(context.Background(), "unknown", []string{"[1]"}, nil)
		require.ErrorContains(t, err, "unknown model unknown")

		_, err = m.CreateEmbeddings(context.Background(), "json", []string{"[1]"}, map[string]any{"unknown": true})
		require.ErrorContains(t, err, "unknown option unknown")

		_, err = m.CreateEmbeddings(context.Background(), "json", []string{"[1]"}, map[string]any{"plus": "1"})
		require.ErrorContains(t, err, "invalid type for 'plus' option")

		_, err = m.CreateEmbeddings(context.Background(), "json", []string{"[1]"}, map[string]any{"delay": 1})
		require.ErrorContains(t, err, "invalid type for 'delay' option")

		_, err = m.CreateEmbeddings(context.Background(), "json", []string{"[1]"}, map[string]any{"delay": "bad"})
		require.ErrorContains(t, err, "invalid delay duration")

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = m.CreateEmbeddings(ctx, "json", []string{"[1]"}, map[string]any{"delay": "1s"})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestOpenAIEmbedderResponseErrors(t *testing.T) {
	run := func(t *testing.T, texts []string, data []map[string]any, wantErr string) {
		setupOpenAIEmbeddingServer(t, func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/embeddings", r.URL.Path)
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"object": "list",
				"model":  "text-embedding-3-small",
				"data":   data,
				"usage": map[string]any{
					"prompt_tokens": 1,
					"total_tokens":  1,
				},
			}))
		})

		_, err := NewOpenAIEmbedder().CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)
		require.ErrorContains(t, err, wantErr)
	}

	t.Run("count mismatch", func(t *testing.T) {
		run(t, []string{"hello"}, []map[string]any{}, "openai provider returned 0 embeddings for 1 inputs")
	})

	t.Run("invalid index", func(t *testing.T) {
		run(t, []string{"hello"}, []map[string]any{
			{
				"object":    "embedding",
				"index":     2,
				"embedding": []float64{1},
			},
		}, "openai provider returned invalid embedding index 2")
	})

	t.Run("omitted index", func(t *testing.T) {
		run(t, []string{"hello", "world"}, []map[string]any{
			{
				"object":    "embedding",
				"index":     0,
				"embedding": []float64{1},
			},
			{
				"object":    "embedding",
				"index":     0,
				"embedding": []float64{2},
			},
		}, "openai provider omitted embedding for input index 1")
	})
}

func TestOpenAIEmbeddingParamsValidation(t *testing.T) {
	params, err := buildOpenAIEmbeddingParams("model", []string{"hello"}, map[string]any{
		"dimensions": int(3),
		"user":       " test-user ",
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), params.Dimensions.Value)
	require.Equal(t, "test-user", params.User.Value)

	params, err = buildOpenAIEmbeddingParams("model", []string{"hello"}, map[string]any{"dimensions": int64(4)})
	require.NoError(t, err)
	require.Equal(t, int64(4), params.Dimensions.Value)

	for _, opt := range []any{int(0), int64(-1), "3"} {
		_, err = buildOpenAIEmbeddingParams("model", []string{"hello"}, map[string]any{"dimensions": opt})
		require.ErrorContains(t, err, "invalid type for 'dimensions' option")
	}

	_, err = buildOpenAIEmbeddingParams("model", []string{"hello"}, map[string]any{"user": "   "})
	require.ErrorContains(t, err, "user option cannot be empty")
}

func TestResolveOpenAIBaseURL(t *testing.T) {
	originalBase := vardef.EmbedOpenAIAPIBase.Load()
	vardef.EmbedOpenAIAPIBase.Store("")
	t.Cleanup(func() {
		vardef.EmbedOpenAIAPIBase.Store(originalBase)
	})

	t.Setenv(openAIBaseURLEnv, "")
	baseURL, err := resolveOpenAIBaseURL()
	require.NoError(t, err)
	require.Equal(t, vardef.DefTiDBExpEmbedOpenAIAPIBase, baseURL)

	t.Setenv(openAIBaseURLEnv, "http://127.0.0.1:8080/v1/embeddings")
	baseURL, err = resolveOpenAIBaseURL()
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:8080/v1/", baseURL)

	t.Setenv(openAIBaseURLEnv, "127.0.0.1:8080/v1")
	_, err = resolveOpenAIBaseURL()
	require.ErrorContains(t, err, "invalid OPENAI_BASE_URL")

	t.Setenv(openAIBaseURLEnv, "http://[::1")
	_, err = resolveOpenAIBaseURL()
	require.ErrorContains(t, err, "invalid OPENAI_BASE_URL")
}

func setupOpenAIEmbeddingServer(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	originalBase := vardef.EmbedOpenAIAPIBase.Load()
	vardef.EmbedOpenAIAPIBase.Store("")
	t.Cleanup(func() {
		vardef.EmbedOpenAIAPIBase.Store(originalBase)
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Setenv(openAIAPIKeyEnv, "test-key")
	t.Setenv(openAIBaseURLEnv, server.URL+"/v1")
}
