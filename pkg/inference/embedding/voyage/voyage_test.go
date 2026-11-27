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

package voyage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/require"
)

func newTestEmbedder(baseURL string) *Embedder {
	return NewVoyageEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return baseURL },
	})
}

// newEchoServer answers every request with one embedding per input, whose
// first component is the decimal value of the input, and records each request body.
func newEchoServer(t *testing.T, requests *[]map[string]any) string {
	t.Helper()
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var request map[string]any
		require.NoError(t, json.Unmarshal(body, &request))
		request["path"] = r.URL.Path
		mu.Lock()
		*requests = append(*requests, request)
		mu.Unlock()

		// Text models send "input"; contextualized models send "inputs", where a
		// document is a string and a query is a one-element list.
		contextualized := request["inputs"] != nil
		var inputs []any
		if contextualized {
			inputs = request["inputs"].([]any)
		} else {
			inputs = request["input"].([]any)
		}
		// The embedding has the requested dimension, or two components.
		dimension := 2
		if requested, ok := request["output_dimension"].(float64); ok {
			dimension = int(requested)
		}
		data := make([]map[string]any, len(inputs))
		// Return the items in reverse order to prove that the index is honored.
		for i, input := range inputs {
			vector := make([]float32, dimension)
			text, isDocument := input.(string)
			if !isDocument {
				text = input.([]any)[0].(string)
			}
			vector[0] = float32(len(text))
			embedding := map[string]any{
				"object":    "embedding",
				"index":     i,
				"embedding": testutil.EncodeFloat32Base64(vector...),
			}
			if contextualized {
				embedding["index"] = 0
				embedding = map[string]any{"object": "list", "index": i, "data": []any{embedding}}
			}
			data[len(inputs)-1-i] = embedding
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "model": request["model"]}))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func TestVoyageEmbedderRequest(t *testing.T) {
	var requests []map[string]any
	embedder := newTestEmbedder(newEchoServer(t, &requests))

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-4-lite", []string{"a", "bbb"}, map[string]any{
		"input_type":       "query",
		"output_dimension": float64(256), // JSON numbers decode as float64.
		"truncation":       false,
	})
	require.NoError(t, err)
	require.Len(t, embeddings, 2)
	require.Len(t, embeddings[0], 256)
	require.Equal(t, []float32{1, 3}, []float32{embeddings[0][0], embeddings[1][0]})
	require.Len(t, requests, 1)
	require.Equal(t, map[string]any{
		"path":             "/embeddings",
		"model":            "voyage-4-lite",
		"input":            []any{"a", "bbb"},
		"encoding_format":  "base64",
		"input_type":       "query",
		"output_dimension": float64(256),
		"truncation":       false,
	}, requests[0])
}

func TestVoyageEmbedderOmitsUnrequestedOptions(t *testing.T) {
	var requests []map[string]any
	embedder := newTestEmbedder(newEchoServer(t, &requests))

	_, err := embedder.CreateEmbeddings(context.Background(), "voyage-law-2", []string{"a"}, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"path":            "/embeddings",
		"model":           "voyage-law-2",
		"input":           []any{"a"},
		"encoding_format": "base64",
	}, requests[0])
}

func TestVoyageEmbedderRejectsInvalidInput(t *testing.T) {
	embedder := NewVoyageEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { panic("validation must happen before reading the API key") },
		GetBaseURL: func() string { panic("invalid input must not issue a request") },
	})

	tests := []struct {
		name  string
		model string
		opts  map[string]any
		err   string
	}{
		{name: "missing model", model: "", err: "model name is required"},
		{name: "unknown model", model: "voyage-nope", err: `Voyage AI model "voyage-nope" is not supported`},
		{name: "model of another provider", model: "text-embedding-3-small", err: "is not supported"},
		{name: "model that is not in the explicit list", model: "voyage-3", err: "is not supported"},
		{name: "input type of another provider", model: "voyage-4", opts: map[string]any{"input_type": "search_document"}, err: `input_type must be "document" or "query"`},
		{name: "input type is not a string", model: "voyage-4", opts: map[string]any{"input_type": 1}, err: `input_type must be "document" or "query"`},
		{name: "dimension not offered by the model", model: "voyage-4", opts: map[string]any{"output_dimension": float64(300)}, err: "output_dimension must be one of [256 512 1024 2048] for model voyage-4"},
		{name: "dimension of a fixed-size model", model: "voyage-finance-2", opts: map[string]any{"output_dimension": float64(512)}, err: "output_dimension must be one of [1024] for model voyage-finance-2"},
		{name: "fractional dimension", model: "voyage-4", opts: map[string]any{"output_dimension": 256.5}, err: "output_dimension must be one of"},
		{name: "dimension is not a number", model: "voyage-4", opts: map[string]any{"output_dimension": "256"}, err: "output_dimension must be one of"},
		{name: "truncation is not a boolean", model: "voyage-4", opts: map[string]any{"truncation": "true"}, err: "truncation must be a boolean"},
		{name: "adapter-owned option", model: "voyage-4", opts: map[string]any{"encoding_format": "float"}, err: `option "encoding_format" is not supported`},
		{name: "unknown option", model: "voyage-4", opts: map[string]any{"enable_auto_chunking": true}, err: `option "enable_auto_chunking" is not supported`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embeddings, err := embedder.CreateEmbeddings(context.Background(), tt.model, []string{"text"}, tt.opts)
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestVoyageEmbedderNoAPIKey(t *testing.T) {
	embedder := NewVoyageEmbedder(base.APIKeyProviderConfig{GetAPIKey: func() string { return "" }})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-4", []string{"hello"}, nil)
	require.Nil(t, embeddings)
	require.ErrorContains(t, err, "API key is not configured for Voyage AI")
}

func TestVoyageEmbedderAPIErrors(t *testing.T) {
	t.Run("unauthorized", func(t *testing.T) {
		url := testutil.NewJSONServer(t, http.StatusUnauthorized, `{"detail":"Provided API key is invalid."}`)
		_, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-4", []string{"hello"}, nil)
		require.ErrorContains(t, err, "check API key")
	})
	t.Run("detail string", func(t *testing.T) {
		url := testutil.NewJSONServer(t, http.StatusBadRequest, `{"detail":"Request to model 'voyage-4' failed. The max allowed tokens per submitted batch is 320000."}`)
		_, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-4", []string{"hello"}, nil)
		require.EqualError(t, err, "Voyage AI: status code 400, message: Request to model 'voyage-4' failed. The max allowed tokens per submitted batch is 320000.")
	})
	t.Run("structured detail", func(t *testing.T) {
		url := testutil.NewJSONServer(t, http.StatusUnprocessableEntity, `{"detail":[{"loc":["body","input"],"msg":"field required"}]}`)
		_, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-4", []string{"hello"}, nil)
		require.EqualError(t, err, `Voyage AI: status code 422, message: [{"loc":["body","input"],"msg":"field required"}]`)
	})
	t.Run("non-JSON body", func(t *testing.T) {
		url := testutil.NewJSONServer(t, http.StatusBadGateway, `<html>bad gateway</html>`)
		_, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-4", []string{"hello"}, nil)
		require.EqualError(t, err, "Voyage AI: status code 502, message: Bad Gateway")
	})
}

func TestVoyageEmbedderRejectsMalformedResponses(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		texts    []string
		opts     map[string]any
		contains string
	}{
		{name: "too few embeddings", body: `{"data":[{"index":0,"embedding":"AACAPw=="}]}`, texts: []string{"a", "b"}, contains: "response data length 1 does not match input texts length 2"},
		{name: "duplicate index", body: `{"data":[{"index":0,"embedding":"AACAPw=="},{"index":0,"embedding":"AACAPw=="}]}`, texts: []string{"a", "b"}, contains: "duplicate index 0"},
		{name: "index out of range", body: `{"data":[{"index":5,"embedding":"AACAPw=="}]}`, texts: []string{"a"}, contains: "out of range"},
		{name: "float array instead of base64", body: `{"data":[{"index":0,"embedding":[1.0]}]}`, texts: []string{"a"}, contains: "unexpected unmarshal response error"},
		{name: "wrong dimension", body: `{"data":[{"index":0,"embedding":"AACAPw=="}]}`, texts: []string{"a"}, opts: map[string]any{"output_dimension": 256}, contains: "dimension 1, expected 256"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := testutil.NewJSONServer(t, http.StatusOK, tt.body)
			embeddings, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-4", tt.texts, tt.opts)
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.contains)
		})
	}
}

func TestSplitRequests(t *testing.T) {
	text := func(n int) string { return strings.Repeat("x", n) }

	require.Empty(t, splitRequests(nil, 100))

	t.Run("fits one request", func(t *testing.T) {
		texts := []string{text(40), text(50)}
		require.Equal(t, [][]string{texts}, splitRequests(texts, 100))
	})
	t.Run("splits at the token ceiling and keeps order", func(t *testing.T) {
		texts := []string{text(60), text(50), text(40), text(60), text(10)}
		require.Equal(t, [][]string{
			{texts[0]},
			{texts[1], texts[2]},
			{texts[3], texts[4]},
		}, splitRequests(texts, 100))
	})
	t.Run("a text above the ceiling is sent alone", func(t *testing.T) {
		texts := []string{text(10), text(500), text(10)}
		require.Equal(t, [][]string{{texts[0]}, {texts[1]}, {texts[2]}}, splitRequests(texts, 100))
	})
	t.Run("invalid UTF-8 is counted at its encoded size", func(t *testing.T) {
		invalid := strings.Repeat("\xff", 40) // 120 bytes once JSON-encoded as U+FFFD.
		texts := []string{invalid, "a"}
		require.Equal(t, [][]string{{invalid}, {"a"}}, splitRequests(texts, 100))
	})
	t.Run("no documented ceiling only limits the number of inputs", func(t *testing.T) {
		texts := make([]string, maxInputsPerRequest+1)
		for i := range texts {
			texts[i] = text(1000)
		}
		requests := splitRequests(texts, 0)
		require.Len(t, requests, 2)
		require.Len(t, requests[0], maxInputsPerRequest)
		require.Len(t, requests[1], 1)
	})
	t.Run("input count limit with a token ceiling", func(t *testing.T) {
		texts := make([]string, 2*maxInputsPerRequest)
		for i := range texts {
			texts[i] = "x"
		}
		requests := splitRequests(texts, 1_000_000)
		require.Len(t, requests, 2)
		require.Len(t, requests[0], maxInputsPerRequest)
	})
}

func TestVoyageEmbedderSplitsLargeInputAcrossRequests(t *testing.T) {
	var requests []map[string]any
	embedder := newTestEmbedder(newEchoServer(t, &requests))

	// voyage-4-large accepts at most 120K tokens per request. Each text is
	// bounded by its byte length, so three 50K texts need two requests.
	texts := []string{strings.Repeat("a", 50_000), strings.Repeat("b", 50_001), strings.Repeat("c", 50_002)}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-4-large", texts, nil)
	require.NoError(t, err)
	require.Len(t, requests, 2)
	require.Len(t, requests[0]["input"], 2)
	require.Len(t, requests[1]["input"], 1)
	require.Equal(t, [][]float32{{50_000, 0}, {50_001, 0}, {50_002, 0}}, embeddings)
}

func TestVoyageEmbedderStopsAtFirstFailedRequest(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"too long"}`))
	}))
	t.Cleanup(server.Close)

	texts := []string{strings.Repeat("a", 100_000), strings.Repeat("b", 100_000)}
	embeddings, err := newTestEmbedder(server.URL).CreateEmbeddings(context.Background(), "voyage-4-large", texts, nil)
	require.Nil(t, embeddings)
	require.ErrorContains(t, err, "too long")
	require.Equal(t, 1, calls)
}

func TestVoyageEmbedderEndpoint(t *testing.T) {
	endpoint, err := endpointURL("", embeddingsPath)
	require.NoError(t, err)
	require.Equal(t, "https://api.voyageai.com/v1/embeddings", endpoint)
	endpoint, err = endpointURL("  http://127.0.0.1:8080/proxy/v1/?x=1 ", contextualizedPath)
	require.NoError(t, err)
	require.Equal(t, "http://127.0.0.1:8080/proxy/v1/contextualizedembeddings?x=1", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/embed"} {
		_, err := endpointURL(baseURL, embeddingsPath)
		require.ErrorContains(t, err, "invalid Voyage AI API base URL")
	}
}

func TestVoyageEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "voyage-4",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewVoyageEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "Voyage AI request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
		RedactionResponse:         `{"detail":"invalid api key: provider-secret"}`,
		RedactionError:            "Voyage AI: status code 400, message: invalid api key: [REDACTED]",
	})
}

func TestVoyageContextualizedRequest(t *testing.T) {
	var requests []map[string]any
	embedder := newTestEmbedder(newEchoServer(t, &requests))

	t.Run("document mode auto-chunks a flat list of documents", func(t *testing.T) {
		requests = nil
		embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-context-3", []string{"a", "bbb"}, map[string]any{
			"input_type":       "document",
			"output_dimension": float64(256),
		})
		require.NoError(t, err)
		require.Len(t, embeddings, 2)
		require.Equal(t, []float32{1, 3}, []float32{embeddings[0][0], embeddings[1][0]})
		require.Equal(t, []map[string]any{{
			"path":                 "/contextualizedembeddings",
			"model":                "voyage-context-3",
			"inputs":               []any{"a", "bbb"},
			"input_type":           "document",
			"output_dimension":     float64(256),
			"enable_auto_chunking": true,
			"chunk_size":           float64(32000),
			"encoding_format":      "base64",
		}}, requests)
	})

	t.Run("query mode sends one single-element list per query without chunking", func(t *testing.T) {
		requests = nil
		embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-context-4", []string{"a", "bbb"}, map[string]any{"input_type": "query"})
		require.NoError(t, err)
		require.Len(t, embeddings, 2)
		require.Equal(t, []float32{1, 3}, []float32{embeddings[0][0], embeddings[1][0]})
		require.Equal(t, []map[string]any{{
			"path":            "/contextualizedembeddings",
			"model":           "voyage-context-4",
			"inputs":          []any{[]any{"a"}, []any{"bbb"}},
			"input_type":      "query",
			"encoding_format": "base64",
		}}, requests)
	})

	t.Run("text models use the embeddings endpoint", func(t *testing.T) {
		requests = nil
		_, err := embedder.CreateEmbeddings(context.Background(), "voyage-4", []string{"a"}, nil)
		require.NoError(t, err)
		require.Equal(t, "/embeddings", requests[0]["path"])
	})
}

func TestVoyageContextualizedRejectsInvalidInput(t *testing.T) {
	embedder := NewVoyageEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { panic("validation must happen before reading the API key") },
		GetBaseURL: func() string { panic("invalid input must not issue a request") },
	})
	tests := []struct {
		name string
		opts map[string]any
		err  string
	}{
		{name: "missing input type", opts: nil, err: `requires option input_type ("document" or "query")`},
		{name: "missing input type with other options", opts: map[string]any{"output_dimension": 256}, err: `requires option input_type`},
		{name: "input type of another provider", opts: map[string]any{"input_type": "search_query"}, err: `input_type must be "document" or "query"`},
		{name: "truncation", opts: map[string]any{"input_type": "document", "truncation": true}, err: "truncation is not supported for contextualized model voyage-context-3"},
		{name: "chunking options are owned by the adapter", opts: map[string]any{"input_type": "document", "enable_auto_chunking": false}, err: `option "enable_auto_chunking" is not supported`},
		{name: "chunk size is owned by the adapter", opts: map[string]any{"input_type": "document", "chunk_size": 100}, err: `option "chunk_size" is not supported, supported options: input_type, output_dimension`},
		{name: "dimension", opts: map[string]any{"input_type": "document", "output_dimension": 300}, err: "output_dimension must be one of [256 512 1024 2048] for model voyage-context-3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-context-3", []string{"text"}, tt.opts)
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestVoyageContextualizedRejectsMalformedResponses(t *testing.T) {
	const vector = `"embedding":"AACAPw=="`
	tests := []struct {
		name     string
		body     string
		texts    []string
		contains string
	}{
		{name: "document split into several chunks", body: `{"data":[{"index":0,"data":[{"index":0,` + vector + `},{"index":1,` + vector + `}]}]}`, texts: []string{"a"}, contains: "input text 0 was split into 2 chunks"},
		{name: "document without chunks", body: `{"data":[{"index":0,"data":[]}]}`, texts: []string{"a"}, contains: "input text 0 was split into 0 chunks"},
		{name: "too few documents", body: `{"data":[{"index":0,"data":[{"index":0,` + vector + `}]}]}`, texts: []string{"a", "b"}, contains: "response data length 1 does not match input texts length 2"},
		{name: "duplicate document index", body: `{"data":[{"index":0,"data":[{"index":0,` + vector + `}]},{"index":0,"data":[{"index":0,` + vector + `}]}]}`, texts: []string{"a", "b"}, contains: "duplicate index 0"},
		{name: "document index out of range", body: `{"data":[{"index":3,"data":[{"index":0,` + vector + `}]}]}`, texts: []string{"a"}, contains: "out of range"},
		{name: "invalid embedding", body: `{"data":[{"index":0,"data":[{"index":0,"embedding":"AAA="}]}]}`, texts: []string{"a"}, contains: "failed to decode embedding for input text 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := testutil.NewJSONServer(t, http.StatusOK, tt.body)
			embeddings, err := newTestEmbedder(url).CreateEmbeddings(context.Background(), "voyage-context-3", tt.texts, map[string]any{"input_type": "document"})
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.contains)
		})
	}
}

func TestVoyageContextualizedSplitsLargeInputAcrossRequests(t *testing.T) {
	var requests []map[string]any
	embedder := newTestEmbedder(newEchoServer(t, &requests))

	// Contextualized requests are limited to 120K tokens in total.
	texts := []string{strings.Repeat("a", 50_000), strings.Repeat("b", 50_001), strings.Repeat("c", 50_002)}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "voyage-context-3", texts, map[string]any{"input_type": "document"})
	require.NoError(t, err)
	require.Len(t, requests, 2)
	require.Len(t, requests[0]["inputs"], 2)
	require.Len(t, requests[1]["inputs"], 1)
	require.Equal(t, []float32{50_000, 50_001, 50_002}, []float32{embeddings[0][0], embeddings[1][0], embeddings[2][0]})
}
