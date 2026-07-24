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

package jina

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJinaEmbedder_Success(t *testing.T) {
	mockResponse := `{
		"data": [
			{"index": 0, "embedding": "` + testutil.EncodeFloat32Base64(1, 2) + `"},
			{"index": 1, "embedding": "` + testutil.EncodeFloat32Base64(3, 4) + `"}
		]
	}`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "jina-embeddings-v3",
			"input": ["hello world", "test text"],
			"embedding_type": "base64"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", texts, nil)

	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
}

func TestJinaEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "jina-embeddings-v3",
			"input": ["test"],
			"embedding_type": "base64",
			"task": "retrieval.passage"
		}`, string(body))

		mockResponse := `{
			"model": "jina-embeddings-v3",
			"object": "list",
			"data": [{
				"object": "embedding",
				"index": 0,
				"embedding": "39MmPZun+j7S4Gw+ZEDbvkeeKj5cVwa/96yDPjPxED6S+VW+3JGYPg=="
			}]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", []string{"test"}, map[string]any{
		"task":           "retrieval.passage",
		"model":          "must-not-override",
		"input":          []string{"must-not-override"},
		"embedding_type": "float",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.0407294, 0.48955998, 0.23132637, -0.42822564, 0.1666194, -0.5247705, 0.257179, 0.1415451, -0.20895985, 0.29798782,
	})
}

func TestJinaEmbedder_ResponseIndexValidation(t *testing.T) {
	firstEmbedding := testutil.EncodeFloat32Base64(1, 2)
	secondEmbedding := testutil.EncodeFloat32Base64(3, 4)
	tests := []struct {
		name         string
		responseData string
		errContains  string
	}{
		{
			name: "out of order",
			responseData: `[
				{"object":"embedding","index":1,"embedding":"` + secondEmbedding + `"},
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"}
			]`,
		},
		{
			name: "mismatched length",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"}
			]`,
			errContains: "response data length 1 does not match input texts length 2",
		},
		{
			name: "duplicate index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"},
				{"object":"embedding","index":0,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "duplicate index 0",
		},
		{
			name: "out of range index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"},
				{"object":"embedding","index":2,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "out of range",
		},
		{
			name: "invalid decoded embedding length",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"AAEC"},
				{"object":"embedding","index":1,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "invalid embedding data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, http.StatusOK,
				`{"object":"list","model":"jina-embeddings-v3","data":`+tt.responseData+`}`)

			embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:  func() string { return "test-api-key" },
				GetBaseURL: func() string { return serverURL },
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", []string{"a", "b"}, nil)
			if tt.errContains != "" {
				require.Nil(t, embeddings)
				require.ErrorContains(t, err, tt.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
		})
	}
}

func TestJinaEmbedder_UnauthorizedAPIKey(t *testing.T) {
	serverURL := testutil.NewJSONServer(t, http.StatusUnauthorized, `{"detail":"Unauthorized"}`)

	embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestJinaEmbedder_InvalidModel(t *testing.T) {
	serverURL := testutil.NewJSONServer(t, http.StatusNotFound, `{"detail":"Model jina-embeddings-v2-small-enx not found"}`)

	embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v2-small-enx", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.EqualError(t, err, "JinaAI: status code 404, message: Model jina-embeddings-v2-small-enx not found")
}

func TestJinaEmbedderEndpoint(t *testing.T) {
	endpoint, err := embeddingsEndpoint("  https://example.com/v1/embeddings?api-version=x  ")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/v1/embeddings?api-version=x", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/embeddings"} {
		_, err := embeddingsEndpoint(baseURL)
		require.ErrorContains(t, err, "invalid Jina AI API base URL")
	}
}

func TestJinaEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "jina-embeddings-v3",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewJinaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "JinaAI request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
		RedactionResponse:         `{"detail":"invalid api key: provider-secret"}`,
		RedactionError:            "JinaAI: status code 400, message: invalid api key: [REDACTED]",
	})
}
