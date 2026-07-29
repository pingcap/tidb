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

package nvidia

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNvidiaEmbedder_Success(t *testing.T) {
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
			"model": "baai/bge-m3",
			"input": ["hello world", "test text"],
			"encoding_format": "base64"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", texts, nil)

	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
}

func TestNvidiaEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "baai/bge-m3",
			"input": ["test"],
			"encoding_format": "base64",
			"embedding_type": "float",
			"input_type": "query"
		}`, string(body))

		mockResponse := `
    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "index": 0,
          "embedding": "oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="
        }
      ],
      "model": "baai/bge-m3",
      "usage": {
        "prompt_tokens": 7,
        "total_tokens": 7
      }
    }`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, map[string]any{
		"input_type":      "query",
		"embedding_type":  "float",
		"model":           "must-not-override",
		"input":           []string{"must-not-override"},
		"encoding_format": "float",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
	})
}

func TestNvidiaEmbedderEmbeddingType(t *testing.T) {
	for _, value := range []any{"int8", "uint8", "binary", 1} {
		embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
			GetAPIKey:  func() string { panic("request validation should happen before reading the API key") },
			GetBaseURL: func() string { panic("invalid options must not issue a request") },
		})
		embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, map[string]any{
			"embedding_type": value,
		})
		require.Nil(t, embeddings)
		require.EqualError(t, err, `NVIDIA NIM embedding_type must be "float"`)
	}
}

func TestNvidiaEmbedder_ResponseIndexValidation(t *testing.T) {
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
				`{"object":"list","model":"baai/bge-m3","data":`+tt.responseData+`}`)

			embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:  func() string { return "test-api-key" },
				GetBaseURL: func() string { return serverURL },
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"a", "b"}, nil)
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

func TestNvidiaEmbedder_UnauthorizedAPIKey(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, status, `{"detail":"Authorization failed"}`)

			embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:  func() string { return "invalid-api-key" },
				GetBaseURL: func() string { return serverURL },
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"hello world"}, nil)
			require.Nil(t, embeddings)
			require.EqualError(t, err, "NVIDIA NIM returns status "+strings.ToLower(http.StatusText(status))+", check API key")
		})
	}
}

func TestNvidiaEmbedder_InvalidModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("404 page not found"))
	}))
	t.Cleanup(server.Close)

	embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3xxxx", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model 'baai/bge-m3xxxx' does not exist")
}

func TestNvidiaEmbedder_BadRequest(t *testing.T) {
	mockResponse := `{
	    "object": "error",
	    "message": "The model expects an input_type from one of 'passage' or 'query' but none was provided.",
	    "type": "validation_error"
	}`

	serverURL := testutil.NewJSONServer(t, http.StatusBadRequest, mockResponse)

	embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "nvidia/embed-qa-4", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "input_type")
}

func TestNvidiaEmbedder_MissingAPIKey(t *testing.T) {
	customErr := fmt.Errorf("custom missing API key error")
	embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
		GetAPIKey:        func() string { return "" },
		GetBaseURL:       func() string { return "http://mock-url" },
		ErrMissingAPIKey: customErr,
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}

func TestNvidiaEmbedder_CustomUnauthorizedError(t *testing.T) {
	customErr := fmt.Errorf("custom unauthorized error")
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, status, `{"detail": "Authorization failed"}`)

			embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:       func() string { return "invalid-key" },
				GetBaseURL:      func() string { return serverURL },
				ErrUnauthorized: customErr,
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, nil)
			require.Nil(t, embeddings)
			require.Equal(t, customErr, err)
		})
	}
}

func TestNvidiaEmbedderEndpoint(t *testing.T) {
	endpoint, err := embeddingsEndpoint("  https://example.com/v1/embeddings?api-version=x  ")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/v1/embeddings?api-version=x", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/embeddings"} {
		_, err := embeddingsEndpoint(baseURL)
		require.ErrorContains(t, err, "invalid NVIDIA NIM API base URL")
	}
}

func TestNvidiaEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "baai/bge-m3",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewNvidiaEmbedder(base.APIKeyProviderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "NVIDIA NIM request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
		RedactionResponse:         `{"error":"invalid api key: provider-secret"}`,
		RedactionError:            "NVIDIA NIM: status code 400, message: invalid api key: [REDACTED]",
	})
}
