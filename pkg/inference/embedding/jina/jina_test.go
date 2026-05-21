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

	"github.com/stretchr/testify/require"
)

func TestJinaEmbedder_Success(t *testing.T) {
	// Mock successful response from real Jina API
	mockResponse := `{
		"model": "jina-embeddings-v3",
		"object": "list",
		"usage": {
			"total_tokens": 410,
			"prompt_tokens": 410
		},
		"data": [
			{
				"object": "embedding",
				"index": 0,
				"embedding": "39MmPZun+j7S4Gw+ZEDbvkeeKj5cVwa/96yDPjPxED6S+VW+3JGYPg=="
			},
			{
				"object": "embedding",
				"index": 1,
				"embedding": "bTC0vQlWEz+9nwo+JkCYvp5FSj7cU9q+l3O6PgBCTD7oCUe+LLyzPg=="
			},
			{
				"object": "embedding",
				"index": 2,
				"embedding": "BW9wvjO4Dz/Ot/E9k1XCvnOLSj1Tady+s3qZPth9yT37tUa+wSDEPg=="
			},
			{
				"object": "embedding",
				"index": 3,
				"embedding": "ejyDPk6/QD7fGMA9DdaAvnlv7j6C7R2/z6yXvUxqlT65eyq9tnW9Pg=="
			},
			{
				"object": "embedding",
				"index": 4,
				"embedding": "2ptJP3bYAj6CbKe9Ff71vY6eOj5imZ6+3GfEvqggUT6Urx8+2JwZuw=="
			}
		]
	}`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "jina-embeddings-v3",
			"input": ["hello world", "test text", "sample input", "more text", "last item"],
			"embedding_type": "base64"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text", "sample input", "more text", "last item"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 5)
	require.Equal(t, embeddings[0], []float32{
		0.0407294, 0.48955998, 0.23132637, -0.42822564, 0.1666194, -0.5247705, 0.257179, 0.1415451, -0.20895985, 0.29798782,
	})
	require.Equal(t, embeddings[1], []float32{
		-0.08798299, 0.57553154, 0.13537498, -0.2973644, 0.1975312, -0.42642105, 0.36416313, 0.19947052, -0.19437373, 0.351045,
	})
	require.Equal(t, embeddings[2], []float32{
		-0.2347985, 0.5614044, 0.11802636, -0.37955913, 0.049449395, -0.43049106, 0.29976425, 0.09838456, -0.19405358, 0.3830624,
	})
	require.Equal(t, embeddings[3], []float32{
		0.25632077, 0.18822977, 0.09379744, -0.25163308, 0.46569422, -0.61690533, -0.074060075, 0.2918266, -0.041621897, 0.3700387,
	})
	require.Equal(t, embeddings[4], []float32{
		0.78753436, 0.12777886, -0.08174993, -0.12011353, 0.18224546, -0.30976397, -0.38360488, 0.20422614, 0.15594321, -0.0023439433,
	})
}

func TestJinaEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
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
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", []string{"test"}, map[string]any{
		"task": "retrieval.passage",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.0407294, 0.48955998, 0.23132637, -0.42822564, 0.1666194, -0.5247705, 0.257179, 0.1415451, -0.20895985, 0.29798782,
	})
}

func TestJinaEmbedder_UnauthorizedAPIKey(t *testing.T) {
	// Mock unauthorized response from real Jina API
	mockResponse := `{"detail":"Unauthorized"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestJinaEmbedder_InvalidModel(t *testing.T) {
	// Mock model not found response from real Jina API
	mockResponse := `{"detail":"Model jina-embeddings-v2-small-enx not found"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v2-small-enx", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "JinaAI: Model jina-embeddings-v2-small-enx not found")
}

func TestJinaEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "jina-embeddings-v3", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestJinaEmbedder_NoModel(t *testing.T) {
	embedder := NewJinaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
}
