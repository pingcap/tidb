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

package cohere

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCohereEmbedder_Success(t *testing.T) {
	// Mock successful response from real Cohere API
	mockResponse := `{
		"response_type": "embeddings_floats",
		"embeddings": [
			[0.016296387, -0.008354187, 0.12345678, -0.98765432, 0.5],
			[0.04663086, -0.023239136, 0.87654321, -0.11111111, 0.3],
			[0.11111111, 0.22222222, 0.33333333, 0.44444444, 0.55555555]
		],
		"id": "1c62213a-1f15-46f1-ac62-36f6bbaf3972",
		"texts": ["hello world", "test text", "sample input"],
		"meta": {
			"api_version": {
				"version": "1"
			},
			"billed_units": {
				"input_tokens": 6
			}
		}
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
			"model": "embed-v4.0",
			"texts": ["hello world", "test text", "sample input"]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text", "sample input"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 3)
	require.Equal(t, embeddings[0], []float32{0.016296387, -0.008354187, 0.12345678, -0.98765432, 0.5})
	require.Equal(t, embeddings[1], []float32{0.04663086, -0.023239136, 0.87654321, -0.11111111, 0.3})
	require.Equal(t, embeddings[2], []float32{0.11111111, 0.22222222, 0.33333333, 0.44444444, 0.55555555})
}

func TestCohereEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "embed-v4.0",
			"texts": ["test"],
			"input_type": "classification"
		}`, string(body))

		mockResponse := `{
			"response_type": "embeddings_floats",
			"embeddings": [[0.1, 0.2, 0.3]],
			"id": "test-id",
			"texts": ["test"]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, map[string]any{
		"input_type": "classification",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{0.1, 0.2, 0.3})
}

func TestCohereEmbedder_NoAPIKey(t *testing.T) {
	// Mock no API key response from real Cohere API
	mockResponse := `{"id":"b6a8a658-261e-4fc2-b44d-0ee3fefa145e","message":"no api key supplied"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "API key is not configured for cohere")
}

func TestCohereEmbedder_InvalidAPIKey(t *testing.T) {
	// Mock invalid API key response from real Cohere API
	mockResponse := `{"id":"276562f4-bb27-49f9-a044-9145b05f0fd0","message":"invalid api token"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestCohereEmbedder_InvalidModel(t *testing.T) {
	// Mock model not found response from real Cohere API
	mockResponse := `{"id":"da62a855-b6e9-4c4e-b232-9ba335a9549a","message":"model 'embed-v4.0x' not found, make sure the correct model ID was used and that you have access to the model."}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0x", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "cohere: model 'embed-v4.0x' not found")
}

func TestCohereEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestCohereEmbedder_NoModel(t *testing.T) {
	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model name is required")
}
