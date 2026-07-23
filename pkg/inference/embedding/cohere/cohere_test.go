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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

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
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "embed-v4.0",
			"texts": ["hello world", "test text", "sample input"],
			"input_type": "search_document"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text", "sample input"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", texts, map[string]any{
		"input_type": "search_document",
	})

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
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "embed-v4.0",
			"texts": ["test"],
			"input_type": "classification",
			"embedding_types": ["float"]
		}`, string(body))

		mockResponse := `{
			"response_type": "embeddings_by_type",
			"embeddings": {"float": [[0.1, 0.2, 0.3]]},
			"id": "test-id",
			"texts": ["test"]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, map[string]any{
		"input_type":      "classification",
		"embedding_types": []string{"float"},
		"model":           "must-not-override",
		"texts":           []string{"must-not-override"},
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{0.1, 0.2, 0.3})
}

func TestCohereEmbedderEmbeddingTypes(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "non-float", value: []string{"int8"}},
		{name: "multiple", value: []string{"float", "int8"}},
		{name: "not an array", value: "float"},
		{name: "non-string element", value: []any{"float", 8}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			embedder := NewCohereEmbedder(EmbedderConfig{
				GetAPIKey:  func() string { panic("request validation should happen before reading the API key") },
				GetBaseURL: func() string { panic("invalid options must not issue a request") },
			})
			embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, map[string]any{
				"embedding_types": tt.value,
			})
			require.Nil(t, embeddings)
			require.EqualError(t, err, `Cohere embedding_types must be exactly ["float"]`)
		})
	}

	_, err := decodeEmbeddings([]byte(`{"int8":[[1,2,3]]}`))
	require.EqualError(t, err, "Cohere response does not contain float embeddings")
}

func TestCohereEmbedder_NoAPIKey(t *testing.T) {
	// Mock no API key response from real Cohere API
	mockResponse := `{"id":"b6a8a658-261e-4fc2-b44d-0ee3fefa145e","message":"no api key supplied"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(mockResponse))
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
		_, _ = w.Write([]byte(mockResponse))
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
		_, _ = w.Write([]byte(mockResponse))
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

func TestCohereEmbedderEndpoint(t *testing.T) {
	endpoint, err := embeddingsEndpoint("  https://example.com/v1/embed?api-version=x  ")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/v1/embed?api-version=x", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/embed"} {
		_, err := embeddingsEndpoint(baseURL)
		require.ErrorContains(t, err, "invalid Cohere API base URL")
	}
}

func TestCohereEmbedderResponseBodyLimit(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(strings.Repeat("x", 65)))
			}))
			defer server.Close()

			embedder := NewCohereEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return "test-api-key" },
				GetBaseURL:           func() string { return server.URL },
				MaxResponseBodyBytes: 64,
			})
			_, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, nil)
			require.ErrorContains(t, err, "response body exceeds maximum size of 64 bytes")
		})
	}
}

func TestCohereEmbedderErrorRedaction(t *testing.T) {
	const apiKey = "provider-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"message":"invalid api key: provider-secret"}`))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return apiKey },
		GetBaseURL: func() string { return server.URL },
	})
	_, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, nil)
	require.EqualError(t, err, "cohere: invalid api key: [REDACTED]")
	require.NotContains(t, err.Error(), apiKey)
}

func TestCohereEmbedderMismatchedResponseLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"embeddings":[[1.0]]}`))
	}))
	defer server.Close()

	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"a", "b"}, nil)
	require.Nil(t, embeddings)
	require.ErrorContains(t, err, "response embeddings length 1 does not match input texts length 2")
}

func TestCohereEmbedderTransportErrorRedaction(t *testing.T) {
	const secret = "super-secret"
	embedder := NewCohereEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "https://internal.example/v1/embed?token=" + secret },
	})
	embedder.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, assert.AnError
	})

	_, err := embedder.CreateEmbeddings(context.Background(), "embed-v4.0", []string{"test"}, nil)
	require.EqualError(t, err, "Cohere request failed")
	require.NotContains(t, err.Error(), secret)
	require.ErrorIs(t, err, assert.AnError)
}
