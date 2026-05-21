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

package gemini

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeminiEmbedder_Success(t *testing.T) {
	// Mock successful response from Gemini API
	mockResponse := `{
		"embeddings": [
			{
				"values": [
					-0.010632273,
					0.019375853,
					0.020965198,
					0.0007706437,
					-0.061464068
				]
			},
			{
				"values": [
					0.018468002,
					0.0054281265,
					-0.017658807,
					0.013859263,
					0.05341865
				]
			},
			{
				"values": [
					0.058089074,
					0.020941732,
					-0.10872878,
					-0.04039259,
					0.12345678
				]
			}
		]
	}`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Equal(t, "test-api-key", r.Header.Get("x-goog-api-key"))

		// Verify URL path
		require.Equal(t, "/text-embedding-004:batchEmbedContents", r.URL.Path)

		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"requests": [
				{
					"model": "models/text-embedding-004",
					"content": {
						"parts": [{"text": "hello world"}]
					}
				},
				{
					"model": "models/text-embedding-004",
					"content": {
						"parts": [{"text": "test text"}]
					}
				},
				{
					"model": "models/text-embedding-004",
					"content": {
						"parts": [{"text": "sample input"}]
					}
				}
			]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text", "sample input"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-004", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 3)
	require.Equal(t, embeddings[0], []float32{
		-0.010632273, 0.019375853, 0.020965198, 0.0007706437, -0.061464068,
	})
	require.Equal(t, embeddings[1], []float32{
		0.018468002, 0.0054281265, -0.017658807, 0.013859263, 0.05341865,
	})
	require.Equal(t, embeddings[2], []float32{
		0.058089074, 0.020941732, -0.10872878, -0.04039259, 0.12345678,
	})
}

func TestGeminiEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body includes options
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"requests": [
				{
					"model": "models/text-embedding-004",
					"content": {
						"parts": [{"text": "test"}]
					},
					"outputDimensionality": 10
				}
			]
		}`, string(body))

		mockResponse := `{
			"embeddings": [
				{
					"values": [
						-0.010632273,
						0.019375853,
						0.020965198,
						0.0007706437,
						-0.061464068,
						0.123456,
						0.789012,
						0.345678,
						0.901234,
						0.567890
					]
				}
			]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-004", []string{"test"}, map[string]any{
		"outputDimensionality": 10,
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Len(t, embeddings[0], 10)
	require.Equal(t, embeddings[0], []float32{
		-0.010632273, 0.019375853, 0.020965198, 0.0007706437, -0.061464068,
		0.123456, 0.789012, 0.345678, 0.901234, 0.567890,
	})
}

func TestGeminiEmbedder_InvalidAPIKey(t *testing.T) {
	mockResponse := `{
		"error": {
			"code": 400,
			"message": "API key not valid. Please pass a valid API key.",
			"status": "INVALID_ARGUMENT"
		}
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-004", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "API key not valid")
}

func TestGeminiEmbedder_InvalidModel(t *testing.T) {
	mockResponse := `{
		"error": {
			"code": 404,
			"message": "models/gemini-embedding-exp-03-09 is not found for API version v1beta, or is not supported for embedContent. Call ListModels to see the list of available models and their supported methods.",
			"status": "NOT_FOUND"
		}
	}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "gemini-embedding-exp-03-09", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "not found for API version v1beta")
}

func TestGeminiEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-004", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestGeminiEmbedder_NoModel(t *testing.T) {
	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model name is required")
}

func TestGeminiEmbedder_MissingAPIKey(t *testing.T) {
	embedder := NewGeminiEmbedder(EmbedderConfig{
		GetAPIKey:        func() string { return "" },
		GetBaseURL:       func() string { return "http://mock-url" },
		ErrMissingAPIKey: fmt.Errorf("custom missing API key error"),
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-004", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "custom missing API key error")
}
