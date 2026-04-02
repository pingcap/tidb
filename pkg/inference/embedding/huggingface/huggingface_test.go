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

package huggingface

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHuggingFaceEmbedder_Success(t *testing.T) {
	// Mock successful response from real HuggingFace API
	mockResponse := `[
		[0.022240305319428444, -0.004567116964608431, 0.15847662091255188, -0.08124932646751404],
		[-0.013980913907289505, -0.058682069182395935, 0.23456789012345678, 0.98765432109876543]
	]`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify URL path contains the model
		require.True(t, strings.Contains(r.URL.Path, "/models/intfloat/multilingual-e5-large/pipeline/feature-extraction"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"inputs": ["hello world", "goodbye world"]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "goodbye world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "intfloat/multilingual-e5-large", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 2)
	require.Equal(t, embeddings[0], []float32{
		0.022240305319428444, -0.004567116964608431, 0.15847662091255188, -0.08124932646751404,
	})
	require.Equal(t, embeddings[1], []float32{
		-0.013980913907289505, -0.058682069182395935, 0.23456789012345678, 0.98765432109876543,
	})
}

func TestHuggingFaceEmbedder_UnauthorizedAPIKey(t *testing.T) {
	// Mock unauthorized response from real HuggingFace API
	mockResponse := `{"error":"Invalid credentials in Authorization header"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "intfloat/multilingual-e5-large", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestHuggingFaceEmbedder_InvalidModel(t *testing.T) {
	// Mock model not found response from real HuggingFace API (404 with no body)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		// No body for 404 responses from HuggingFace
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "abc/def", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model 'abc/def' does not exist")
}

func TestHuggingFaceEmbedder_ErrorWithMessage(t *testing.T) {
	// Mock error response with message
	mockResponse := `{"error":"Model is currently loading"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "some/model", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "HuggingFace: Model is currently loading")
}

func TestHuggingFaceEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "intfloat/multilingual-e5-large", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestHuggingFaceEmbedder_NoModel(t *testing.T) {
	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model name is required")
}

func TestHuggingFaceEmbedder_NoAPIKey(t *testing.T) {
	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey: func() string { return "" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "API key is not configured for HuggingFace")
}

func TestHuggingFaceEmbedder_CustomErrorHandling(t *testing.T) {
	customErr := fmt.Errorf("custom missing API key error")
	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:        func() string { return "" },
		ErrMissingAPIKey: customErr,
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}

func TestHuggingFaceEmbedder_CustomUnauthorizedError(t *testing.T) {
	customErr := fmt.Errorf("custom unauthorized error")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:       func() string { return "invalid-key" },
		GetBaseURL:      func() string { return server.URL },
		ErrUnauthorized: customErr,
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}

func TestHuggingFaceEmbedder_MismatchedResponseLength(t *testing.T) {
	// Mock response with different length than input
	mockResponse := `[
		[0.1, 0.2, 0.3]
	]`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello", "world"} // 2 texts but response has only 1 embedding
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "response data length 1 does not match input texts length 2")
}
