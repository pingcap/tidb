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

package tidbcloud

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateEmbeddings_EmptyTexts(t *testing.T) {
	cfg := EmbedderConfig{}
	embedder := NewTiDBCloudFreeEmbedder(cfg)

	ctx := context.Background()
	embeddings, err := embedder.CreateEmbeddings(ctx, "test-model", []string{}, nil)

	require.NoError(t, err)
	assert.Empty(t, embeddings)
}

func TestCreateEmbeddings_EmptyModel(t *testing.T) {
	cfg := EmbedderConfig{}
	embedder := NewTiDBCloudFreeEmbedder(cfg)

	ctx := context.Background()
	_, err := embedder.CreateEmbeddings(ctx, "", []string{"test"}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "model name is required")
}

func TestTiDBCloudFreeEmbedder_Success(t *testing.T) {
	mockResponse := `{
		"embeddings": [
			"39MmPZun+j7S4Gw+ZEDbvkeeKj5cVwa/96yDPjPxED6S+VW+3JGYPg==",
			"bTC0vQlWEz+9nwo+JkCYvp5FSj7cU9q+l3O6PgBCTD7oCUe+LLyzPg=="
		]
	}`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "amazon/titan-embed-text-v2",
			"texts": ["abc", "def"]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "amazon/titan-embed-text-v2", texts, nil)

	require.NoError(t, err)
	require.Len(t, embeddings, 2)
	require.Equal(t, embeddings[0], []float32{
		0.0407294, 0.48955998, 0.23132637, -0.42822564, 0.1666194, -0.5247705, 0.257179, 0.1415451, -0.20895985, 0.29798782,
	})
	require.Equal(t, embeddings[1], []float32{
		-0.08798299, 0.57553154, 0.13537498, -0.2973644, 0.1975312, -0.42642105, 0.36416313, 0.19947052, -0.19437373, 0.351045,
	})
}

func TestTiDBCloudFreeEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body includes additional options
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "cohere/embed-english-v3",
			"texts": ["test"],
			"input_type": "search_document"
		}`, string(body))

		mockResponse := `{
			"embeddings": ["AAAAAAAAAAA="]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "cohere/embed-english-v3", []string{"test"}, map[string]any{
		"input_type": "search_document",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.NotEmpty(t, embeddings[0])
}

func TestTiDBCloudFreeEmbedder_UnknownModel(t *testing.T) {
	// Mock error response for unknown model
	mockResponse := `{"error":"Unknown model 'abc'"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "abc", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "Unknown model 'abc'")
}

func TestTiDBCloudFreeEmbedder_MalformedRequest(t *testing.T) {
	// Mock error response for malformed request (missing required parameters)
	mockResponse := `{"error":"Malformed input request: #: required key [input_type] not found#: required key [images] not found#/texts: false schema always fails, please reformat your input and try again."}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "cohere/embed-english-v3", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "required key [input_type] not found")
}

func TestTiDBCloudFreeEmbedder_QuotaExceeded(t *testing.T) {
	// Mock error response for quota exceeded
	mockResponse := `{"error":"Exceeded quota limit. Current usage: $0.0000, Limit: $0.0000"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "amazon/titan-embed-text-v2", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "Exceeded quota limit")
}
