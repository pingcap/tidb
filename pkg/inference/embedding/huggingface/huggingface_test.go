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

	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify URL path contains the model
		assert.True(t, strings.Contains(r.URL.Path, "/models/intfloat/multilingual-e5-large/pipeline/feature-extraction"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"inputs": ["hello world", "goodbye world"]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
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
	serverURL := testutil.NewJSONServer(t, http.StatusUnauthorized, `{"error":"Invalid credentials in Authorization header"}`)

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "intfloat/multilingual-e5-large", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestHuggingFaceEmbedder_InvalidModel(t *testing.T) {
	serverURL := testutil.NewJSONServer(t, http.StatusNotFound, "")

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "abc/def", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model 'abc/def' does not exist")
}

func TestHuggingFaceEmbedder_ErrorWithMessage(t *testing.T) {
	serverURL := testutil.NewJSONServer(t, http.StatusServiceUnavailable, `{"error":"Model is currently loading"}`)

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return serverURL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "some/model", []string{"hello world"}, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.EqualError(t, err, "HuggingFace: status code 503, message: Model is currently loading")
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
	serverURL := testutil.NewJSONServer(t, http.StatusUnauthorized, "")

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:       func() string { return "invalid-key" },
		GetBaseURL:      func() string { return serverURL },
		ErrUnauthorized: customErr,
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}

func TestHuggingFaceEmbedder_MismatchedResponseLength(t *testing.T) {
	mockResponse := `[
		[0.1, 0.2, 0.3]
	]`
	serverURL := testutil.NewJSONServer(t, http.StatusOK, mockResponse)

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return serverURL },
	})

	texts := []string{"hello", "world"} // 2 texts but response has only 1 embedding
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "response data length 1 does not match input texts length 2")
}

func TestHuggingFaceEmbedder_WithOptions(t *testing.T) {
	requestCh := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		requestCh <- body
		_, _ = w.Write([]byte(`[[1.0]]`))
	}))
	defer server.Close()

	embedder := NewHuggingFaceEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "intfloat/multilingual-e5-large", []string{"test"}, map[string]any{
		"normalize": true,
		"inputs":    []string{"must-not-override"},
	})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1.0}}, embeddings)
	require.JSONEq(t, `{"inputs":["test"],"normalize":true}`, string(<-requestCh))
}

func TestHuggingFaceEmbedderEndpoint(t *testing.T) {
	endpoint, err := featureExtractionEndpoint(
		" https://example.com/base/?tenant=x ",
		"org/model?revision=1",
	)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/base/models/org/model%3Frevision=1/pipeline/feature-extraction?tenant=x", endpoint)
	endpoint, err = featureExtractionEndpoint("https://example.com", "org/../model")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/models/org/%2E%2E/model/pipeline/feature-extraction", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/inference"} {
		_, err := featureExtractionEndpoint(baseURL, "org/model")
		require.ErrorContains(t, err, "invalid HuggingFace API base URL")
	}
}

func TestHuggingFaceEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "org/model",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewHuggingFaceEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "HuggingFace request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
		RedactionResponse:         `{"error":"invalid api key: provider-secret"}`,
		RedactionError:            "HuggingFace: status code 400, message: invalid api key: [REDACTED]",
	})
}
