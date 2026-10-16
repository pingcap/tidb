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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTiDBCloudFreeEmbedder_Success(t *testing.T) {
	mockResponse := `{
		"embeddings": [
			"` + testutil.EncodeFloat32Base64(1, 2) + `",
			"` + testutil.EncodeFloat32Base64(3, 4) + `"
		]
	}`

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return "http://unused.example" },
	})
	embedder.client.Transport = testutil.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		// Verify request method and headers
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Empty(t, r.Header.Values("Authorization"))
		assert.Equal(t, "/api/v1/inference/embeddings/default_billing_id", r.URL.Path)

		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "amazon/titan-embed-text-v2",
			"texts": ["abc", "def"]
		}`, string(body))

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(mockResponse)),
			Request:    r,
		}, nil
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "amazon/titan-embed-text-v2", texts, nil)

	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
}

func TestTiDBCloudFreeEmbedder_WithOptions(t *testing.T) {
	const mockResponse = `{
		"embeddings": ["AAAAAAAAAAA="]
	}`
	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://unused.example" },
	})
	embedder.client.Transport = testutil.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		assert.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))
		// Verify request body includes additional options
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "cohere/embed-english-v3",
			"texts": ["test"],
			"input_type": "search_document"
		}`, string(body))

		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(mockResponse)),
			Request:    r,
		}, nil
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "cohere/embed-english-v3", []string{"test"}, map[string]any{
		"input_type": "search_document",
		"model":      "must-not-override",
		"texts":      []string{"must-not-override"},
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.NotEmpty(t, embeddings[0])
}

func TestTiDBCloudFreeEmbedderErrors(t *testing.T) {
	tests := []struct {
		name        string
		statusCode  int
		response    string
		model       string
		errContains string
	}{
		{
			name:        "unknown model",
			statusCode:  http.StatusBadRequest,
			response:    `{"error":"Unknown model 'abc'"}`,
			model:       "abc",
			errContains: "Unknown model 'abc'",
		},
		{
			name:        "malformed request",
			statusCode:  http.StatusBadRequest,
			response:    `{"error":"Malformed input request: #: required key [input_type] not found#: required key [images] not found#/texts: false schema always fails, please reformat your input and try again."}`,
			model:       "cohere/embed-english-v3",
			errContains: "required key [input_type] not found",
		},
		{
			name:        "quota exceeded",
			statusCode:  http.StatusForbidden,
			response:    `{"error":"Exceeded quota limit. Current usage: $0.0000, Limit: $0.0000"}`,
			model:       "amazon/titan-embed-text-v2",
			errContains: "Exceeded quota limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, tt.statusCode, tt.response)
			embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
				GetBaseURL: func() string { return serverURL },
			})
			embeddings, err := embedder.CreateEmbeddings(context.Background(), tt.model, []string{"abc", "def"}, nil)
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.errContains)
		})
	}
}

func TestTiDBCloudFreeEmbedderEndpoint(t *testing.T) {
	endpoint, err := embeddingsEndpoint(
		" https://example.com/root/?tenant=x ",
		"billing/id?revision=1",
	)
	require.NoError(t, err)
	require.Equal(t, "https://example.com/root/api/v1/inference/embeddings/billing%2Fid%3Frevision=1?tenant=x", endpoint)
	endpoint, err = embeddingsEndpoint("https://example.com", "..")
	require.NoError(t, err)
	require.Equal(t, "https://example.com/api/v1/inference/embeddings/%2E%2E", endpoint)

	for _, baseURL := range []string{"://invalid", "/relative", "ftp://example.com/inference"} {
		_, err := embeddingsEndpoint(baseURL, "billing-id")
		require.ErrorContains(t, err, "invalid TiDB Cloud Inference base URL")
	}
}

func TestTiDBCloudFreeEmbedderPreservesContextCause(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()

		embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
			GetBaseURL: func() string { return "http://127.0.0.1" },
		})
		_, err := embedder.CreateEmbeddings(ctx, "test-model", []string{"test"}, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestTiDBCloudFreeEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "test-model",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "TiDB Cloud Inference request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
		RedactionResponse:         `{"error":"invalid api key: provider-secret"}`,
		RedactionError:            "TiDB Cloud Inference: status code 400, message: invalid api key: [REDACTED]",
	})
}

func TestTiDBCloudFreeEmbedderResponseValidation(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		texts       []string
		errContains string
	}{
		{
			name:        "mismatched length",
			response:    `{"embeddings":["AACAPw=="]}`,
			texts:       []string{"a", "b"},
			errContains: "response embeddings length 1 does not match input texts length 2",
		},
		{
			name:        "invalid decoded embedding length",
			response:    `{"embeddings":["AAEC"]}`,
			texts:       []string{"a"},
			errContains: "invalid embedding data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, http.StatusOK, tt.response)

			embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
				GetBaseURL: func() string { return serverURL },
			})
			embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", tt.texts, nil)
			require.Nil(t, embeddings)
			require.ErrorContains(t, err, tt.errContains)
		})
	}
}

func TestTiDBCloudFreeEmbedderMissingBaseURL(t *testing.T) {
	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.ErrorContains(t, err, "base URL is not configured for TiDB Cloud Inference")
}
