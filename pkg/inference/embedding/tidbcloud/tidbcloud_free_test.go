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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type contextBlockingReader struct {
	ctx         context.Context
	readStarted chan<- struct{}
}

func (r *contextBlockingReader) Read([]byte) (int, error) {
	close(r.readStarted)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func TestTiDBCloudFreeEmbedder_Success(t *testing.T) {
	mockResponse := `{
		"embeddings": [
			"` + testutil.EncodeFloat32Base64(1, 2) + `",
			"` + testutil.EncodeFloat32Base64(3, 4) + `"
		]
	}`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "/api/v1/inference/embeddings/default_billing_id", r.URL.Path)

		// Verify request body
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "amazon/titan-embed-text-v2",
			"texts": ["abc", "def"]
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"abc", "def"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "amazon/titan-embed-text-v2", texts, nil)

	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
}

func TestTiDBCloudFreeEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body includes additional options
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, `{
			"model": "cohere/embed-english-v3",
			"texts": ["test"],
			"input_type": "search_document"
		}`, string(body))

		mockResponse := `{
			"embeddings": ["AAAAAAAAAAA="]
		}`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetBaseURL: func() string { return server.URL },
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

	t.Run("response body", func(t *testing.T) {
		cause := errors.New("response read canceled by caller")
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		readStarted := make(chan struct{})
		embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
			GetBaseURL: func() string { return "http://127.0.0.1" },
		})
		embedder.client.Transport = testutil.RoundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:     "200 OK",
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body: io.NopCloser(&contextBlockingReader{
					ctx:         req.Context(),
					readStarted: readStarted,
				}),
				Request: req,
			}, nil
		})
		errCh := make(chan error, 1)
		go func() {
			_, err := embedder.CreateEmbeddings(ctx, "test-model", []string{"test"}, nil)
			errCh <- err
		}()

		select {
		case <-readStarted:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "provider did not start reading the response body")
		}
		cancel(cause)

		select {
		case err := <-errCh:
			require.ErrorIs(t, err, cause)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "provider did not return after context cancellation")
		}
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
		RequestError:              "failed to request TiDB Cloud Inference Service",
		ResponseBodyLimitError:    "failed to read from TiDB Cloud Inference Service",
		TransportCauseIsPreserved: false,
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
