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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type contextBlockingReader struct {
	ctx         context.Context
	readStarted chan<- struct{}
}

func (r *contextBlockingReader) Read([]byte) (int, error) {
	close(r.readStarted)
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

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

func TestTiDBCloudFreeEmbedder_UnknownModel(t *testing.T) {
	// Mock error response for unknown model
	mockResponse := `{"error":"Unknown model 'abc'"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(mockResponse))
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
		_, _ = w.Write([]byte(mockResponse))
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
		_, _ = w.Write([]byte(mockResponse))
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

func TestTiDBCloudFreeEmbedderResponseBodyLimit(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(strings.Repeat("x", 65)))
			}))
			defer server.Close()

			embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return "test-api-key" },
				GetBaseURL:           func() string { return server.URL },
				MaxResponseBodyBytes: 64,
			})
			_, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
			require.EqualError(t, err, "failed to read from TiDB Cloud Inference Service")
		})
	}
}

func TestTiDBCloudFreeEmbedderPreservesContextCause(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		cause := errors.New("request canceled by caller")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)

		embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
			GetBaseURL: func() string { return "http://127.0.0.1" },
		})
		_, err := embedder.CreateEmbeddings(ctx, "test-model", []string{"test"}, nil)
		require.ErrorIs(t, err, cause)
	})

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
		embedder.client.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
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

func TestTiDBCloudFreeEmbedderErrorRedaction(t *testing.T) {
	const apiKey = "provider-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid api key: provider-secret"}`))
	}))
	defer server.Close()

	embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return apiKey },
		GetBaseURL: func() string { return server.URL },
	})
	_, err := embedder.CreateEmbeddings(context.Background(), "test-model", []string{"test"}, nil)
	require.EqualError(t, err, "TiDB Cloud Inference: invalid api key: [REDACTED]")
	require.NotContains(t, err.Error(), apiKey)
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
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tt.response))
			}))
			defer server.Close()

			embedder := NewTiDBCloudFreeEmbedder(EmbedderConfig{
				GetBaseURL: func() string { return server.URL },
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
