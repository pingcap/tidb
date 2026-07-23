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

package openai

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/inference/embedding/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type capturedHTTPRequest struct {
	path          string
	method        string
	contentType   string
	authorization string
	body          []byte
	bodyErr       error
}

func captureHTTPRequest(r *http.Request) capturedHTTPRequest {
	body, err := io.ReadAll(r.Body)
	return capturedHTTPRequest{
		path:          r.URL.Path,
		method:        r.Method,
		contentType:   r.Header.Get("Content-Type"),
		authorization: r.Header.Get("Authorization"),
		body:          body,
		bodyErr:       err,
	}
}

func receiveRequestPath(t *testing.T, requestPathCh <-chan string) string {
	t.Helper()
	select {
	case path := <-requestPathCh:
		return path
	case <-time.After(time.Second):
		require.FailNow(t, "HTTP request was not captured")
		return ""
	}
}

func TestOpenAIEmbedder_Success(t *testing.T) {
	mockResponse := `{
		"data": [
			{"index": 0, "embedding": "` + testutil.EncodeFloat32Base64(1, 2) + `"},
			{"index": 1, "embedding": "` + testutil.EncodeFloat32Base64(3, 4) + `"}
		]
	}`

	requestCh := make(chan capturedHTTPRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- captureHTTPRequest(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)

	require.NoError(t, err)
	request := <-requestCh
	require.NoError(t, request.bodyErr)
	require.Equal(t, "/embeddings", request.path)
	require.Equal(t, "POST", request.method)
	require.Equal(t, "application/json", request.contentType)
	require.Equal(t, "Bearer test-api-key", request.authorization)
	require.JSONEq(t, `{
		"model": "text-embedding-3-small",
		"input": ["hello world", "test text"],
		"encoding_format": "base64"
	}`, string(request.body))
	require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
}

func TestOpenAIEmbedder_WithOptions(t *testing.T) {
	requestCh := make(chan capturedHTTPRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- captureHTTPRequest(r)
		mockResponse := `
    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "index": 0,
          "embedding": "oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="
        }
      ],
      "model": "text-embedding-3-small",
      "usage": {
        "prompt_tokens": 7,
        "total_tokens": 7
      }
    }`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, map[string]any{
		"my_opt": "abc",
	})

	require.NoError(t, err)
	request := <-requestCh
	require.NoError(t, request.bodyErr)
	require.Equal(t, "/embeddings", request.path)
	require.JSONEq(t, `{
		"model": "text-embedding-3-small",
		"input": ["test"],
		"encoding_format": "base64",
		"my_opt": "abc"
	}`, string(request.body))
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
	})
}

func TestOpenAIEmbedderBaseURL(t *testing.T) {
	type requestURL struct {
		path     string
		rawQuery string
	}
	requestURLCh := make(chan requestURL, 7)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURLCh <- requestURL{path: r.URL.Path, rawQuery: r.URL.RawQuery}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"data":[{"index":0,"embedding":"AACAPw=="}],
			"model":"text-embedding-3-small"
		}`))
	}))
	defer server.Close()

	testCases := []struct {
		suffix   string
		path     string
		rawQuery string
	}{
		{suffix: "", path: "/embeddings"},
		{suffix: "/", path: "/embeddings"},
		{suffix: "/embeddings", path: "/embeddings"},
		{suffix: "/embeddings/", path: "/embeddings"},
		{suffix: "/v1?api-version=x", path: "/v1/embeddings", rawQuery: "api-version=x"},
		{suffix: "/v1/embeddings?api-version=x", path: "/v1/embeddings", rawQuery: "api-version=x"},
		{suffix: "/v1/embeddings/?api-version=x#section", path: "/v1/embeddings", rawQuery: "api-version=x"},
	}
	for _, tt := range testCases {
		embedder := NewOpenAIEmbedder(EmbedderConfig{
			GetAPIKey:  func() string { return "test-api-key" },
			GetBaseURL: func() string { return server.URL + tt.suffix },
		})
		embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, nil)
		require.NoError(t, err)
		request := <-requestURLCh
		require.Equal(t, tt.path, request.path)
		require.Equal(t, tt.rawQuery, request.rawQuery)
		require.Equal(t, [][]float32{{1}}, embeddings)
	}

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "://invalid" },
	})
	_, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, nil)
	require.ErrorContains(t, err, "invalid OpenAI API base URL")
}

func TestOpenAIEmbedderHTTPClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})
	embedder.client.Timeout = 20 * time.Millisecond

	_, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, nil)
	require.Error(t, err)
	var netErr net.Error
	require.ErrorAs(t, err, &netErr)
	require.True(t, netErr.Timeout())
}

func TestOpenAIEmbedder_ResponseIndexValidation(t *testing.T) {
	firstEmbedding := testutil.EncodeFloat32Base64(1, 2)
	secondEmbedding := testutil.EncodeFloat32Base64(3, 4)
	tests := []struct {
		name         string
		responseData string
		errContains  string
	}{
		{
			name: "out of order",
			responseData: `[
				{"object":"embedding","index":1,"embedding":"` + secondEmbedding + `"},
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"}
			]`,
		},
		{
			name: "duplicate index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"},
				{"object":"embedding","index":0,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "duplicate index 0",
		},
		{
			name: "out of range index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"` + firstEmbedding + `"},
				{"object":"embedding","index":2,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "out of range",
		},
		{
			name: "invalid decoded embedding length",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"AAEC"},
				{"object":"embedding","index":1,"embedding":"` + secondEmbedding + `"}
			]`,
			errContains: "invalid embedding data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverURL := testutil.NewJSONServer(t, http.StatusOK,
				`{"object":"list","model":"text-embedding-3-small","data":`+tt.responseData+`}`)

			embedder := NewOpenAIEmbedder(EmbedderConfig{
				GetAPIKey:  func() string { return "test-api-key" },
				GetBaseURL: func() string { return serverURL },
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"a", "b"}, nil)
			if tt.errContains != "" {
				require.Nil(t, embeddings)
				require.ErrorContains(t, err, tt.errContains)
				return
			}
			require.NoError(t, err)
			require.Equal(t, [][]float32{{1, 2}, {3, 4}}, embeddings)
		})
	}
}

func TestOpenAIEmbedder_UnauthorizedAPIKey(t *testing.T) {
	mockResponse := `{
    "error": {
        "message": "Incorrect API key provided: sk-proj-xxx. You can find your API key at https://platform.openai.com/account/api-keys.",
        "type": "invalid_request_error",
        "param": null,
        "code": "invalid_api_key"
    }
}`

	requestPathCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPathCh <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)

	require.Equal(t, "/embeddings", receiveRequestPath(t, requestPathCh))
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")

	core, observedLogs := observer.New(zap.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
	t.Cleanup(restore)

	providerKey := `dash"secret`
	badRequestServerURL := testutil.NewJSONServer(t, http.StatusBadRequest,
		`{"error":{"message":"invalid api key: dash\"secret"}}`)

	embedder = NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return providerKey },
		GetBaseURL: func() string { return badRequestServerURL },
	})
	_, err = embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)
	require.EqualError(t, err, "OpenAI: status code 400, message: invalid api key: [REDACTED]")
	require.NotContains(t, err.Error(), providerKey)

	entries := observedLogs.FilterMessage("OpenAI API request failed").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, int64(http.StatusBadRequest), fields["status"])
	require.Equal(t, "invalid api key: [REDACTED]", fields["message"])
	require.NotContains(t, fields, "body")

	malformedResponseServerURL := testutil.NewJSONServer(t, http.StatusBadGateway, `{"error":`)

	embedder = NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return malformedResponseServerURL },
	})
	_, err = embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)
	require.EqualError(t, err, "OpenAI: status code 502, message: Bad Gateway")

	entries = observedLogs.FilterMessage("OpenAI API request failed").All()
	require.Len(t, entries, 2)
	fields = entries[1].ContextMap()
	require.Equal(t, int64(http.StatusBadGateway), fields["status"])
	require.NotEmpty(t, fields["parse_error"])
	require.NotContains(t, fields, "body")
}

func TestOpenAIEmbedder_InvalidModel(t *testing.T) {
	// Mock model not found response from real OpenAI API
	mockResponse := `
{
    "error": {
        "message": "The model 'text-embedding-3x' does not exist or you do not have access to it.",
        "type": "invalid_request_error",
        "param": null,
        "code": "model_not_found"
    }
}`

	requestPathCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPathCh <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3x", texts, nil)

	require.Equal(t, "/embeddings", receiveRequestPath(t, requestPathCh))
	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model 'text-embedding-3x' does not exist")
}

func TestOpenAIEmbedderContract(t *testing.T) {
	testutil.RunEmbedderContract(t, testutil.EmbedderContract[*Embedder]{
		Model: "text-embedding-3-small",
		New: func(cfg testutil.EmbedderConfig) *Embedder {
			embedder := NewOpenAIEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return cfg.APIKey },
				GetBaseURL:           func() string { return cfg.BaseURL },
				MaxResponseBodyBytes: cfg.MaxResponseBodyBytes,
			})
			embedder.client.Transport = cfg.Transport
			return embedder
		},
		RequestError:              "OpenAI request failed",
		ResponseBodyLimitError:    "response body exceeds maximum size of 64 bytes",
		TransportCauseIsPreserved: true,
	})
}
