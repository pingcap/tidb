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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
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
	// Mock successful response from real OpenAI API
	mockResponse := `
    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "index": 0,
          "embedding": "oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="
        },
        {
          "object": "embedding",
          "index": 1,
          "embedding": "LhF9PkGwzzwFGLA+qFe+PlU42j5Fo4K+ExUAvwi7eD5pNLU9ucivPg=="
        },
        {
          "object": "embedding",
          "index": 2,
          "embedding": "fSC3PuGE5b3tlAc/C0BCPYgoMTvDC+k+MTB5PkS+7j5W9pm+4DxNvQ=="
        },
        {
          "object": "embedding",
          "index": 3,
          "embedding": "9Lo8vsEPLD4Dqho/+EVqPtFTV75Zg6q9NR3HPfjuGL6j26C+SwQVvw=="
        },
        {
          "object": "embedding",
          "index": 4,
          "embedding": "57uJvmuWor7Zn0o+adtgPt1OlD5osRC/TzTUvgBgGD6aNfI9qEi3vg=="
        }
      ],
      "model": "text-embedding-3-small",
      "usage": {
        "prompt_tokens": 7,
        "total_tokens": 7
      }
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

	texts := []string{"hello world", "test text", "sample input", "more text", "last item"}
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
		"input": ["hello world", "test text", "sample input", "more text", "last item"],
		"encoding_format": "base64"
	}`, string(request.body))
	require.Len(t, embeddings, 5)
	require.Equal(t, embeddings[0], []float32{
		0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
	})
	require.Equal(t, embeddings[1], []float32{
		0.24713585, 0.0253526, 0.34393325, 0.3717625, 0.42621103, -0.2551519, -0.50032157, 0.24290097, 0.08847887, 0.34332827,
	})
	require.Equal(t, embeddings[2], []float32{
		0.35766974, -0.11206985, 0.5296162, 0.047424357, 0.0027032215, 0.45516786, 0.2433479, 0.46629536, -0.30070752, -0.050106883,
	})
	require.Equal(t, embeddings[3], []float32{
		-0.18430692, 0.16802885, 0.6041567, 0.22878253, -0.21028067, -0.08325834, 0.09722368, -0.1493491, -0.3141757, -0.58209676,
	})
	require.Equal(t, embeddings[4], []float32{
		-0.2690117, -0.31755385, 0.1978754, 0.21958698, 0.28966418, -0.565207, -0.41446158, 0.14880371, 0.1182663, -0.3579762,
	})
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

func TestOpenAIEmbedderResponseBodyLimit(t *testing.T) {
	body, err := readResponseBody(strings.NewReader(strings.Repeat("x", 64)), 64)
	require.NoError(t, err)
	require.Len(t, body, 64)

	for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(strings.Repeat("x", 65)))
			}))
			defer server.Close()

			embedder := NewOpenAIEmbedder(EmbedderConfig{
				GetAPIKey:            func() string { return "test-api-key" },
				GetBaseURL:           func() string { return server.URL },
				MaxResponseBodyBytes: 64,
			})

			_, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, nil)
			require.ErrorContains(t, err, "response body exceeds maximum size of 64 bytes")
		})
	}
}

func TestOpenAIEmbedder_ResponseIndexValidation(t *testing.T) {
	tests := []struct {
		name         string
		responseData string
		errContains  string
	}{
		{
			name: "out of order",
			responseData: `[
				{"object":"embedding","index":1,"embedding":"LhF9PkGwzzwFGLA+qFe+PlU42j5Fo4K+ExUAvwi7eD5pNLU9ucivPg=="},
				{"object":"embedding","index":0,"embedding":"oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="}
			]`,
		},
		{
			name: "duplicate index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="},
				{"object":"embedding","index":0,"embedding":"LhF9PkGwzzwFGLA+qFe+PlU42j5Fo4K+ExUAvwi7eD5pNLU9ucivPg=="}
			]`,
			errContains: "duplicate index 0",
		},
		{
			name: "out of range index",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"oTwEP2H/Kz4Jwho/Gf2RPvl5lb3N1IU+z+t6Pb9Sej5h/6u+UXO5vQ=="},
				{"object":"embedding","index":2,"embedding":"LhF9PkGwzzwFGLA+qFe+PlU42j5Fo4K+ExUAvwi7eD5pNLU9ucivPg=="}
			]`,
			errContains: "out of range",
		},
		{
			name: "invalid decoded embedding length",
			responseData: `[
				{"object":"embedding","index":0,"embedding":"AAEC"},
				{"object":"embedding","index":1,"embedding":"LhF9PkGwzzwFGLA+qFe+PlU42j5Fo4K+ExUAvwi7eD5pNLU9ucivPg=="}
			]`,
			errContains: "invalid embedding data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"object":"list","model":"text-embedding-3-small","data":` + tt.responseData + `}`))
			}))
			defer server.Close()

			embedder := NewOpenAIEmbedder(EmbedderConfig{
				GetAPIKey:  func() string { return "test-api-key" },
				GetBaseURL: func() string { return server.URL },
			})

			embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"a", "b"}, nil)
			if tt.errContains != "" {
				require.Nil(t, embeddings)
				require.ErrorContains(t, err, tt.errContains)
				return
			}
			require.NoError(t, err)
			require.Len(t, embeddings, 2)
			require.Equal(t, []float32{
				0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
			}, embeddings[0])
			require.Equal(t, []float32{
				0.24713585, 0.0253526, 0.34393325, 0.3717625, 0.42621103, -0.2551519, -0.50032157, 0.24290097, 0.08847887, 0.34332827,
			}, embeddings[1])
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
	badRequestServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"invalid api key: dash\"secret"}}`))
	}))
	defer badRequestServer.Close()

	embedder = NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return providerKey },
		GetBaseURL: func() string { return badRequestServer.URL },
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

	malformedResponseServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":`))
	}))
	defer malformedResponseServer.Close()

	embedder = NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return malformedResponseServer.URL },
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

func TestOpenAIEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestOpenAIEmbedder_NoModel(t *testing.T) {
	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
}
