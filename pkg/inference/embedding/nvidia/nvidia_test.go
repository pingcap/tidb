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

package nvidia

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNvidiaEmbedder_Success(t *testing.T) {
	// Mock successful response from Nvidia NIM API
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
      "model": "baai/bge-m3",
      "usage": {
        "prompt_tokens": 18,
        "total_tokens": 18
      }
    }`

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and headers
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.Equal(t, "Bearer test-api-key", r.Header.Get("Authorization"))

		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "baai/bge-m3",
			"input": ["hello world", "test text", "sample input", "more text", "last item"],
			"encoding_format": "base64"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	// Create embedder with mock server URL
	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world", "test text", "sample input", "more text", "last item"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", texts, nil)

	require.NoError(t, err)
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

func TestNvidiaEmbedder_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "baai/bge-m3",
			"input": ["test"],
			"encoding_format": "base64",
			"input_type": "query"
		}`, string(body))

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
      "model": "baai/bge-m3",
      "usage": {
        "prompt_tokens": 7,
        "total_tokens": 7
      }
    }`

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, map[string]any{
		"input_type": "query",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
	})
}

func TestNvidiaEmbedder_UnauthorizedAPIKey(t *testing.T) {
	mockResponse := `{
    "status": 403,
    "title": "Forbidden",
    "detail": "Authorization failed"
}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestNvidiaEmbedder_InvalidModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("404 page not found"))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3xxxx", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "model 'baai/bge-m3xxxx' does not exist")
}

func TestNvidiaEmbedder_BadRequest(t *testing.T) {
	mockResponse := `{
    "error": "The model expects an input_type from one of 'passage' or 'query' but none was provided."
}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "nvidia/embed-qa-4", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "input_type")
}

func TestNvidiaEmbedder_EmptyTexts(t *testing.T) {
	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{}, nil)
	require.NoError(t, err)
	require.Len(t, embeddings, 0)
}

func TestNvidiaEmbedder_NoModel(t *testing.T) {
	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return "http://mock-url" },
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Error(t, err)
}

func TestNvidiaEmbedder_MissingAPIKey(t *testing.T) {
	customErr := fmt.Errorf("custom missing API key error")
	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:        func() string { return "" },
		GetBaseURL:       func() string { return "http://mock-url" },
		ErrMissingAPIKey: customErr,
	})
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}

func TestNvidiaEmbedder_CustomUnauthorizedError(t *testing.T) {
	customErr := fmt.Errorf("custom unauthorized error")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"detail": "Authorization failed"}`))
	}))
	defer server.Close()

	embedder := NewNvidiaEmbedder(EmbedderConfig{
		GetAPIKey:       func() string { return "invalid-key" },
		GetBaseURL:      func() string { return server.URL },
		ErrUnauthorized: customErr,
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "baai/bge-m3", []string{"test"}, nil)
	require.Nil(t, embeddings)
	require.Equal(t, customErr, err)
}
