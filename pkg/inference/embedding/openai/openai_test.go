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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIEmbedder_Success(t *testing.T) {
	// Mock successful response from real Jina API
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
			"model": "text-embedding-3-small",
			"input": ["hello world", "test text", "sample input", "more text", "last item"],
			"encoding_format": "base64"
		}`, string(body))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(mockResponse))
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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request body
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"model": "text-embedding-3-small",
			"input": ["test"],
			"encoding_format": "base64",
			"my_opt": "abc"
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
      "model": "text-embedding-3-small",
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

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "test-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", []string{"test"}, map[string]any{
		"my_opt": "abc",
	})

	require.NoError(t, err)
	require.Len(t, embeddings, 1)
	require.Equal(t, embeddings[0], []float32{
		0.5165501, 0.16796638, 0.60452324, 0.2851341, -0.07298655, 0.26138917, 0.06126004, 0.24445628, -0.33593276, -0.09055198,
	})
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

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "invalid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3-small", texts, nil)

	require.Nil(t, embeddings)
	require.Error(t, err)
	require.ErrorContains(t, err, "check API key")
}

func TestOpenAIEmbedder_InvalidModel(t *testing.T) {
	// Mock model not found response from real Jina API
	mockResponse := `
{
    "error": {
        "message": "The model 'text-embedding-3x' does not exist or you do not have access to it.",
        "type": "invalid_request_error",
        "param": null,
        "code": "model_not_found"
    }
}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(mockResponse))
	}))
	defer server.Close()

	embedder := NewOpenAIEmbedder(EmbedderConfig{
		GetAPIKey:  func() string { return "valid-api-key" },
		GetBaseURL: func() string { return server.URL },
	})

	texts := []string{"hello world"}
	embeddings, err := embedder.CreateEmbeddings(context.Background(), "text-embedding-3x", texts, nil)

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
