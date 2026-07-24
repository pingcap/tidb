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

package base

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBase64EmbeddingF32(t *testing.T) {
	// This data comes from a real JINA embedding using this request:
	// curl https://api.jina.ai/v1/embeddings \
	// -H "Content-Type: application/json" \
	// -H "Authorization: Bearer <token>" \
	// -d @- <<EOFEOF
	// {
	// 	"model": "jina-embeddings-v4",
	// 	"task": "text-matching",
	// 	"dimensions": 10,
	// 	"embedding_type": "base64",
	// 	"input": [{
	// 		"text": "A beautiful sunset over the beach"
	// 	}]
	// }
	// EOFEOF
	decodedBytes, err := base64.StdEncoding.DecodeString("AAAYPgAAEb8AACq+AAAXPgAA4b0AAP0+AACUvQAA4TwAAC67AAAVPw==")
	require.NoError(t, err)
	result, err := DecodeFloat32ArrayBytes(decodedBytes)
	require.NoError(t, err)
	expected := []float32{0.1484375, -0.56640625, -0.166015625, 0.1474609375, -0.10986328125, 0.494140625, -0.072265625, 0.0274658203125, -0.002655029296875, 0.58203125}
	require.Equal(t, expected, result)

	decodedBytes, err = base64.StdEncoding.DecodeString("")
	require.NoError(t, err)
	result, err = DecodeFloat32ArrayBytes(decodedBytes)
	require.NoError(t, err)
	require.Empty(t, result)

	decodedBytes, err = base64.StdEncoding.DecodeString("AAAY")
	require.NoError(t, err)
	_, err = DecodeFloat32ArrayBytes(decodedBytes)
	require.Error(t, err)

	_, err = DecodeFloat32ArrayBytes([]byte{0x00, 0x01, 0x02})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid embedding data")

	_, err = DecodeFloat32ArrayBytes([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid embedding data")

	t.Run("indexed response", func(t *testing.T) {
		items := []IndexedBase64Embedding{
			{Index: 1, Embedding: []byte{0x00, 0x00, 0x00, 0x40}},
			{Index: 0, Embedding: []byte{0x00, 0x00, 0x80, 0x3f}},
		}
		embeddings, err := DecodeIndexedBase64Embeddings(items, 2)
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1}, {2}}, embeddings)

		_, err = DecodeIndexedBase64Embeddings(items[:1], 2)
		require.EqualError(t, err, "response data length 1 does not match input texts length 2")

		for _, index := range []int{-1, 2} {
			invalid := []IndexedBase64Embedding{
				{Index: index, Embedding: []byte{0x00, 0x00, 0x80, 0x3f}},
				{Index: 0, Embedding: []byte{0x00, 0x00, 0x00, 0x40}},
			}
			_, err = DecodeIndexedBase64Embeddings(invalid, 2)
			require.EqualError(t, err, fmt.Sprintf("response data index %d is out of range [0, 2)", index))
		}

		duplicate := []IndexedBase64Embedding{
			{Index: 0, Embedding: []byte{0x00, 0x00, 0x80, 0x3f}},
			{Index: 0, Embedding: []byte{0x00, 0x00, 0x00, 0x40}},
		}
		_, err = DecodeIndexedBase64Embeddings(duplicate, 2)
		require.EqualError(t, err, "response data contains duplicate index 0")

		malformed := []IndexedBase64Embedding{{Index: 0, Embedding: []byte{0x00}}}
		_, err = DecodeIndexedBase64Embeddings(malformed, 1)
		require.EqualError(t, err, "failed to decode embedding for index 0: invalid embedding data")
	})
}

func TestReadResponseBody(t *testing.T) {
	body, err := ReadResponseBody(strings.NewReader(strings.Repeat("x", 64)), 64)
	require.NoError(t, err)
	require.Len(t, body, 64)

	_, err = ReadResponseBody(strings.NewReader(strings.Repeat("x", 65)), 64)
	require.EqualError(t, err, "response body exceeds maximum size of 64 bytes")

	body, err = ReadResponseBody(strings.NewReader("x"), math.MaxInt64)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), body)

	_, err = ReadResponseBody(strings.NewReader(""), -1)
	require.EqualError(t, err, "maximum response body size must not be negative")
}

func TestJSONFieldsWithOptions(t *testing.T) {
	fixed := map[string]any{
		"model":           "fixed-model",
		"input":           []string{"fixed-input"},
		"encoding_format": "base64",
	}
	opts := map[string]any{
		"model":           "overridden-model",
		"input":           []string{"overridden-input"},
		"encoding_format": "float",
		"dimensions":      512,
	}

	merged := JSONFieldsWithOptions(fixed, opts)
	require.Equal(t, "fixed-model", merged["model"])
	require.Equal(t, []string{"fixed-input"}, merged["input"])
	require.Equal(t, "base64", merged["encoding_format"])
	require.Equal(t, 512, merged["dimensions"])
}

func TestSanitizeErrorText(t *testing.T) {
	text := `{"authorization":"Bearer secret-token","api_key":"plain-key","message":"Bearer another-secret"}`
	sanitized := SanitizeErrorText(text)
	require.NotContains(t, sanitized, "secret-token")
	require.NotContains(t, sanitized, "plain-key")
	require.NotContains(t, sanitized, "another-secret")
	require.Contains(t, sanitized, "[REDACTED]")

	openAIKey := "sk-proj-super-secret-value"
	sanitized = SanitizeErrorText(`{"message":"Incorrect API key provided: ` + openAIKey + `"}`)
	require.NotContains(t, sanitized, openAIKey)
	require.Contains(t, sanitized, "[REDACTED]")

	providerKey := "dashscope-secret-value"
	sanitized = SanitizeErrorText(
		`{"error":{"message":"invalid api key: `+providerKey+`"}}`,
		providerKey,
	)
	require.NotContains(t, sanitized, providerKey)
	require.Contains(t, sanitized, "invalid api key: [REDACTED]")

	longKey := strings.Repeat("s", maxSanitizedErrorTextBytes+128)
	sanitized = SanitizeErrorText(`{"api_key":"` + longKey + `"}`)
	require.NotContains(t, sanitized, longKey[:maxSanitizedErrorTextBytes/2])
	require.Contains(t, sanitized, "[REDACTED]")
	require.LessOrEqual(t, len(sanitized), maxSanitizedErrorTextBytes+len("...[truncated]"))
}

func TestRedactedErrors(t *testing.T) {
	const secretURL = "http://internal.example/embed?token=super-secret"
	cause := &url.Error{Op: "Post", URL: secretURL, Err: errors.New("connection failed")}

	err := NewRedactedError("invalid provider endpoint", cause)
	require.EqualError(t, err, "invalid provider endpoint")
	require.NotContains(t, err.Error(), secretURL)
	require.ErrorIs(t, err, cause)

	err = NewProviderRequestError(context.Background(), "test provider", cause)
	require.EqualError(t, err, "test provider request failed")
	require.NotContains(t, err.Error(), secretURL)
	require.ErrorIs(t, err, cause)

	customCause := errors.New("caller stopped request")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(customCause)
	err = NewProviderRequestError(ctx, "test provider", cause)
	require.ErrorIs(t, err, customCause)
	require.Equal(t, customCause, err)
}

func TestHTTPHelpers(t *testing.T) {
	t.Run("API key provider config", func(t *testing.T) {
		fallbackErr := errors.New("default missing API key error")
		customErr := errors.New("custom missing API key error")
		cfg := APIKeyProviderConfig{
			GetAPIKey:        func() string { return "test-api-key" },
			GetBaseURL:       func() string { return "https://example.com/embed" },
			ErrMissingAPIKey: customErr,
		}

		normalized := cfg.WithDefaults()
		require.Zero(t, cfg.MaxResponseBodyBytes)
		require.Equal(t, DefaultMaxResponseBodyBytes, normalized.MaxResponseBodyBytes)
		require.Equal(t, "https://example.com/embed", normalized.ConfiguredBaseURL())
		apiKey, err := normalized.ResolveAPIKey(fallbackErr)
		require.NoError(t, err)
		require.Equal(t, "test-api-key", apiKey)

		customLimit := APIKeyProviderConfig{MaxResponseBodyBytes: 64}.WithDefaults()
		require.Equal(t, int64(64), customLimit.MaxResponseBodyBytes)
		require.Empty(t, customLimit.ConfiguredBaseURL())

		normalized.GetAPIKey = func() string { return "" }
		_, err = normalized.ResolveAPIKey(fallbackErr)
		require.ErrorIs(t, err, customErr)

		normalized.ErrMissingAPIKey = nil
		_, err = normalized.ResolveAPIKey(fallbackErr)
		require.ErrorIs(t, err, fallbackErr)

		_, err = APIKeyProviderConfig{}.ResolveAPIKey(nil)
		require.EqualError(t, err, "API key is not configured")
	})

	t.Run("parse HTTP URL", func(t *testing.T) {
		u, err := ParseHTTPURL("  https://example.com/root?tenant=x  ", "test provider URL")
		require.NoError(t, err)
		require.Equal(t, "https://example.com/root?tenant=x", u.String())

		const secret = "super-secret"
		_, err = ParseHTTPURL("https://example.com/%zz?token="+secret, "test provider URL")
		require.EqualError(t, err, "invalid test provider URL")
		require.NotContains(t, err.Error(), secret)

		for _, rawURL := range []string{"/relative", "ftp://example.com/path"} {
			_, err = ParseHTTPURL(rawURL, "test provider URL")
			require.EqualError(t, err, "invalid test provider URL: absolute HTTP(S) URL is required")
		}
	})

	t.Run("escaped path", func(t *testing.T) {
		u, err := ParseHTTPURL("https://example.com?tenant=x", "test provider URL")
		require.NoError(t, err)
		require.NoError(t, SetEscapedURLPath(u, "/models/org%2Fmodel", "test provider URL path"))
		require.Equal(t, "https://example.com/models/org%2Fmodel?tenant=x", u.String())

		const secret = "super-secret"
		err = SetEscapedURLPath(u, "/%zz/"+secret, "test provider URL path")
		require.EqualError(t, err, "invalid test provider URL path")
		require.NotContains(t, err.Error(), secret)
	})

	t.Run("JSON request", func(t *testing.T) {
		req, err := NewJSONRequest(context.Background(), "test provider", "https://example.com/embed", []byte(`{"input":"test"}`))
		require.NoError(t, err)
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"input":"test"}`, string(body))
	})

	t.Run("execute request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		req, err := NewJSONRequest(context.Background(), "test provider", server.URL, nil)
		require.NoError(t, err)
		statusCode, body, err := DoRequest(context.Background(), &http.Client{}, "test provider", req, 64)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, statusCode)
		require.JSONEq(t, `{"ok":true}`, string(body))
	})

	t.Run("post JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Errorf("unexpected method %q", r.Method)
			}
			if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
				t.Errorf("unexpected content type %q", contentType)
			}
			if authorization := r.Header.Get("Authorization"); authorization != "Bearer test-key" {
				t.Errorf("unexpected authorization header %q", authorization)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("failed to read request body: %v", err)
			}
			if string(body) != `{"input":["hello"]}` {
				t.Errorf("unexpected request body %q", body)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		statusCode, body, err := PostJSON(
			context.Background(),
			&http.Client{},
			"test provider",
			server.URL,
			map[string]any{"input": []string{"hello"}},
			http.Header{"Authorization": []string{"Bearer test-key"}},
			64,
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, statusCode)
		require.JSONEq(t, `{"ok":true}`, string(body))

		_, _, err = PostJSON(
			context.Background(),
			&http.Client{},
			"test provider",
			server.URL,
			map[string]any{"unsupported": make(chan struct{})},
			nil,
			64,
		)
		require.ErrorContains(t, err, "unexpected marshal request error")
	})

	t.Run("response error", func(t *testing.T) {
		err := NewProviderResponseError("test provider", http.StatusBadRequest, "invalid input")
		require.EqualError(t, err, "test provider: status code 400, message: invalid input")
		err = NewProviderResponseError("test provider", http.StatusServiceUnavailable, "")
		require.EqualError(t, err, "test provider: status code 503, message: Service Unavailable")
	})
}
