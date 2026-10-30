// Copyright 2026 PingCAP, Inc.
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

// Package testutil provides shared contract tests and fixtures for embedding
// provider adapters.
package testutil

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

const (
	testAPIKey         = "test-api-key"
	testSecret         = "super-secret"
	testProviderSecret = "provider-secret"
)

// RoundTripFunc adapts a function to http.RoundTripper.
type RoundTripFunc func(*http.Request) (*http.Response, error)

// RoundTrip implements http.RoundTripper.
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// EmbedderConfig contains the protocol-independent test knobs used by an
// embedding provider factory.
type EmbedderConfig struct {
	APIKey               string
	BaseURL              string
	MaxResponseBodyBytes int64
	Transport            http.RoundTripper
}

// EmbedderContract describes behavior shared by every provider adapter.
type EmbedderContract[T base.Embedder] struct {
	Model                     string
	New                       func(EmbedderConfig) T
	RequestError              string
	ResponseBodyLimitError    string
	TransportCauseIsPreserved bool
	RedactionResponse         string
	RedactionError            string
}

// RunEmbedderContract verifies behavior that is independent of provider wire
// formats. Provider-specific request, response, and status mappings remain in
// each provider package's own tests.
func RunEmbedderContract[T base.Embedder](t *testing.T, contract EmbedderContract[T]) {
	t.Helper()

	t.Run("empty texts", func(t *testing.T) {
		embedder := contract.New(EmbedderConfig{APIKey: testAPIKey, BaseURL: "http://unused.example"})
		embeddings, err := embedder.CreateEmbeddings(context.Background(), contract.Model, nil, nil)
		if err != nil {
			t.Fatalf("empty texts returned an error: %v", err)
		}
		if len(embeddings) != 0 {
			t.Fatalf("empty texts returned %d embeddings, expected none", len(embeddings))
		}
	})

	t.Run("empty model", func(t *testing.T) {
		embedder := contract.New(EmbedderConfig{APIKey: testAPIKey, BaseURL: "http://unused.example"})
		embeddings, err := embedder.CreateEmbeddings(context.Background(), "", []string{"test"}, nil)
		if embeddings != nil {
			t.Fatalf("empty model returned embeddings: %v", embeddings)
		}
		if err == nil || !strings.Contains(err.Error(), "model name is required") {
			t.Fatalf("empty model returned error %v, expected model name validation", err)
		}
	})

	t.Run("response body limit", func(t *testing.T) {
		for _, statusCode := range []int{http.StatusOK, http.StatusBadRequest} {
			t.Run(http.StatusText(statusCode), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(statusCode)
					_, _ = w.Write([]byte(strings.Repeat("x", 65)))
				}))
				t.Cleanup(server.Close)

				embedder := contract.New(EmbedderConfig{
					APIKey:               testAPIKey,
					BaseURL:              server.URL,
					MaxResponseBodyBytes: 64,
				})
				_, err := embedder.CreateEmbeddings(context.Background(), contract.Model, []string{"test"}, nil)
				if err == nil || !strings.Contains(err.Error(), contract.ResponseBodyLimitError) {
					t.Fatalf("oversized response returned error %v, expected %q", err, contract.ResponseBodyLimitError)
				}
			})
		}
	})

	t.Run("transport error redaction", func(t *testing.T) {
		transportErr := errors.New("transport failed")
		embedder := contract.New(EmbedderConfig{
			APIKey:  testAPIKey,
			BaseURL: "https://internal.example/root?token=" + testSecret,
			Transport: RoundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, transportErr
			}),
		})

		_, err := embedder.CreateEmbeddings(context.Background(), contract.Model, []string{"test"}, nil)
		if err == nil || err.Error() != contract.RequestError {
			t.Fatalf("transport failure returned error %v, expected %q", err, contract.RequestError)
		}
		if strings.Contains(err.Error(), testSecret) {
			t.Fatalf("transport failure exposed secret in %q", err.Error())
		}
		if contract.TransportCauseIsPreserved && !errors.Is(err, transportErr) {
			t.Fatalf("transport failure did not preserve its cause: %v", err)
		}
	})

	if contract.RedactionResponse != "" {
		t.Run("error redaction", func(t *testing.T) {
			serverURL := NewJSONServer(t, http.StatusBadRequest, contract.RedactionResponse)
			embedder := contract.New(EmbedderConfig{
				APIKey:  testProviderSecret,
				BaseURL: serverURL,
			})

			_, err := embedder.CreateEmbeddings(context.Background(), contract.Model, []string{"test"}, nil)
			if err == nil || err.Error() != contract.RedactionError {
				t.Fatalf("provider error returned %v, expected %q", err, contract.RedactionError)
			}
			if strings.Contains(err.Error(), testProviderSecret) {
				t.Fatalf("provider error exposed secret in %q", err.Error())
			}
		})
	}

	t.Run("context cause", func(t *testing.T) {
		cause := errors.New("request canceled by caller")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)

		embedder := contract.New(EmbedderConfig{
			APIKey:  testAPIKey,
			BaseURL: "http://127.0.0.1",
			Transport: RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				return nil, req.Context().Err()
			}),
		})
		_, err := embedder.CreateEmbeddings(ctx, contract.Model, []string{"test"}, nil)
		if !errors.Is(err, cause) {
			t.Fatalf("context cancellation returned %v, expected cause %v", err, cause)
		}
	})
}

// NewJSONServer starts a test server that returns a fixed JSON response and
// registers its cleanup with t.
func NewJSONServer(t *testing.T, statusCode int, body string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// EncodeFloat32Base64 returns the base64 representation used by providers
// whose embedding response is a little-endian float32 byte array.
func EncodeFloat32Base64(values ...float32) string {
	data := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(data[i*4:], math.Float32bits(value))
	}
	return base64.StdEncoding.EncodeToString(data)
}
