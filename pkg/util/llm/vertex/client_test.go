// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vertex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/llm"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestVertexClientCompleteRetriesOn429(t *testing.T) {
	var (
		mu       sync.Mutex
		attempts int
		gotPath  string
		gotAuth  string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()

		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	}))
	t.Cleanup(srv.Close)

	client := newTestVertexClient(srv.URL)
	text, err := client.Complete(context.Background(), "model", "hi", llm.CompleteOptions{})
	require.NoError(t, err)
	require.Equal(t, "ok", text)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 3, attempts)
	require.Equal(t, "/v1/projects/p1/locations/loc/publishers/google/models/model:generateContent", gotPath)
	require.Equal(t, "Bearer test-token", gotAuth)
}

func TestVertexClientCompleteIncludesOptions(t *testing.T) {
	var (
		mu      sync.Mutex
		gotBody []byte
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		mu.Lock()
		gotBody = body
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	}))
	t.Cleanup(srv.Close)

	client := newTestVertexClient(srv.URL)
	temp := 0.2
	topP := 0.9
	opts := llm.CompleteOptions{
		MaxTokens:   256,
		Temperature: &temp,
		TopP:        &topP,
	}
	_, err := client.Complete(context.Background(), "model", "hi", opts)
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &payload))
	gen, ok := payload["generationConfig"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(256), gen["maxOutputTokens"])
	require.Equal(t, 0.2, gen["temperature"])
	require.Equal(t, 0.9, gen["topP"])
}

func TestVertexClientEmbedReturnsVector(t *testing.T) {
	var (
		mu      sync.Mutex
		gotPath string
		gotAuth string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"predictions":[{"embeddings":{"values":[0.1,0.2]}}]}`)
	}))
	t.Cleanup(srv.Close)

	client := newTestVertexClient(srv.URL)
	vec, err := client.EmbedText(context.Background(), "model", "hi")
	require.NoError(t, err)
	require.Equal(t, []float32{0.1, 0.2}, vec)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "/v1/projects/p1/locations/loc/publishers/google/models/model:predict", gotPath)
	require.Equal(t, "Bearer test-token", gotAuth)
}

func newTestVertexClient(baseURL string) *Client {
	client, err := NewClient(Config{
		BaseURL:   baseURL,
		Project:  "p1",
		Location: "loc",
		TokenSource: oauth2.StaticTokenSource(&oauth2.Token{
			AccessToken: "test-token",
			Expiry:      time.Now().Add(time.Hour),
			TokenType:   "Bearer",
		}),
		Timeout:    2 * time.Second,
		MaxRetries: 2,
	})
	if err != nil {
		panic(errors.Trace(err))
	}
	client.sleep = func(time.Duration) {}
	return client
}
