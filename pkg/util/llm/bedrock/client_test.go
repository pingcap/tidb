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

package bedrock

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/tidb/pkg/util/llm"
	"github.com/stretchr/testify/require"
)

func TestBedrockClientCompleteRequest(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"results":[{"outputText":"ok"}]}`))
	}))
	t.Cleanup(srv.Close)

	client := newTestClient(t, srv.URL)
	temp := 0.2
	topP := 0.9
	opts := llm.CompleteOptions{
		MaxTokens:   256,
		Temperature: &temp,
		TopP:        &topP,
	}
	out, err := client.Complete(context.Background(), "amazon.titan-text-v1", "hi", opts)
	require.NoError(t, err)
	require.Equal(t, "ok", out)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &payload))
	require.Equal(t, "hi", payload["inputText"])
	cfg := payload["textGenerationConfig"].(map[string]any)
	require.Equal(t, float64(256), cfg["maxTokenCount"])
	require.Equal(t, 0.2, cfg["temperature"])
	require.Equal(t, 0.9, cfg["topP"])
}

func TestBedrockClientEmbedTextRequest(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"embedding":[0.1,0.2]}`))
	}))
	t.Cleanup(srv.Close)

	client := newTestClient(t, srv.URL)
	vec, err := client.EmbedText(context.Background(), "amazon.titan-embed-text-v1", "hi")
	require.NoError(t, err)
	require.Equal(t, []float32{0.1, 0.2}, vec)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(gotBody, &payload))
	require.Equal(t, "hi", payload["inputText"])
}

func TestBedrockClientValidatesModelPrefix(t *testing.T) {
	client := newTestClient(t, "http://localhost")
	_, err := client.Complete(context.Background(), "anthropic.claude-v2", "hi", llm.CompleteOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "amazon.titan-text")

	_, err = client.EmbedText(context.Background(), "amazon.titan-text-v1", "hi")
	require.Error(t, err)
	require.Contains(t, err.Error(), "amazon.titan-embed-text")
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := NewClient(context.Background(), Config{
		Region:              "us-east-1",
		Endpoint:            endpoint,
		Timeout:             2 * time.Second,
		Credentials:         credentials.NewStaticCredentials("akid", "secret", ""),
	})
	require.NoError(t, err)
	return client
}
