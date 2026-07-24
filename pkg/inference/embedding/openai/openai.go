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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// DefaultAPIBaseURL is the default base URL (without the /embeddings suffix) for OpenAI embeddings API.
const DefaultAPIBaseURL = "https://api.openai.com/v1"

// Embedder is for OpenAI embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewOpenAIEmbedder creates a new OpenAIEmbedder instance with the provided configuration.
// cfg.GetBaseURL may return an OpenAI-compatible API base URL with or without
// the trailing /embeddings path.
func NewOpenAIEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg.WithDefaults(),
	}
}

// embeddingsEndpoint resolves an OpenAI-compatible base URL to the embeddings endpoint.
// The OpenAI API defines the endpoint as POST /v1/embeddings:
// https://platform.openai.com/docs/api-reference/embeddings/create
// For compatibility with existing callers, baseURL may already end in /embeddings.
func embeddingsEndpoint(baseURL string) (string, error) {
	u, err := base.ParseHTTPURL(baseURL, "OpenAI API base URL")
	if err != nil {
		return "", err
	}

	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	if !strings.HasSuffix(escapedPath, "/embeddings") {
		escapedPath += "/embeddings"
	}
	if err := base.SetEscapedURLPath(u, escapedPath, "OpenAI API base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.Error.Message, nil
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(response.Data, expectedCount)
}

func (e *Embedder) unauthorizedError() error {
	if e.cfg.ErrUnauthorized != nil {
		return e.cfg.ErrUnauthorized
	}
	return fmt.Errorf("OpenAI returns status unauthorized, check API key")
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://platform.openai.com/docs/api-reference/embeddings/create
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for OpenAI"))
	if err != nil {
		return nil, err
	}
	baseURL := DefaultAPIBaseURL
	if configured := strings.TrimSpace(e.cfg.ConfiguredBaseURL()); configured != "" {
		baseURL = configured
	}
	endpoint, err := embeddingsEndpoint(baseURL)
	if err != nil {
		return nil, err
	}

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "OpenAI",
		Client:   &e.client,
		Endpoint: endpoint,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"model":           model,
			"input":           texts,
			"encoding_format": "base64",
		}, opts),
		Headers:              http.Header{"Authorization": {"Bearer " + apiKey}},
		MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
		Secrets:              []string{apiKey},
		DecodeErrorMessage:   decodeErrorMessage,
		StatusErrors: map[int]error{
			http.StatusUnauthorized: e.unauthorizedError(),
		},
		DecodeEmbeddings: decodeEmbeddings,
	})
}
