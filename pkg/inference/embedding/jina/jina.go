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

package jina

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// DefaultAPIBaseURL is the default endpoint URL for the Jina AI embeddings API.
const DefaultAPIBaseURL = "https://api.jina.ai/v1/embeddings"

// Embedder is for JinaAI embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewJinaEmbedder creates a new JinaEmbedder instance with the provided configuration.
// cfg.GetBaseURL returns the complete Jina AI embeddings endpoint; an empty
// value uses DefaultAPIBaseURL.
func NewJinaEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg.WithDefaults(),
	}
}

func embeddingsEndpoint(configured string) (string, error) {
	endpoint := strings.TrimSpace(configured)
	if endpoint == "" {
		endpoint = DefaultAPIBaseURL
	}
	u, err := base.ParseHTTPURL(endpoint, "Jina AI API base URL")
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.Detail, nil
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(response.Data, expectedCount)
}

func validateOptions(opts map[string]any) error {
	if enabled, ok := opts["return_multivector"].(bool); ok && enabled {
		return fmt.Errorf("JinaAI option return_multivector=true is not supported")
	}
	return nil
}

func (e *Embedder) unauthorizedError() error {
	if e.cfg.ErrUnauthorized != nil {
		return e.cfg.ErrUnauthorized
	}
	return fmt.Errorf("JinaAI returns status unauthorized, check API key")
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://jina.ai/embeddings/
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for JinaAI"))
	if err != nil {
		return nil, err
	}
	endpoint, err := embeddingsEndpoint(e.cfg.ConfiguredBaseURL())
	if err != nil {
		return nil, err
	}

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "JinaAI",
		Client:   &e.client,
		Endpoint: endpoint,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"model":          model,
			"input":          texts,
			"embedding_type": "base64",
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
