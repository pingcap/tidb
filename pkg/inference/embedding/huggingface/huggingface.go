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

package huggingface

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// DefaultAPIBaseURL is the default base URL for HuggingFace inference API.
const DefaultAPIBaseURL = "https://router.huggingface.co/hf-inference"

// Embedder is for HuggingFace embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewHuggingFaceEmbedder creates a new HuggingFaceEmbedder instance with the provided configuration.
// cfg.GetBaseURL returns the inference API base; the embedder appends
// /models/<model>/pipeline/feature-extraction. An empty value uses
// DefaultAPIBaseURL.
func NewHuggingFaceEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg.WithDefaults(),
	}
}

func featureExtractionEndpoint(configured, model string) (string, error) {
	baseURL := strings.TrimSpace(configured)
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}
	u, err := base.ParseHTTPURL(baseURL, "HuggingFace API base URL")
	if err != nil {
		return "", err
	}
	modelParts := strings.Split(model, "/")
	for i := range modelParts {
		modelParts[i] = escapePathSegment(modelParts[i])
	}
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/models/" + strings.Join(modelParts, "/") + "/pipeline/feature-extraction"
	if err := base.SetEscapedURLPath(u, escapedPath, "HuggingFace API base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

func escapePathSegment(segment string) string {
	escaped := url.PathEscape(segment)
	// url.PathEscape intentionally leaves the complete dot segments "." and
	// ".." unchanged. Escape them explicitly so intermediaries cannot remove
	// or normalize model path segments according to RFC 3986 section 5.2.4.
	if escaped == "." {
		return "%2E"
	}
	if escaped == ".." {
		return "%2E%2E"
	}
	return escaped
}

func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.Error, nil
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var embeddings Response
	if err := json.Unmarshal(body, &embeddings); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(embeddings) != expectedCount {
		return nil, fmt.Errorf("response data length %d does not match input texts length %d", len(embeddings), expectedCount)
	}
	return embeddings, nil
}

func (e *Embedder) unauthorizedError() error {
	if e.cfg.ErrUnauthorized != nil {
		return e.cfg.ErrUnauthorized
	}
	return fmt.Errorf("HuggingFace returns status unauthorized, check API key")
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://huggingface.co/docs/inference-providers/en/tasks/feature-extraction
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}

	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for HuggingFace"))
	if err != nil {
		return nil, err
	}

	endpoint, err := featureExtractionEndpoint(e.cfg.ConfiguredBaseURL(), model)
	if err != nil {
		return nil, err
	}

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "HuggingFace",
		Client:   &e.client,
		Endpoint: endpoint,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"inputs": texts,
		}, opts),
		Headers:              http.Header{"Authorization": {"Bearer " + apiKey}},
		MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
		Secrets:              []string{apiKey},
		DecodeErrorMessage:   decodeErrorMessage,
		StatusErrors: map[int]error{
			http.StatusUnauthorized: e.unauthorizedError(),
			http.StatusNotFound:     fmt.Errorf("HuggingFace model '%s' does not exist or is not available", model),
		},
		DecodeEmbeddings: decodeEmbeddings,
	})
}
