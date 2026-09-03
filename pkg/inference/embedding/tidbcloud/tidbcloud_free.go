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

package tidbcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// Embedder is for TiDB Cloud Free embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for TiDBCloudFreeEmbedder.
type EmbedderConfig struct {
	// GetBillingID returns the billing identifier appended to the request
	// path. An empty value uses the service's default billing identifier.
	GetBillingID func() string
	// GetAPIKey returns an optional API key. An empty key sends the request
	// without an Authorization header.
	GetAPIKey func() string
	// GetBaseURL returns the TiDB Cloud Inference service base. The embedder
	// appends /api/v1/inference/embeddings/<billing-id>.
	GetBaseURL func() string
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use base.DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewTiDBCloudFreeEmbedder creates a new TiDBCloudFreeEmbedder instance with the provided configuration.
func NewTiDBCloudFreeEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = base.DefaultMaxResponseBodyBytes
	}
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
}

func embeddingsEndpoint(configured, billingID string) (string, error) {
	u, err := base.ParseHTTPURL(configured, "TiDB Cloud Inference base URL")
	if err != nil {
		return "", err
	}
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/api/v1/inference/embeddings/" + base.EscapeURLPathSegment(billingID)
	if err := base.SetEscapedURLPath(u, escapedPath, "TiDB Cloud Inference base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.Error, nil
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(response.Embeddings) != expectedCount {
		return nil, fmt.Errorf("response embeddings length %d does not match input texts length %d", len(response.Embeddings), expectedCount)
	}

	embeddings := make([][]float32, len(response.Embeddings))
	for idx, item := range response.Embeddings {
		embedding, err := base.DecodeFloat32ArrayBytes(item)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d: %w", idx, err)
		}
		embeddings[idx] = embedding
	}
	return embeddings, nil
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}

	var apiKey string
	if e.cfg.GetAPIKey != nil {
		apiKey = e.cfg.GetAPIKey()
	}
	var baseURL string
	if e.cfg.GetBaseURL != nil {
		baseURL = e.cfg.GetBaseURL()
	}
	if baseURL == "" {
		return nil, fmt.Errorf("base URL is not configured for TiDB Cloud Inference")
	}

	var billingID string
	if e.cfg.GetBillingID != nil {
		billingID = e.cfg.GetBillingID()
	}
	if billingID == "" {
		billingID = "default_billing_id"
	}

	fullURL, err := embeddingsEndpoint(baseURL, billingID)
	if err != nil {
		return nil, err
	}

	headers := make(http.Header)
	// TiDB Cloud Free may allow anonymous requests when no API key is configured.
	if apiKey != "" {
		headers.Set("Authorization", "Bearer "+apiKey)
	}

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "TiDB Cloud Inference",
		Client:   &e.client,
		Endpoint: fullURL,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"model": model,
			"texts": texts,
		}, opts),
		Headers:              headers,
		MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
		Secrets:              []string{apiKey},
		DecodeErrorMessage:   decodeErrorMessage,
		DecodeEmbeddings:     decodeEmbeddings,
	})
}
