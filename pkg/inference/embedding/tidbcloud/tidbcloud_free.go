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
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/api/v1/inference/embeddings/" + escapePathSegment(billingID)
	if err := base.SetEscapedURLPath(u, escapedPath, "TiDB Cloud Inference base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

func escapePathSegment(segment string) string {
	escaped := url.PathEscape(segment)
	if escaped == "." {
		return "%2E"
	}
	if escaped == ".." {
		return "%2E%2E"
	}
	return escaped
}

func logRequestError(apiKey string, err error) {
	logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
		zap.String("error", base.SanitizeErrorText(err.Error(), apiKey)))
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
		logRequestError(apiKey, err)
		// Do not return error directly to users to avoid exposing URLs.
		return nil, fmt.Errorf("failed to request TiDB Cloud Inference Service")
	}

	jsonData, err := json.Marshal(base.JSONFieldsWithOptions(map[string]any{
		"model": model,
		"texts": texts,
	}, opts))
	if err != nil {
		logRequestError(apiKey, err)
		return nil, fmt.Errorf("unexpected marshal request error")
	}

	httpReq, err := base.NewJSONRequest(ctx, "TiDB Cloud Inference", fullURL, jsonData)
	if err != nil {
		logRequestError(apiKey, err)
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to request TiDB Cloud Inference Service")
	}

	// TiDB Cloud Free may allow anonymous requests when no API key is configured.
	if apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
	}

	resp, err := e.client.Do(httpReq)
	if err != nil {
		logRequestError(apiKey, base.NewProviderRequestError(ctx, "TiDB Cloud Inference", err))
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to request TiDB Cloud Inference Service")
	}
	defer resp.Body.Close()

	body, err := base.ReadResponseBody(resp.Body, e.cfg.MaxResponseBodyBytes)
	if err != nil {
		logRequestError(apiKey, err)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to read from TiDB Cloud Inference Service")
	}

	if resp.StatusCode != http.StatusOK {
		// Try to unmarshal an error response if available
		var errResp ErrorResponse
		message := ""
		var parseErr error
		if err := json.Unmarshal(body, &errResp); err != nil {
			parseErr = err
		} else if errResp.Error != "" {
			message = base.SanitizeErrorText(errResp.Error, apiKey)
		}
		logFields := []zap.Field{zap.Int("status", resp.StatusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed", logFields...)
		return nil, base.NewProviderResponseError("TiDB Cloud Inference", resp.StatusCode, message)
	}

	var respObj Response
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(respObj.Embeddings) != len(texts) {
		return nil, fmt.Errorf("response embeddings length %d does not match input texts length %d", len(respObj.Embeddings), len(texts))
	}

	embeddings := make([][]float32, len(respObj.Embeddings))
	for idx, item := range respObj.Embeddings {
		// item.Embedding is []byte. During JSON unmarshal,
		// it is already base64 decoded by Golang from base64.
		embedding, err := base.DecodeFloat32ArrayBytes(item)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d: %w", idx, err)
		}
		embeddings[idx] = embedding
	}
	return embeddings, nil
}
