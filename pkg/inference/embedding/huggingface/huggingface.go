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
	"bytes"
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

const (
	// DefaultAPIBaseURL is the default base URL for HuggingFace inference API.
	DefaultAPIBaseURL = "https://router.huggingface.co/hf-inference"
	// DefaultMaxResponseBodyBytes bounds memory used to read a HuggingFace response.
	DefaultMaxResponseBodyBytes int64 = base.DefaultMaxResponseBodyBytes
)

// Embedder is for HuggingFace embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for HuggingFaceEmbedder.
type EmbedderConfig struct {
	GetAPIKey func() string
	// GetBaseURL returns the inference API base. The embedder appends
	// /models/<model>/pipeline/feature-extraction. An empty value uses
	// DefaultAPIBaseURL.
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	ErrUnauthorized  error // The error to return when API key is invalid
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewHuggingFaceEmbedder creates a new HuggingFaceEmbedder instance with the provided configuration.
func NewHuggingFaceEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = DefaultMaxResponseBodyBytes
	}
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
}

func featureExtractionEndpoint(configured, model string) (string, error) {
	baseURL := strings.TrimSpace(configured)
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", base.NewRedactedError("invalid HuggingFace API base URL", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid HuggingFace API base URL: absolute HTTP(S) URL is required")
	}
	modelParts := strings.Split(model, "/")
	for i := range modelParts {
		modelParts[i] = escapePathSegment(modelParts[i])
	}
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/models/" + strings.Join(modelParts, "/") + "/pipeline/feature-extraction"
	path, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", base.NewRedactedError("invalid HuggingFace API base URL path", err)
	}
	u.Path = path
	u.RawPath = escapedPath
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

	var apiKey string
	if e.cfg.GetAPIKey != nil {
		apiKey = e.cfg.GetAPIKey()
	}
	if apiKey == "" {
		if e.cfg.ErrMissingAPIKey != nil {
			return nil, e.cfg.ErrMissingAPIKey
		}
		return nil, fmt.Errorf("API key is not configured for HuggingFace")
	}

	var configuredBaseURL string
	if e.cfg.GetBaseURL != nil {
		configuredBaseURL = e.cfg.GetBaseURL()
	}
	endpoint, err := featureExtractionEndpoint(configuredBaseURL, model)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(base.JSONFieldsWithOptions(map[string]any{
		"inputs": texts,
	}, opts))
	if err != nil {
		return nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, base.NewProviderRequestError(ctx, "HuggingFace", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return nil, base.NewProviderRequestError(ctx, "HuggingFace", err)
	}
	defer resp.Body.Close()

	body, err := base.ReadResponseBody(resp.Body, e.cfg.MaxResponseBodyBytes)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
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
		logutil.BgLogger().Error("HuggingFace API request failed", logFields...)

		if resp.StatusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("HuggingFace returns status unauthorized, check API key")
		}

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("HuggingFace model '%s' does not exist or is not available", model)
		}

		if message != "" {
			return nil, fmt.Errorf("HuggingFace: %s", message)
		}

		return nil, fmt.Errorf("HuggingFace: status code %d", resp.StatusCode)
	}

	var embeddings Response
	if err := json.Unmarshal(body, &embeddings); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}

	if len(embeddings) != len(texts) {
		return nil, fmt.Errorf("response data length %d does not match input texts length %d", len(embeddings), len(texts))
	}

	return embeddings, nil
}
