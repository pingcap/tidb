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

package cohere

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
	// DefaultAPIBaseURL is the default endpoint URL for the Cohere embeddings API.
	DefaultAPIBaseURL = "https://api.cohere.com/v1/embed"
	// DefaultMaxResponseBodyBytes bounds memory used to read a Cohere response.
	DefaultMaxResponseBodyBytes int64 = base.DefaultMaxResponseBodyBytes
)

// Embedder is for Cohere embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for CohereEmbedder.
type EmbedderConfig struct {
	GetAPIKey func() string
	// GetBaseURL returns the complete Cohere embeddings endpoint. An empty
	// value uses DefaultAPIBaseURL.
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	ErrUnauthorized  error // The error to return when API key is invalid
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewCohereEmbedder creates a new CohereEmbedder instance with the provided configuration.
func NewCohereEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = DefaultMaxResponseBodyBytes
	}
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
}

func embeddingsEndpoint(configured string) (string, error) {
	endpoint := strings.TrimSpace(configured)
	if endpoint == "" {
		endpoint = DefaultAPIBaseURL
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid Cohere API base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid Cohere API base URL: absolute HTTP(S) URL is required")
	}
	return u.String(), nil
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://docs.cohere.com/v1/reference/embed
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
		return nil, fmt.Errorf("API key is not configured for cohere")
	}
	var configuredBaseURL string
	if e.cfg.GetBaseURL != nil {
		configuredBaseURL = e.cfg.GetBaseURL()
	}
	endpoint, err := embeddingsEndpoint(configuredBaseURL)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(base.JSONFieldsWithOptions(map[string]any{
		"model": model,
		"texts": texts,
	}, opts))
	if err != nil {
		return nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return nil, err
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
		} else if errResp.Message != "" {
			message = base.SanitizeErrorText(errResp.Message, apiKey)
		}
		logFields := []zap.Field{zap.Int("status", resp.StatusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("Cohere API request failed", logFields...)
		if resp.StatusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("cohere returns status unauthorized, check API key")
		}
		if message != "" {
			return nil, fmt.Errorf("cohere: %s", message)
		}
		return nil, fmt.Errorf("cohere: status code %d", resp.StatusCode)
	}

	var respObj Response
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(respObj.Embeddings) != len(texts) {
		return nil, fmt.Errorf("response embeddings length %d does not match input texts length %d", len(respObj.Embeddings), len(texts))
	}

	return respObj.Embeddings, nil
}
