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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DefaultAPIBaseURL is the default endpoint URL for the Jina AI embeddings API.
const DefaultAPIBaseURL = "https://api.jina.ai/v1/embeddings"

// Embedder is for JinaAI embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for JinaEmbedder.
type EmbedderConfig struct {
	GetAPIKey func() string
	// GetBaseURL returns the complete Jina AI embeddings endpoint. An empty
	// value uses DefaultAPIBaseURL.
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	ErrUnauthorized  error // The error to return when API key is invalid
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use base.DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewJinaEmbedder creates a new JinaEmbedder instance with the provided configuration.
func NewJinaEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = base.DefaultMaxResponseBodyBytes
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
	u, err := base.ParseHTTPURL(endpoint, "Jina AI API base URL")
	if err != nil {
		return "", err
	}
	return u.String(), nil
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
	var apiKey string
	if e.cfg.GetAPIKey != nil {
		apiKey = e.cfg.GetAPIKey()
	}
	if apiKey == "" {
		if e.cfg.ErrMissingAPIKey != nil {
			return nil, e.cfg.ErrMissingAPIKey
		}
		return nil, fmt.Errorf("API key is not configured for JinaAI")
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
		"model":          model,
		"input":          texts,
		"embedding_type": "base64",
	}, opts))
	if err != nil {
		return nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}
	httpReq, err := base.NewJSONRequest(ctx, "JinaAI", endpoint, jsonData)
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	statusCode, body, err := base.DoRequest(ctx, &e.client, "JinaAI", httpReq, e.cfg.MaxResponseBodyBytes)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		var errResp ErrorResponse
		message := ""
		var parseErr error
		if err := json.Unmarshal(body, &errResp); err != nil {
			parseErr = err
		} else if errResp.Detail != "" {
			message = base.SanitizeErrorText(errResp.Detail, apiKey)
		}
		logFields := []zap.Field{zap.Int("status", statusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("JinaAI API request failed", logFields...)
		if statusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("JinaAI returns status unauthorized, check API key")
		}
		return nil, base.NewProviderResponseError("JinaAI", statusCode, message)
	}

	var respObj Response
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(respObj.Data) != len(texts) {
		return nil, fmt.Errorf("response data length %d does not match input texts length %d", len(respObj.Data), len(texts))
	}
	embeddings := make([][]float32, len(respObj.Data))
	for _, item := range respObj.Data {
		if item.Index < 0 || item.Index >= len(texts) {
			return nil, fmt.Errorf("response data index %d is out of range [0, %d)", item.Index, len(texts))
		}
		if embeddings[item.Index] != nil {
			return nil, fmt.Errorf("response data contains duplicate index %d", item.Index)
		}
		// item.Embedding is []byte. During JSON unmarshal,
		// it is already base64 decoded by Golang from base64.
		embedding, err := base.DecodeFloat32ArrayBytes(item.Embedding)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d: %w", item.Index, err)
		}
		embeddings[item.Index] = embedding
	}
	return embeddings, nil
}
