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

package gemini

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

// DefaultAPIBaseURL is the default base URL for the Gemini embeddings API.
const DefaultAPIBaseURL = "https://generativelanguage.googleapis.com/v1beta/models"

// Embedder is for Gemini embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for GeminiEmbedder.
type EmbedderConfig struct {
	GetAPIKey func() string
	// GetBaseURL returns the API base ending before the model name. The
	// embedder appends /<model>:batchEmbedContents. An empty value uses
	// DefaultAPIBaseURL.
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use base.DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewGeminiEmbedder creates a new GeminiEmbedder instance with the provided configuration.
func NewGeminiEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = base.DefaultMaxResponseBodyBytes
	}
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
}

func batchEmbeddingsEndpoint(configured, model string) (string, error) {
	baseURL := strings.TrimSpace(configured)
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}
	u, err := base.ParseHTTPURL(baseURL, "Gemini API base URL")
	if err != nil {
		return "", err
	}
	// Gemini batch embeddings append /<model>:batchEmbedContents to the models
	// base. See https://ai.google.dev/api/rest/v1beta/models/batchEmbedContents.
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/" + url.PathEscape(model) + ":batchEmbedContents"
	if err := base.SetEscapedURLPath(u, escapedPath, "Gemini API base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://ai.google.dev/api/rest/v1beta/models/batchEmbedContents
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}

	requests := make([]map[string]any, len(texts))
	for i, text := range texts {
		requests[i] = base.JSONFieldsWithOptions(map[string]any{
			"model": fmt.Sprintf("models/%s", model),
			"content": map[string]any{
				"parts": []map[string]string{{"text": text}},
			},
		}, opts)
	}

	var apiKey string
	if e.cfg.GetAPIKey != nil {
		apiKey = e.cfg.GetAPIKey()
	}
	if apiKey == "" {
		if e.cfg.ErrMissingAPIKey != nil {
			return nil, e.cfg.ErrMissingAPIKey
		}
		return nil, fmt.Errorf("API key is not configured for Gemini")
	}

	var configuredBaseURL string
	if e.cfg.GetBaseURL != nil {
		configuredBaseURL = e.cfg.GetBaseURL()
	}
	fullURL, err := batchEmbeddingsEndpoint(configuredBaseURL, model)
	if err != nil {
		return nil, err
	}

	statusCode, body, err := base.PostJSON(ctx, &e.client, "Gemini", fullURL, map[string]any{
		"requests": requests,
	}, http.Header{
		"x-goog-api-key": {apiKey},
	}, e.cfg.MaxResponseBodyBytes)
	if err != nil {
		return nil, err
	}

	if statusCode != http.StatusOK {
		var errResp ErrorResponse
		message := ""
		var parseErr error
		if err := json.Unmarshal(body, &errResp); err != nil {
			parseErr = err
		} else if errResp.Error.Message != "" {
			message = base.SanitizeErrorText(errResp.Error.Message, apiKey)
		}
		logFields := []zap.Field{zap.Int("status", statusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("Gemini API request failed", logFields...)
		return nil, base.NewProviderResponseError("Gemini", statusCode, message)
	}

	var respObj BatchResponse
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}

	if len(respObj.Embeddings) != len(texts) {
		return nil, fmt.Errorf("response embeddings length %d does not match input texts length %d", len(respObj.Embeddings), len(texts))
	}

	embeddings := make([][]float32, len(respObj.Embeddings))
	for i, embedding := range respObj.Embeddings {
		embeddings[i] = embedding.Values
	}

	return embeddings, nil
}
