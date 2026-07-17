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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultAPIBaseURL is the default base URL (without the /embeddings suffix) for OpenAI embeddings API.
	DefaultAPIBaseURL = "https://api.openai.com/v1"
)

// Embedder is for OpenAI embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for OpenAIEmbedder.
type EmbedderConfig struct {
	GetAPIKey func() string
	// GetBaseURL returns an OpenAI-compatible API base URL. A trailing
	// /embeddings path is optional and is normalized by the embedder.
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	ErrUnauthorized  error // The error to return when API key is invalid
}

// NewOpenAIEmbedder creates a new OpenAIEmbedder instance with the provided configuration.
func NewOpenAIEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
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
	var apiKey string
	if e.cfg.GetAPIKey != nil {
		apiKey = e.cfg.GetAPIKey()
	}
	if apiKey == "" {
		if e.cfg.ErrMissingAPIKey != nil {
			return nil, e.cfg.ErrMissingAPIKey
		}
		return nil, fmt.Errorf("API key is not configured for OpenAI")
	}
	baseURL := DefaultAPIBaseURL
	if e.cfg.GetBaseURL != nil {
		if configured := strings.TrimSpace(e.cfg.GetBaseURL()); configured != "" {
			baseURL = configured
		}
	}
	endpoint := strings.TrimRight(baseURL, "/")
	if !strings.HasSuffix(endpoint, "/embeddings") {
		endpoint += "/embeddings"
	}

	jsonData, err := base.MarshalJSONWithOptions(map[string]any{
		"model":           model,
		"input":           texts,
		"encoding_format": "base64",
	}, opts)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("OpenAI API request failed",
			zap.Int("status", resp.StatusCode),
			zap.String("body", base.SanitizeErrorBodyForLog(body)),
		)
		if resp.StatusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("OpenAI returns status unauthorized, check API key")
		}
		// Try to unmarshal an error response if available
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error.Message != "" {
			return nil, fmt.Errorf("OpenAI: %s", errResp.Error.Message)
		}
		return nil, fmt.Errorf("OpenAI: status code %d", resp.StatusCode)
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
		e, err := base.DecodeFloat32ArrayBytes(item.Embedding)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d", item.Index)
		}
		embeddings[item.Index] = e
	}
	return embeddings, nil
}
