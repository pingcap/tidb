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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/go-json-experiment/json"
	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultAPIBaseURL is the default base URL for Gemini embeddings API.
	DefaultAPIBaseURL = "https://generativelanguage.googleapis.com/v1beta/models"
)

// Embedder is for Gemini embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for GeminiEmbedder.
type EmbedderConfig struct {
	GetAPIKey        func() string
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
}

// NewGeminiEmbedder creates a new GeminiEmbedder instance with the provided configuration.
func NewGeminiEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{},
		cfg:    cfg,
	}
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

	// Construct requests array
	requests := make([]Request, len(texts))
	for i, text := range texts {
		var r = &requests[i]
		r.Model = fmt.Sprintf("models/%s", model)
		r.Content.Parts = []struct {
			Text string `json:"text"`
		}{{Text: text}}
		r.OtherOptions = opts
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

	var baseURL string
	if e.cfg.GetBaseURL != nil {
		baseURL = e.cfg.GetBaseURL()
	}
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}

	// Construct the full URL with model and endpoint
	fullURL := fmt.Sprintf("%s/%s:batchEmbedContents", baseURL, model)

	jsonData, err := json.Marshal(BatchRequest{Requests: requests})
	if err != nil {
		return nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", fullURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-goog-api-key", apiKey)

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
		logutil.BgLogger().Error("Gemini API request failed",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error.Message != "" {
			return nil, fmt.Errorf("%s: %s", "Gemini", errResp.Error.Message)
		}
		return nil, fmt.Errorf("%s: status code %d", "Gemini", resp.StatusCode)
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
