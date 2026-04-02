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
	"fmt"
	"io"
	"net/http"

	"github.com/go-json-experiment/json"
	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultAPIBaseURL is the default base URL for HuggingFace inference API.
	DefaultAPIBaseURL = "https://router.huggingface.co/hf-inference"
)

// Embedder is for HuggingFace embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for HuggingFaceEmbedder.
type EmbedderConfig struct {
	GetAPIKey        func() string
	GetBaseURL       func() string
	ErrMissingAPIKey error // The error to return when API key is missing
	ErrUnauthorized  error // The error to return when API key is invalid
}

// NewHuggingFaceEmbedder creates a new HuggingFaceEmbedder instance with the provided configuration.
func NewHuggingFaceEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{},
		cfg:    cfg,
	}
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

	req := Request{
		Inputs: texts,
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

	var baseURL string
	if e.cfg.GetBaseURL != nil {
		baseURL = e.cfg.GetBaseURL()
	}
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}

	// Construct the full URL with model path
	url := fmt.Sprintf("%s/models/%s/pipeline/feature-extraction", baseURL, model)

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
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
		logutil.BgLogger().Error("HuggingFace API request failed",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)),
		)

		if resp.StatusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("HuggingFace returns status unauthorized, check API key")
		}

		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("HuggingFace model '%s' does not exist or is not available", model)
		}

		// Try to unmarshal an error response if available
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return nil, fmt.Errorf("HuggingFace: %s", errResp.Error)
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
