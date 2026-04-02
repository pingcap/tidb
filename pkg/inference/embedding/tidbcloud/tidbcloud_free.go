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

// Embedder is for TiDB Cloud Free embeddings.
type Embedder struct {
	client http.Client
	cfg    EmbedderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for TiDBCloudFreeEmbedder.
type EmbedderConfig struct {
	GetBillingID func() string
	GetAPIKey    func() string
	GetBaseURL   func() string
}

// NewTiDBCloudFreeEmbedder creates a new TiDBCloudFreeEmbedder instance with the provided configuration.
func NewTiDBCloudFreeEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{},
		cfg:    cfg,
	}
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

	req := Request{
		Model:        model,
		Texts:        texts,
		OtherOptions: opts,
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

	fullURL := fmt.Sprintf("%s/api/v1/inference/embeddings/%s", baseURL, billingID)

	jsonData, err := json.Marshal(req)
	if err != nil {
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
			zap.Error(err))
		return nil, fmt.Errorf("unexpected marshal request error")
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", fullURL, bytes.NewBuffer(jsonData))
	if err != nil {
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
			zap.Error(err))
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to request TiDB Cloud Inference Service")
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
			zap.Error(err))
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to request TiDB Cloud Inference Service")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
			zap.Error(err))
		// Do not return error directly to users to avoid exposing URLs
		return nil, fmt.Errorf("failed to read from TiDB Cloud Inference Service")
	}

	if resp.StatusCode != http.StatusOK {
		logutil.BgLogger().Error("TiDB Cloud Inference API request failed",
			zap.Int("status", resp.StatusCode),
			zap.String("body", string(body)))
		// Try to unmarshal an error response if available
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err == nil && errResp.Error != "" {
			return nil, fmt.Errorf("TiDB Cloud Inference: %s", errResp.Error)
		}
		return nil, fmt.Errorf("TiDB Cloud Inference: status code %d", resp.StatusCode)
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
		e, err := base.DecodeFloat32ArrayBytes(item)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d", idx)
		}
		embeddings[idx] = e
	}
	return embeddings, nil
}
