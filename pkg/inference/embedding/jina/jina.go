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
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for JinaEmbedder.
// GetBaseURL returns the complete Jina AI embeddings endpoint. An empty value
// uses DefaultAPIBaseURL.
type EmbedderConfig base.APIKeyProviderConfig

// NewJinaEmbedder creates a new JinaEmbedder instance with the provided configuration.
func NewJinaEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    base.APIKeyProviderConfig(cfg).WithDefaults(),
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
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for JinaAI"))
	if err != nil {
		return nil, err
	}
	endpoint, err := embeddingsEndpoint(e.cfg.ConfiguredBaseURL())
	if err != nil {
		return nil, err
	}

	statusCode, body, err := base.PostJSON(ctx, &e.client, "JinaAI", endpoint, base.JSONFieldsWithOptions(map[string]any{
		"model":          model,
		"input":          texts,
		"embedding_type": "base64",
	}, opts), http.Header{
		"Authorization": {"Bearer " + apiKey},
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
	return base.DecodeIndexedBase64Embeddings(respObj.Data, len(texts))
}
