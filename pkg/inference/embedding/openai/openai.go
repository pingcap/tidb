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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// DefaultAPIBaseURL is the default base URL (without the /embeddings suffix) for OpenAI embeddings API.
const DefaultAPIBaseURL = "https://api.openai.com/v1"

// Embedder is for OpenAI embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for OpenAIEmbedder.
// GetBaseURL returns an OpenAI-compatible API base URL. A trailing /embeddings
// path is optional and is normalized by the embedder.
type EmbedderConfig base.APIKeyProviderConfig

// NewOpenAIEmbedder creates a new OpenAIEmbedder instance with the provided configuration.
func NewOpenAIEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    base.APIKeyProviderConfig(cfg).WithDefaults(),
	}
}

// embeddingsEndpoint resolves an OpenAI-compatible base URL to the embeddings endpoint.
// The OpenAI API defines the endpoint as POST /v1/embeddings:
// https://platform.openai.com/docs/api-reference/embeddings/create
// For compatibility with existing callers, baseURL may already end in /embeddings.
func embeddingsEndpoint(baseURL string) (string, error) {
	u, err := base.ParseHTTPURL(baseURL, "OpenAI API base URL")
	if err != nil {
		return "", err
	}

	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	if !strings.HasSuffix(escapedPath, "/embeddings") {
		escapedPath += "/embeddings"
	}
	if err := base.SetEscapedURLPath(u, escapedPath, "OpenAI API base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
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
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for OpenAI"))
	if err != nil {
		return nil, err
	}
	baseURL := DefaultAPIBaseURL
	if configured := strings.TrimSpace(e.cfg.ConfiguredBaseURL()); configured != "" {
		baseURL = configured
	}
	endpoint, err := embeddingsEndpoint(baseURL)
	if err != nil {
		return nil, err
	}

	statusCode, body, err := base.PostJSON(ctx, &e.client, "OpenAI", endpoint, base.JSONFieldsWithOptions(map[string]any{
		"model":           model,
		"input":           texts,
		"encoding_format": "base64",
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
		logutil.BgLogger().Error("OpenAI API request failed",
			logFields...,
		)
		if statusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("OpenAI returns status unauthorized, check API key")
		}
		return nil, base.NewProviderResponseError("OpenAI", statusCode, message)
	}

	var respObj Response
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(respObj.Data, len(texts))
}
