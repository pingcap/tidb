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

// DefaultAPIBaseURL is the default base URL for HuggingFace inference API.
const DefaultAPIBaseURL = "https://router.huggingface.co/hf-inference"

// Embedder is for HuggingFace embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for HuggingFaceEmbedder.
// GetBaseURL returns the inference API base. The embedder appends
// /models/<model>/pipeline/feature-extraction. An empty value uses
// DefaultAPIBaseURL.
type EmbedderConfig base.APIKeyProviderConfig

// NewHuggingFaceEmbedder creates a new HuggingFaceEmbedder instance with the provided configuration.
func NewHuggingFaceEmbedder(cfg EmbedderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    base.APIKeyProviderConfig(cfg).WithDefaults(),
	}
}

func featureExtractionEndpoint(configured, model string) (string, error) {
	baseURL := strings.TrimSpace(configured)
	if baseURL == "" {
		baseURL = DefaultAPIBaseURL
	}
	u, err := base.ParseHTTPURL(baseURL, "HuggingFace API base URL")
	if err != nil {
		return "", err
	}
	modelParts := strings.Split(model, "/")
	for i := range modelParts {
		modelParts[i] = escapePathSegment(modelParts[i])
	}
	escapedPath := strings.TrimRight(u.EscapedPath(), "/") + "/models/" + strings.Join(modelParts, "/") + "/pipeline/feature-extraction"
	if err := base.SetEscapedURLPath(u, escapedPath, "HuggingFace API base URL path"); err != nil {
		return "", err
	}
	return u.String(), nil
}

func escapePathSegment(segment string) string {
	escaped := url.PathEscape(segment)
	// url.PathEscape intentionally leaves the complete dot segments "." and
	// ".." unchanged. Escape them explicitly so intermediaries cannot remove
	// or normalize model path segments according to RFC 3986 section 5.2.4.
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

	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for HuggingFace"))
	if err != nil {
		return nil, err
	}

	endpoint, err := featureExtractionEndpoint(e.cfg.ConfiguredBaseURL(), model)
	if err != nil {
		return nil, err
	}

	statusCode, body, err := base.PostJSON(ctx, &e.client, "HuggingFace", endpoint, base.JSONFieldsWithOptions(map[string]any{
		"inputs": texts,
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
		} else if errResp.Error != "" {
			message = base.SanitizeErrorText(errResp.Error, apiKey)
		}
		logFields := []zap.Field{zap.Int("status", statusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("HuggingFace API request failed", logFields...)

		if statusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("HuggingFace returns status unauthorized, check API key")
		}

		if statusCode == http.StatusNotFound {
			return nil, fmt.Errorf("HuggingFace model '%s' does not exist or is not available", model)
		}

		return nil, base.NewProviderResponseError("HuggingFace", statusCode, message)
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
