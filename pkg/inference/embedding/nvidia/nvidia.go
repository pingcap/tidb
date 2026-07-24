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

package nvidia

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

// DefaultAPIBaseURL is the default endpoint URL for the NVIDIA NIM embeddings API.
const DefaultAPIBaseURL = "https://integrate.api.nvidia.com/v1/embeddings"

// Embedder is for NVIDIA NIM embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// EmbedderConfig holds the configuration for NvidiaEmbedder.
// GetBaseURL returns the complete NVIDIA NIM embeddings endpoint. An empty
// value uses DefaultAPIBaseURL.
type EmbedderConfig base.APIKeyProviderConfig

// NewNvidiaEmbedder creates a new NvidiaEmbedder instance with the provided configuration.
func NewNvidiaEmbedder(cfg EmbedderConfig) *Embedder {
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
	u, err := base.ParseHTTPURL(endpoint, "NVIDIA NIM API base URL")
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func validateEmbeddingType(opts map[string]any) error {
	value, ok := opts["embedding_type"]
	if !ok {
		return nil
	}
	embeddingType, ok := value.(string)
	if !ok || embeddingType != "float" {
		return fmt.Errorf(`NVIDIA NIM embedding_type must be "float"`)
	}
	return nil
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://docs.api.nvidia.com/nim/reference/baai-bge-m3-invoke
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}
	if err := validateEmbeddingType(opts); err != nil {
		return nil, err
	}
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for NVIDIA NIM"))
	if err != nil {
		return nil, err
	}
	endpoint, err := embeddingsEndpoint(e.cfg.ConfiguredBaseURL())
	if err != nil {
		return nil, err
	}

	statusCode, body, err := base.PostJSON(ctx, &e.client, "NVIDIA NIM", endpoint, base.JSONFieldsWithOptions(map[string]any{
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
		// NVIDIA endpoints use different error schemas across hosted and
		// self-hosted versions, so accept all known message fields.
		var errResp ErrorResponse
		message := ""
		var parseErr error
		if err := json.Unmarshal(body, &errResp); err != nil {
			parseErr = err
		} else if errResp.Detail != "" {
			message = base.SanitizeErrorText(errResp.Detail, apiKey)
		} else if errResp.Message != "" {
			message = base.SanitizeErrorText(errResp.Message, apiKey)
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
		logutil.BgLogger().Error("NVIDIA NIM API request failed", logFields...)
		if statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("NVIDIA NIM returns status %s, check API key", strings.ToLower(http.StatusText(statusCode)))
		}
		if statusCode == http.StatusNotFound {
			return nil, fmt.Errorf("NVIDIA NIM model '%s' does not exist or is not available", model)
		}
		return nil, base.NewProviderResponseError("NVIDIA NIM", statusCode, message)
	}

	var respObj Response
	if err := json.Unmarshal(body, &respObj); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(respObj.Data, len(texts))
}
