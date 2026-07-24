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
)

// DefaultAPIBaseURL is the default endpoint URL for the NVIDIA NIM embeddings API.
const DefaultAPIBaseURL = "https://integrate.api.nvidia.com/v1/embeddings"

// Embedder is for NVIDIA NIM embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewNvidiaEmbedder creates a new NvidiaEmbedder instance with the provided configuration.
// cfg.GetBaseURL returns the complete NVIDIA NIM embeddings endpoint; an empty
// value uses DefaultAPIBaseURL.
func NewNvidiaEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg.WithDefaults(),
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

func decodeErrorMessage(body []byte) (string, error) {
	// NVIDIA endpoints use different error schemas across hosted and
	// self-hosted versions, so accept all known message fields.
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	if response.Detail != "" {
		return response.Detail, nil
	}
	if response.Message != "" {
		return response.Message, nil
	}
	return response.Error, nil
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(response.Data, expectedCount)
}

func (e *Embedder) unauthorizedError(statusCode int) error {
	if e.cfg.ErrUnauthorized != nil {
		return e.cfg.ErrUnauthorized
	}
	return fmt.Errorf("NVIDIA NIM returns status %s, check API key", strings.ToLower(http.StatusText(statusCode)))
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

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "NVIDIA NIM",
		Client:   &e.client,
		Endpoint: endpoint,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"model":           model,
			"input":           texts,
			"encoding_format": "base64",
		}, opts),
		Headers:              http.Header{"Authorization": {"Bearer " + apiKey}},
		MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
		Secrets:              []string{apiKey},
		DecodeErrorMessage:   decodeErrorMessage,
		StatusErrors: map[int]error{
			http.StatusUnauthorized: e.unauthorizedError(http.StatusUnauthorized),
			http.StatusForbidden:    e.unauthorizedError(http.StatusForbidden),
			http.StatusNotFound:     fmt.Errorf("NVIDIA NIM model '%s' does not exist or is not available", model),
		},
		DecodeEmbeddings: decodeEmbeddings,
	})
}
