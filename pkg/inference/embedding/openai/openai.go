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
	"net/url"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultAPIBaseURL is the default base URL (without the /embeddings suffix) for OpenAI embeddings API.
	DefaultAPIBaseURL = "https://api.openai.com/v1"
	// DefaultMaxResponseBodyBytes bounds memory used to read an OpenAI-compatible response.
	DefaultMaxResponseBodyBytes int64 = 32 << 20
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
	GetBaseURL func() string
	// ErrMissingAPIKey optionally overrides the default missing-key error so
	// callers can include deployment-specific configuration guidance.
	ErrMissingAPIKey error
	// ErrUnauthorized optionally overrides the default unauthorized error so
	// callers can include deployment-specific configuration guidance.
	ErrUnauthorized error
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// NewOpenAIEmbedder creates a new OpenAIEmbedder instance with the provided configuration.
func NewOpenAIEmbedder(cfg EmbedderConfig) *Embedder {
	if cfg.MaxResponseBodyBytes <= 0 {
		cfg.MaxResponseBodyBytes = DefaultMaxResponseBodyBytes
	}
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg,
	}
}

func readResponseBody(reader io.Reader, maxBytes int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response body exceeds maximum size of %d bytes", maxBytes)
	}
	return body, nil
}

// embeddingsEndpoint resolves an OpenAI-compatible base URL to the embeddings endpoint.
// The OpenAI API defines the endpoint as POST /v1/embeddings:
// https://platform.openai.com/docs/api-reference/embeddings/create
// For compatibility with existing callers, baseURL may already end in /embeddings.
func embeddingsEndpoint(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid OpenAI API base URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid OpenAI API base URL: absolute URL is required")
	}

	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	if !strings.HasSuffix(escapedPath, "/embeddings") {
		escapedPath += "/embeddings"
	}
	path, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", fmt.Errorf("invalid OpenAI API base URL path: %w", err)
	}
	u.Path = path
	u.RawPath = escapedPath
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
	endpoint, err := embeddingsEndpoint(baseURL)
	if err != nil {
		return nil, err
	}

	jsonData, err := json.Marshal(base.JSONFieldsWithOptions(map[string]any{
		"model":           model,
		"input":           texts,
		"encoding_format": "base64",
	}, opts))
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

	body, err := readResponseBody(resp.Body, e.cfg.MaxResponseBodyBytes)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		message := ""
		var parseErr error
		if err := json.Unmarshal(body, &errResp); err != nil {
			parseErr = err
		} else if errResp.Error.Message != "" {
			message = base.SanitizeErrorText(errResp.Error.Message, apiKey)
		}

		logFields := []zap.Field{zap.Int("status", resp.StatusCode)}
		if message != "" {
			logFields = append(logFields, zap.String("message", message))
		}
		if parseErr != nil {
			logFields = append(logFields, zap.String("parse_error", base.SanitizeErrorText(parseErr.Error(), apiKey)))
		}
		logutil.BgLogger().Error("OpenAI API request failed",
			logFields...,
		)
		if resp.StatusCode == http.StatusUnauthorized {
			if e.cfg.ErrUnauthorized != nil {
				return nil, e.cfg.ErrUnauthorized
			}
			return nil, fmt.Errorf("OpenAI returns status unauthorized, check API key")
		}
		if message != "" {
			return nil, fmt.Errorf("OpenAI: %s", message)
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
			return nil, fmt.Errorf("failed to decode embedding for index %d: %w", item.Index, err)
		}
		embeddings[item.Index] = e
	}
	return embeddings, nil
}
