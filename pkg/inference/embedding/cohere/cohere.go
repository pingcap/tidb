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

package cohere

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// DefaultAPIBaseURL is the default endpoint URL for the Cohere embeddings API.
const DefaultAPIBaseURL = "https://api.cohere.com/v1/embed"

// Embedder is for Cohere embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewCohereEmbedder creates a new CohereEmbedder instance with the provided configuration.
// cfg.GetBaseURL returns the complete Cohere embeddings endpoint; an empty
// value uses DefaultAPIBaseURL.
func NewCohereEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
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
	u, err := base.ParseHTTPURL(endpoint, "Cohere API base URL")
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func validateEmbeddingTypes(opts map[string]any) error {
	value, ok := opts["embedding_types"]
	if !ok {
		return nil
	}

	var embeddingTypes []string
	switch types := value.(type) {
	case []string:
		embeddingTypes = types
	case []any:
		embeddingTypes = make([]string, len(types))
		for i, item := range types {
			var ok bool
			embeddingTypes[i], ok = item.(string)
			if !ok {
				return fmt.Errorf(`Cohere embedding_types must be exactly ["float"]`)
			}
		}
	default:
		return fmt.Errorf(`Cohere embedding_types must be exactly ["float"]`)
	}

	if len(embeddingTypes) != 1 || embeddingTypes[0] != "float" {
		return fmt.Errorf(`Cohere embedding_types must be exactly ["float"]`)
	}
	return nil
}

func decodeEmbeddings(raw json.RawMessage) ([][]float32, error) {
	// When embedding_types is omitted, Cohere returns embeddings as an array.
	// When it is set, Cohere returns an object keyed by embedding type.
	// See https://docs.cohere.com/v1/reference/embed.
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("Cohere response does not contain embeddings")
	}

	switch trimmed[0] {
	case '[':
		var embeddings [][]float32
		if err := json.Unmarshal(trimmed, &embeddings); err != nil {
			return nil, fmt.Errorf("unexpected unmarshal response embeddings error: %w", err)
		}
		return embeddings, nil
	case '{':
		var embeddingsByType struct {
			Float [][]float32 `json:"float"`
		}
		if err := json.Unmarshal(trimmed, &embeddingsByType); err != nil {
			return nil, fmt.Errorf("unexpected unmarshal typed response embeddings error: %w", err)
		}
		if embeddingsByType.Float == nil {
			return nil, fmt.Errorf("Cohere response does not contain float embeddings")
		}
		return embeddingsByType.Float, nil
	default:
		return nil, fmt.Errorf("unexpected Cohere embeddings response format")
	}
}

func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	return response.Message, nil
}

func decodeResponse(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	embeddings, err := decodeEmbeddings(response.Embeddings)
	if err != nil {
		return nil, err
	}
	if len(embeddings) != expectedCount {
		return nil, fmt.Errorf("response embeddings length %d does not match input texts length %d", len(embeddings), expectedCount)
	}
	return embeddings, nil
}

func (e *Embedder) unauthorizedError() error {
	if e.cfg.ErrUnauthorized != nil {
		return e.cfg.ErrUnauthorized
	}
	return fmt.Errorf("cohere returns status unauthorized, check API key")
}

// CreateEmbeddings creates embeddings for the given texts using the specified model.
// Cohere v3 and newer models require opts to include an "input_type" appropriate
// for the current call, such as "search_document" or "search_query".
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://docs.cohere.com/v1/reference/embed
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}
	if err := validateEmbeddingTypes(opts); err != nil {
		return nil, err
	}

	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for cohere"))
	if err != nil {
		return nil, err
	}
	endpoint, err := embeddingsEndpoint(e.cfg.ConfiguredBaseURL())
	if err != nil {
		return nil, err
	}

	return base.ExecuteJSONEmbeddingCall(ctx, len(texts), base.JSONEmbeddingCall{
		Provider: "Cohere",
		Client:   &e.client,
		Endpoint: endpoint,
		Payload: base.JSONFieldsWithOptions(map[string]any{
			"model": model,
			"texts": texts,
		}, opts),
		Headers:              http.Header{"Authorization": {"Bearer " + apiKey}},
		MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
		Secrets:              []string{apiKey},
		DecodeErrorMessage:   decodeErrorMessage,
		StatusErrors: map[int]error{
			http.StatusUnauthorized: e.unauthorizedError(),
		},
		DecodeEmbeddings: decodeResponse,
	})
}
