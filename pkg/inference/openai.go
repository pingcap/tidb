// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inference

import (
	"context"
	"net/url"
	"os"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

const (
	openAIAPIKeyEnv  = "OPENAI_API_KEY"
	openAIBaseURLEnv = "OPENAI_BASE_URL"
)

var openAIOptionKeys = map[string]struct{}{
	"dimensions": {},
	"user":       {},
}

// OpenAIEmbedder calls the OpenAI embeddings API for EMBED_TEXT().
type OpenAIEmbedder struct{}

// NewOpenAIEmbedder creates an embedder backed by the OpenAI embeddings API.
func NewOpenAIEmbedder() *OpenAIEmbedder {
	return &OpenAIEmbedder{}
}

// CreateEmbeddings implements Embedder.
func (o *OpenAIEmbedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	client, err := newOpenAIClientFromEnv()
	if err != nil {
		return nil, err
	}

	params, err := buildOpenAIEmbeddingParams(model, texts, opts)
	if err != nil {
		return nil, err
	}

	resp, err := client.Embeddings.New(ctx, params)
	if err != nil {
		return nil, err
	}

	if len(resp.Data) != len(texts) {
		return nil, errors.Errorf("openai provider returned %d embeddings for %d inputs", len(resp.Data), len(texts))
	}

	embeddings := make([][]float32, len(texts))
	for _, embedding := range resp.Data {
		if embedding.Index < 0 || int(embedding.Index) >= len(texts) {
			return nil, errors.Errorf("openai provider returned invalid embedding index %d", embedding.Index)
		}
		vector := make([]float32, len(embedding.Embedding))
		for i, value := range embedding.Embedding {
			vector[i] = float32(value)
		}
		embeddings[embedding.Index] = vector
	}
	for i, vector := range embeddings {
		if vector == nil {
			return nil, errors.Errorf("openai provider omitted embedding for input index %d", i)
		}
	}
	return embeddings, nil
}

func newOpenAIClientFromEnv() (*openai.Client, error) {
	apiKey := strings.TrimSpace(os.Getenv(openAIAPIKeyEnv))
	if apiKey == "" {
		return nil, errors.Errorf("%s is not set for EMBED_TEXT openai provider", openAIAPIKeyEnv)
	}

	clientOpts := []option.RequestOption{option.WithAPIKey(apiKey)}
	baseURL, err := resolveOpenAIBaseURL()
	if err != nil {
		return nil, err
	}
	clientOpts = append(clientOpts, option.WithBaseURL(baseURL))
	return openai.NewClient(clientOpts...), nil
}

func resolveOpenAIBaseURL() (string, error) {
	if baseURL := strings.TrimSpace(vardef.EmbedOpenAIAPIBase.Load()); baseURL != "" {
		return prepareOpenAIClientBaseURL(baseURL), nil
	}

	baseURL := strings.TrimSpace(os.Getenv(openAIBaseURLEnv))
	if baseURL == "" {
		return vardef.DefTiDBExpEmbedOpenAIAPIBase, nil
	}

	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil {
		return "", errors.Annotatef(err, "invalid %s %q", openAIBaseURLEnv, baseURL)
	}
	if !parsedBaseURL.IsAbs() || parsedBaseURL.Host == "" {
		return "", errors.Errorf("invalid %s %q", openAIBaseURLEnv, baseURL)
	}
	return prepareOpenAIClientBaseURL(parsedBaseURL.String()), nil
}

func prepareOpenAIClientBaseURL(baseURL string) string {
	baseURL = strings.TrimSuffix(strings.TrimRight(baseURL, "/"), "/embeddings")
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	return baseURL
}

func buildOpenAIEmbeddingParams(model string, texts []string, opts map[string]any) (openai.EmbeddingNewParams, error) {
	for opt := range opts {
		if _, ok := openAIOptionKeys[opt]; !ok {
			return openai.EmbeddingNewParams{}, errors.Errorf("unknown option %s", opt)
		}
	}

	params := openai.EmbeddingNewParams{
		Input:          openai.F[openai.EmbeddingNewParamsInputUnion](openai.EmbeddingNewParamsInputArrayOfStrings(texts)),
		Model:          openai.F(model),
		EncodingFormat: openai.F(openai.EmbeddingNewParamsEncodingFormatFloat),
	}

	if value, ok := opts["dimensions"]; ok {
		dimensions, err := asPositiveInt64(value)
		if err != nil {
			return openai.EmbeddingNewParams{}, errors.Annotate(err, "invalid type for 'dimensions' option")
		}
		params.Dimensions = openai.F(dimensions)
	}

	if value, ok := opts["user"]; ok {
		user, ok := value.(string)
		if !ok {
			return openai.EmbeddingNewParams{}, errors.Errorf("invalid type for 'user' option: %T", value)
		}
		user = strings.TrimSpace(user)
		if user == "" {
			return openai.EmbeddingNewParams{}, errors.New("user option cannot be empty")
		}
		params.User = openai.F(user)
	}

	return params, nil
}

func asPositiveInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		if v <= 0 {
			return 0, errors.New("must be greater than 0")
		}
		return int64(v), nil
	case int64:
		if v <= 0 {
			return 0, errors.New("must be greater than 0")
		}
		return v, nil
	case float64:
		if v <= 0 || float64(int64(v)) != v {
			return 0, errors.New("must be a positive integer")
		}
		return int64(v), nil
	default:
		return 0, errors.Errorf("got %T", value)
	}
}
