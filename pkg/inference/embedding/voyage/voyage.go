// Copyright 2026 PingCAP, Inc.
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

package voyage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/inference/embedding/base"
)

// DefaultAPIBaseURL is the default base URL of the Voyage AI API. The adapter
// appends the path of the endpoint that serves the requested model.
const DefaultAPIBaseURL = "https://api.voyageai.com/v1"

const (
	embeddingsPath     = "embeddings"
	contextualizedPath = "contextualizedembeddings"
	inputTypeDocument  = "document"
	inputTypeQuery     = "query"
	// contextualizedChunkSize is the chunk_size sent with every auto-chunked
	// contextualized document request.
	contextualizedChunkSize = 32_000
)

// maxInputsPerRequest is the documented maximum length of the input list.
// See https://docs.voyageai.com/reference/embeddings-api.
const maxInputsPerRequest = 1000

// perInputTokenAllowance covers tokens a tokenizer may add around each input.
const perInputTokenAllowance = 2

var matryoshkaDimensions = []int{256, 512, 1024, 2048}

// modelSpec describes the request limits and output sizes of one Voyage
// embedding model.
type modelSpec struct {
	// maxRequestTokens is the documented ceiling on the total tokens of one
	// request. Zero means Voyage documents no ceiling for the model.
	maxRequestTokens int
	// outputDimensions lists the accepted output_dimension values. The first
	// value is the model's default dimension.
	outputDimensions []int
	// contextualized marks the contextualized embedding models, which are served
	// by a different endpoint and need an input_type.
	contextualized bool
}

// models lists the supported text and contextualized embedding models explicitly. A model that is
// not listed is rejected instead of being forwarded, because the per-request
// token ceiling cannot be enforced for it.
// See https://docs.voyageai.com/docs/embeddings and
// https://docs.voyageai.com/reference/embeddings-api.
var models = map[string]modelSpec{
	"voyage-4-large":   {maxRequestTokens: 120_000, outputDimensions: matryoshkaDimensions},
	"voyage-4":         {maxRequestTokens: 320_000, outputDimensions: matryoshkaDimensions},
	"voyage-4-lite":    {maxRequestTokens: 1_000_000, outputDimensions: matryoshkaDimensions},
	"voyage-code-4":    {outputDimensions: matryoshkaDimensions},
	"voyage-3-large":   {maxRequestTokens: 120_000, outputDimensions: matryoshkaDimensions},
	"voyage-3.5":       {maxRequestTokens: 320_000, outputDimensions: matryoshkaDimensions},
	"voyage-3.5-lite":  {maxRequestTokens: 1_000_000, outputDimensions: matryoshkaDimensions},
	"voyage-code-3":    {maxRequestTokens: 120_000, outputDimensions: matryoshkaDimensions},
	"voyage-finance-2": {maxRequestTokens: 120_000, outputDimensions: []int{1024}},
	"voyage-law-2":     {maxRequestTokens: 120_000, outputDimensions: []int{1024}},
	"voyage-context-4": {maxRequestTokens: 120_000, outputDimensions: matryoshkaDimensions, contextualized: true},
	"voyage-context-3": {maxRequestTokens: 120_000, outputDimensions: matryoshkaDimensions, contextualized: true},
}

func supportedModels() string {
	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

// Embedder is for Voyage AI text and contextualized embeddings.
type Embedder struct {
	client http.Client
	cfg    base.APIKeyProviderConfig
}

var _ base.Embedder = (*Embedder)(nil)

// NewVoyageEmbedder creates a new Voyage AI embedder with the provided configuration.
// cfg.GetBaseURL returns the Voyage AI API base URL, such as DefaultAPIBaseURL;
// an empty value uses DefaultAPIBaseURL.
func NewVoyageEmbedder(cfg base.APIKeyProviderConfig) *Embedder {
	return &Embedder{
		client: http.Client{Timeout: base.DefaultHTTPClientTimeout},
		cfg:    cfg.WithDefaults(),
	}
}

// endpointURL returns the URL of the endpoint at path below the configured API
// base URL.
func endpointURL(configured, path string) (string, error) {
	endpoint := strings.TrimSpace(configured)
	if endpoint == "" {
		endpoint = DefaultAPIBaseURL
	}
	u, err := base.ParseHTTPURL(endpoint, "Voyage AI API base URL")
	if err != nil {
		return "", err
	}
	return u.JoinPath(path).String(), nil
}

// decodeErrorMessage extracts the message from a Voyage error body, which is
// {"detail": "<text>"}. A non-string detail, such as a validation error list,
// is returned as compact JSON.
func decodeErrorMessage(body []byte) (string, error) {
	var response ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return "", err
	}
	trimmed := bytes.TrimSpace(response.Detail)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	var message string
	if err := json.Unmarshal(trimmed, &message); err == nil {
		return message, nil
	}
	return string(trimmed), nil
}

// requestOptions holds the validated Voyage-specific options of one call.
type requestOptions struct {
	inputType       string
	outputDimension int
	truncation      *bool
}

func (o requestOptions) fields() map[string]any {
	fields := map[string]any{}
	if o.inputType != "" {
		fields["input_type"] = o.inputType
	}
	if o.outputDimension != 0 {
		fields["output_dimension"] = o.outputDimension
	}
	if o.truncation != nil {
		fields["truncation"] = *o.truncation
	}
	return fields
}

func parseDimension(value any) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		if v != math.Trunc(v) || v < 0 || v > math.MaxInt32 {
			return 0, false
		}
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil || n < 0 || n > math.MaxInt32 {
			return 0, false
		}
		return int(n), true
	default:
		return 0, false
	}
}

// parseOptions validates opts against the model. Only options that the Voyage
// embeddings API defines and this adapter can decode are accepted; the
// adapter owns encoding_format and output_dtype.
func parseOptions(model string, spec modelSpec, opts map[string]any) (requestOptions, error) {
	var parsed requestOptions
	for key, value := range opts {
		switch key {
		case "input_type":
			inputType, ok := value.(string)
			if !ok || (inputType != inputTypeDocument && inputType != inputTypeQuery) {
				return requestOptions{}, fmt.Errorf(`Voyage AI option input_type must be "document" or "query"`)
			}
			parsed.inputType = inputType
		case "output_dimension":
			dimension, ok := parseDimension(value)
			if !ok || !containsInt(spec.outputDimensions, dimension) {
				return requestOptions{}, fmt.Errorf("Voyage AI option output_dimension must be one of %v for model %s", spec.outputDimensions, model)
			}
			parsed.outputDimension = dimension
		case "truncation":
			if spec.contextualized {
				return requestOptions{}, fmt.Errorf("Voyage AI option truncation is not supported for contextualized model %s", model)
			}
			truncation, ok := value.(bool)
			if !ok {
				return requestOptions{}, fmt.Errorf("Voyage AI option truncation must be a boolean")
			}
			parsed.truncation = &truncation
		default:
			supported := "input_type, output_dimension, truncation"
			if spec.contextualized {
				supported = "input_type, output_dimension"
			}
			return requestOptions{}, fmt.Errorf("Voyage AI option %q is not supported, supported options: %s", key, supported)
		}
	}
	if spec.contextualized && parsed.inputType == "" {
		return requestOptions{}, fmt.Errorf(`Voyage AI model %s requires option input_type ("document" or "query")`, model)
	}
	return parsed, nil
}

func containsInt(values []int, target int) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

// tokenUpperBound returns a conservative count of the tokens Voyage bills for
// text. Go has no official Voyage SDK and no Voyage tokenizer, so the count is
// not computed exactly: a token covers at least one byte of UTF-8 input, so the
// byte length bounds the token count, and a small allowance covers tokens the
// tokenizer adds around the input. Invalid UTF-8 is counted at three bytes per
// byte because JSON encoding replaces each such byte with U+FFFD. The bound
// only decides where a request is split; Voyage reports the real usage.
func tokenUpperBound(text string) int {
	if !utf8.ValidString(text) {
		return 3*len(text) + perInputTokenAllowance
	}
	return len(text) + perInputTokenAllowance
}

// splitRequests groups texts, in their original order, into consecutive
// requests that respect the input count limit and, when maxTokens is positive,
// the token ceiling. A text whose own bound exceeds maxTokens is sent alone and
// left to the model's context limit instead of being rejected here.
func splitRequests(texts []string, maxTokens int) [][]string {
	var requests [][]string
	start, tokens := 0, 0
	for i, text := range texts {
		bound := tokenUpperBound(text)
		if i > start && (i-start == maxInputsPerRequest || (maxTokens > 0 && tokens+bound > maxTokens)) {
			requests = append(requests, texts[start:i])
			start, tokens = i, 0
		}
		tokens += bound
	}
	if start < len(texts) {
		requests = append(requests, texts[start:])
	}
	return requests
}

func decodeEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	return base.DecodeIndexedBase64Embeddings(response.Data, expectedCount)
}

// decodeContextualizedEmbeddings decodes a contextualized response, which holds
// one list of chunk embeddings per input document. EMBED_TEXT-style callers need
// exactly one vector per text, so a document that Voyage split into several
// chunks is an error rather than a silently truncated or merged result.
func decodeContextualizedEmbeddings(body []byte, expectedCount int) ([][]float32, error) {
	var response ContextualizedResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("unexpected unmarshal response error: %w", err)
	}
	if len(response.Data) != expectedCount {
		return nil, fmt.Errorf("response data length %d does not match input texts length %d", len(response.Data), expectedCount)
	}
	embeddings := make([][]float32, expectedCount)
	for _, item := range response.Data {
		if item.Index < 0 || item.Index >= expectedCount {
			return nil, fmt.Errorf("response data index %d is out of range [0, %d)", item.Index, expectedCount)
		}
		if embeddings[item.Index] != nil {
			return nil, fmt.Errorf("response data contains duplicate index %d", item.Index)
		}
		if len(item.Data) != 1 {
			return nil, fmt.Errorf("input text %d was split into %d chunks; a text must fit in one chunk of %d tokens to produce a single embedding", item.Index, len(item.Data), contextualizedChunkSize)
		}
		decoded, err := base.DecodeIndexedBase64Embeddings(item.Data, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for input text %d: %w", item.Index, err)
		}
		embeddings[item.Index] = decoded[0]
	}
	return embeddings, nil
}

// requestPayload builds the JSON body for one request. Text models take a flat
// "input" list. Contextualized models take "inputs": in document mode each text
// is one document that Voyage chunks itself (enable_auto_chunking, which Voyage
// only accepts together with input_type "document"); in query mode each text is
// a single-element list and nothing is chunked.
func requestPayload(model string, spec modelSpec, input []string, opts requestOptions) map[string]any {
	fields := opts.fields()
	if !spec.contextualized {
		return base.JSONFieldsWithOptions(map[string]any{
			"model":           model,
			"input":           input,
			"encoding_format": "base64",
		}, fields)
	}
	fixed := map[string]any{
		"model":           model,
		"encoding_format": "base64",
	}
	if opts.inputType == inputTypeDocument {
		fixed["inputs"] = input
		fixed["enable_auto_chunking"] = true
		fixed["chunk_size"] = contextualizedChunkSize
	} else {
		inputs := make([][]string, len(input))
		for i, text := range input {
			inputs[i] = []string{text}
		}
		fixed["inputs"] = inputs
	}
	return base.JSONFieldsWithOptions(fixed, fields)
}

// CreateEmbeddings creates embeddings for the given texts using the specified
// Voyage AI model. The model is required and must be one of the supported text
// or contextualized embedding models. Supported opts are input_type
// ("document" or "query", required for contextualized models), output_dimension
// and, for text models only, truncation. Texts are split into several requests
// when they exceed the model's per-request limits, and the embeddings are
// returned in the order of texts.
// CreateEmbeddings implements base.Embedder
func (e *Embedder) CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error) {
	// ref: https://docs.voyageai.com/reference/embeddings-api and
	// https://docs.voyageai.com/reference/contextualized-embeddings-api
	if len(texts) == 0 {
		return [][]float32{}, nil
	}
	if model == "" {
		return nil, fmt.Errorf("model name is required")
	}
	spec, ok := models[model]
	if !ok {
		return nil, fmt.Errorf("Voyage AI model %q is not supported, supported models: %s", model, supportedModels())
	}
	parsed, err := parseOptions(model, spec, opts)
	if err != nil {
		return nil, err
	}
	apiKey, err := e.cfg.ResolveAPIKey(fmt.Errorf("API key is not configured for Voyage AI"))
	if err != nil {
		return nil, err
	}
	path, decode := embeddingsPath, decodeEmbeddings
	if spec.contextualized {
		path, decode = contextualizedPath, decodeContextualizedEmbeddings
	}
	endpoint, err := endpointURL(e.cfg.ConfiguredBaseURL(), path)
	if err != nil {
		return nil, err
	}

	embeddings := make([][]float32, 0, len(texts))
	for _, input := range splitRequests(texts, spec.maxRequestTokens) {
		batch, err := base.ExecuteJSONEmbeddingCall(ctx, len(input), base.JSONEmbeddingCall{
			Provider:             "Voyage AI",
			Client:               &e.client,
			Endpoint:             endpoint,
			Payload:              requestPayload(model, spec, input, parsed),
			Headers:              http.Header{"Authorization": {"Bearer " + apiKey}},
			MaxResponseBodyBytes: e.cfg.MaxResponseBodyBytes,
			Secrets:              []string{apiKey},
			DecodeErrorMessage:   decodeErrorMessage,
			StatusErrors: map[int]error{
				http.StatusUnauthorized: e.cfg.UnauthorizedError("Voyage AI", http.StatusUnauthorized),
			},
			DecodeEmbeddings: decode,
		})
		if err != nil {
			return nil, err
		}
		if parsed.outputDimension != 0 {
			for _, embedding := range batch {
				if len(embedding) != parsed.outputDimension {
					return nil, fmt.Errorf("Voyage AI returned an embedding of dimension %d, expected %d", len(embedding), parsed.outputDimension)
				}
			}
		}
		embeddings = append(embeddings, batch...)
	}
	return embeddings, nil
}
