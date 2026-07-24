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

package base

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

const (
	// DefaultHTTPClientTimeout bounds embedding provider requests when the caller context is not cancelled.
	DefaultHTTPClientTimeout = 30 * time.Second
	// DefaultMaxResponseBodyBytes bounds memory used to read an embedding provider response.
	DefaultMaxResponseBodyBytes int64 = 32 * units.MiB

	maxSanitizedErrorTextBytes = 4 * units.KiB
)

// ReadResponseBody reads an embedding provider response up to maxBytes and
// reports an error if the response contains more data.
func ReadResponseBody(reader io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("maximum response body size must not be negative")
	}

	// Read one extra byte to distinguish a response at the limit from one over
	// the limit. Avoid overflowing when callers intentionally use MaxInt64.
	readLimit := maxBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	body, err := io.ReadAll(io.LimitReader(reader, readLimit))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("response body exceeds maximum size of %d bytes", maxBytes)
	}
	return body, nil
}

var (
	sensitiveJSONFieldPattern = regexp.MustCompile(`(?i)("(?:authorization|api[_-]?key|token|access[_-]?token|credentials)"\s*:\s*")([^"]*)(")`)
	bearerTokenPattern        = regexp.MustCompile(`(?i)Bearer\s+[A-Za-z0-9._~+/=-]+`)
	openAIAPIKeyPattern       = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`)
)

// Embedder is an interface for embedding providers.
type Embedder interface {
	// CreateEmbeddings generates embeddings for the given texts using the specified model and options.
	// Different implementations requires different options types. Options can be nil if not needed.
	CreateEmbeddings(ctx context.Context, model string, texts []string, opts map[string]any) ([][]float32, error)
}

// IndexedBase64Embedding is an embedding response item whose position is
// identified by Index and whose Embedding is base64-decoded by encoding/json.
type IndexedBase64Embedding struct {
	Object    string `json:"object"`
	Index     int    `json:"index"`
	Embedding []byte `json:"embedding"`
}

// APIKeyProviderConfig contains configuration shared by embedding providers
// that authenticate with an API key and support custom missing-key and
// unauthorized errors. Each provider defines the exact meaning of GetBaseURL.
type APIKeyProviderConfig struct {
	// GetAPIKey returns the current API key.
	GetAPIKey func() string
	// GetBaseURL returns the provider-specific configured base URL or endpoint.
	GetBaseURL func() string
	// ErrMissingAPIKey overrides the provider's default missing-key error.
	ErrMissingAPIKey error
	// ErrUnauthorized overrides the provider's default unauthorized error.
	ErrUnauthorized error
	// MaxResponseBodyBytes limits both successful and error response bodies.
	// Non-positive values use DefaultMaxResponseBodyBytes.
	MaxResponseBodyBytes int64
}

// JSONEmbeddingCall describes one conventional JSON embedding provider call.
// Provider must be a stable, non-sensitive label because it is used in logs
// and generic response errors. Provider-specific wire schemas stay in the
// decoder functions supplied by the provider package.
type JSONEmbeddingCall struct {
	Provider             string
	Client               *http.Client
	Endpoint             string
	Payload              any
	Headers              http.Header
	MaxResponseBodyBytes int64
	Secrets              []string

	// DecodeErrorMessage parses a non-200 response and returns an unsanitized
	// provider message. ExecuteJSONEmbeddingCall owns sanitization and logging.
	DecodeErrorMessage func(body []byte) (string, error)
	// StatusErrors maps special HTTP statuses to provider-specific errors.
	// Nil or missing entries use NewProviderResponseError.
	StatusErrors map[int]error
	// DecodeEmbeddings parses and validates a successful response.
	DecodeEmbeddings func(body []byte, expectedCount int) ([][]float32, error)
}

// WithDefaults returns a copy with default values applied.
func (c APIKeyProviderConfig) WithDefaults() APIKeyProviderConfig {
	if c.MaxResponseBodyBytes <= 0 {
		c.MaxResponseBodyBytes = DefaultMaxResponseBodyBytes
	}
	return c
}

// ResolveAPIKey returns the configured API key. If it is empty, the custom
// missing-key error is preferred over fallbackErr.
func (c APIKeyProviderConfig) ResolveAPIKey(fallbackErr error) (string, error) {
	if c.GetAPIKey != nil {
		if apiKey := c.GetAPIKey(); apiKey != "" {
			return apiKey, nil
		}
	}
	if c.ErrMissingAPIKey != nil {
		return "", c.ErrMissingAPIKey
	}
	if fallbackErr != nil {
		return "", fallbackErr
	}
	return "", fmt.Errorf("API key is not configured")
}

// ConfiguredBaseURL returns the configured provider URL or an empty string.
func (c APIKeyProviderConfig) ConfiguredBaseURL() string {
	if c.GetBaseURL == nil {
		return ""
	}
	return c.GetBaseURL()
}

type redactedError struct {
	message string
	cause   error
}

func (e *redactedError) Error() string {
	return e.message
}

func (e *redactedError) Unwrap() error {
	return e.cause
}

// NewRedactedError returns an error with a safe user-facing message while
// preserving cause for errors.Is and errors.As. It is intended for errors
// whose original text may contain configured endpoints or credentials.
func NewRedactedError(message string, cause error) error {
	return &redactedError{message: message, cause: cause}
}

// NewProviderRequestError redacts endpoint details from request and transport
// errors. If the caller's context has completed, its cause is returned so
// cancellation and deadline errors remain recognizable to callers.
func NewProviderRequestError(ctx context.Context, provider string, cause error) error {
	if contextCause := context.Cause(ctx); contextCause != nil {
		return contextCause
	}
	return NewRedactedError(provider+" request failed", cause)
}

// ParseHTTPURL parses and validates an absolute HTTP(S) URL. description must
// be a fixed, non-sensitive name used to construct a safe error message.
func ParseHTTPURL(rawURL, description string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, NewRedactedError("invalid "+description, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid %s: absolute HTTP(S) URL is required", description)
	}
	return u, nil
}

// SetEscapedURLPath assigns an escaped URL path without exposing the original
// path in an error. description must be a fixed, non-sensitive name.
func SetEscapedURLPath(u *url.URL, escapedPath, description string) error {
	path, err := url.PathUnescape(escapedPath)
	if err != nil {
		return NewRedactedError("invalid "+description, err)
	}
	u.Path = path
	u.RawPath = escapedPath
	return nil
}

// NewJSONRequest creates an HTTP POST request with a JSON content type while
// keeping endpoint details out of request-construction errors.
func NewJSONRequest(ctx context.Context, provider, endpoint string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, NewProviderRequestError(ctx, provider, err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// DoRequest executes an embedding provider request, closes the response body,
// and reads at most maxResponseBodyBytes bytes.
func DoRequest(
	ctx context.Context,
	client *http.Client,
	provider string,
	req *http.Request,
	maxResponseBodyBytes int64,
) (int, []byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, NewProviderRequestError(ctx, provider, err)
	}
	defer resp.Body.Close()

	body, err := ReadResponseBody(resp.Body, maxResponseBodyBytes)
	if err != nil {
		if contextCause := context.Cause(ctx); contextCause != nil {
			return 0, nil, contextCause
		}
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

// PostJSON marshals payload, creates a JSON POST request, applies headers, and
// executes it with a bounded response body.
func PostJSON(
	ctx context.Context,
	client *http.Client,
	provider string,
	endpoint string,
	payload any,
	headers http.Header,
	maxResponseBodyBytes int64,
) (int, []byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("unexpected marshal request error: %w", err)
	}

	req, err := NewJSONRequest(ctx, provider, endpoint, body)
	if err != nil {
		return 0, nil, err
	}
	for name, values := range headers {
		req.Header.Del(name)
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	// The payload is always JSON even if callers supplied a conflicting header.
	req.Header.Set("Content-Type", "application/json")

	return DoRequest(ctx, client, provider, req, maxResponseBodyBytes)
}

// ExecuteJSONEmbeddingCall executes a conventional JSON provider call and
// applies the common response lifecycle. Provider-specific error and success
// schemas are delegated to the supplied decoder functions.
func ExecuteJSONEmbeddingCall(
	ctx context.Context,
	expectedCount int,
	call JSONEmbeddingCall,
) ([][]float32, error) {
	if call.DecodeErrorMessage == nil {
		return nil, fmt.Errorf("%s error response decoder is not configured", call.Provider)
	}
	if call.DecodeEmbeddings == nil {
		return nil, fmt.Errorf("%s success response decoder is not configured", call.Provider)
	}

	statusCode, body, err := PostJSON(
		ctx,
		call.Client,
		call.Provider,
		call.Endpoint,
		call.Payload,
		call.Headers,
		call.MaxResponseBodyBytes,
	)
	if err != nil {
		return nil, err
	}
	if statusCode == http.StatusOK {
		return call.DecodeEmbeddings(body, expectedCount)
	}

	message, parseErr := call.DecodeErrorMessage(body)
	message = SanitizeErrorText(message, call.Secrets...)
	logFields := []zap.Field{zap.Int("status", statusCode)}
	if message != "" {
		logFields = append(logFields, zap.String("message", message))
	}
	if parseErr != nil {
		logFields = append(logFields, zap.String("parse_error", SanitizeErrorText(parseErr.Error(), call.Secrets...)))
	}
	logutil.BgLogger().Error(call.Provider+" API request failed", logFields...)

	if statusErr := call.StatusErrors[statusCode]; statusErr != nil {
		return nil, statusErr
	}
	return nil, NewProviderResponseError(call.Provider, statusCode, message)
}

// NewProviderResponseError returns a consistent generic provider error. The
// message must already be sanitized when it originated from a remote service.
func NewProviderResponseError(provider string, statusCode int, message string) error {
	if message == "" {
		message = http.StatusText(statusCode)
	}
	return fmt.Errorf("%s: status code %d, message: %s", provider, statusCode, message)
}

// DecodeFloat32ArrayBytes decodes bytes of an float32 array in little endian into a float32 slice.
func DecodeFloat32ArrayBytes(item []byte) ([]float32, error) {
	if len(item)%4 != 0 {
		return nil, fmt.Errorf("invalid embedding data")
	}
	dims := len(item) / 4
	embeddings := make([]float32, dims)
	for i := range dims {
		bytes := item[i*4 : (i+1)*4]
		bits := binary.LittleEndian.Uint32(bytes)
		embeddings[i] = math.Float32frombits(bits)
	}
	return embeddings, nil
}

// DecodeIndexedBase64Embeddings validates and decodes indexed embedding
// response items, restoring the order of the original input texts.
func DecodeIndexedBase64Embeddings(items []IndexedBase64Embedding, expectedCount int) ([][]float32, error) {
	if len(items) != expectedCount {
		return nil, fmt.Errorf("response data length %d does not match input texts length %d", len(items), expectedCount)
	}

	embeddings := make([][]float32, expectedCount)
	for _, item := range items {
		if item.Index < 0 || item.Index >= expectedCount {
			return nil, fmt.Errorf("response data index %d is out of range [0, %d)", item.Index, expectedCount)
		}
		if embeddings[item.Index] != nil {
			return nil, fmt.Errorf("response data contains duplicate index %d", item.Index)
		}
		embedding, err := DecodeFloat32ArrayBytes(item.Embedding)
		if err != nil {
			return nil, fmt.Errorf("failed to decode embedding for index %d: %w", item.Index, err)
		}
		embeddings[item.Index] = embedding
	}
	return embeddings, nil
}

// JSONFieldsWithOptions returns a JSON object map containing fixed request fields
// plus provider-specific options. Fixed fields override options when keys collide.
func JSONFieldsWithOptions(fields map[string]any, opts map[string]any) map[string]any {
	merged := make(map[string]any, len(fields)+len(opts))
	maps.Copy(merged, opts)
	maps.Copy(merged, fields)
	return merged
}

// SanitizeErrorText redacts common credential patterns and the provided exact
// secrets from decoded provider error text before logging or returning it.
func SanitizeErrorText(text string, secrets ...string) string {
	s := text
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, "[REDACTED]")
		}
	}
	s = sensitiveJSONFieldPattern.ReplaceAllString(s, `$1[REDACTED]$3`)
	s = bearerTokenPattern.ReplaceAllString(s, "Bearer [REDACTED]")
	s = openAIAPIKeyPattern.ReplaceAllString(s, "[REDACTED]")
	if len(s) > maxSanitizedErrorTextBytes {
		s = s[:maxSanitizedErrorTextBytes] + "...[truncated]"
	}
	return s
}
