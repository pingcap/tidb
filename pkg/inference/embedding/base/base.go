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
