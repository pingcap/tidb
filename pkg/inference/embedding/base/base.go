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
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"math"
	"regexp"
	"strings"
	"time"
)

const (
	// DefaultHTTPClientTimeout bounds embedding provider requests when the caller context is not cancelled.
	DefaultHTTPClientTimeout = 30 * time.Second
	// DefaultMaxResponseBodyBytes bounds memory used to read an embedding provider response.
	DefaultMaxResponseBodyBytes int64 = 32 << 20

	maxSanitizedErrorTextBytes = 4096
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
