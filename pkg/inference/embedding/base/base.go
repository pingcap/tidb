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
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"time"
)

const (
	// DefaultHTTPClientTimeout bounds embedding provider requests when the caller context is not cancelled.
	DefaultHTTPClientTimeout = 30 * time.Second

	maxLoggedErrorBodyBytes = 4096
)

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
	for key, value := range opts {
		merged[key] = value
	}
	for key, value := range fields {
		merged[key] = value
	}
	return merged
}

// MarshalJSONWithOptions marshals fixed request fields plus provider-specific options.
func MarshalJSONWithOptions(fields map[string]any, opts map[string]any) ([]byte, error) {
	return json.Marshal(JSONFieldsWithOptions(fields, opts))
}

// SanitizeErrorBodyForLog redacts common credential fields from a provider error response body before logging.
func SanitizeErrorBodyForLog(body []byte) string {
	s := string(body)
	s = sensitiveJSONFieldPattern.ReplaceAllString(s, `$1[REDACTED]$3`)
	s = bearerTokenPattern.ReplaceAllString(s, "Bearer [REDACTED]")
	s = openAIAPIKeyPattern.ReplaceAllString(s, "[REDACTED]")
	if len(s) > maxLoggedErrorBodyBytes {
		s = s[:maxLoggedErrorBodyBytes] + "...[truncated]"
	}
	return s
}
