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
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeBase64EmbeddingF32(t *testing.T) {
	// This data comes from a real JINA embedding using this request:
	// curl https://api.jina.ai/v1/embeddings \
	// -H "Content-Type: application/json" \
	// -H "Authorization: Bearer <token>" \
	// -d @- <<EOFEOF
	// {
	// 	"model": "jina-embeddings-v4",
	// 	"task": "text-matching",
	// 	"dimensions": 10,
	// 	"embedding_type": "base64",
	// 	"input": [{
	// 		"text": "A beautiful sunset over the beach"
	// 	}]
	// }
	// EOFEOF
	decodedBytes, err := base64.StdEncoding.DecodeString("AAAYPgAAEb8AACq+AAAXPgAA4b0AAP0+AACUvQAA4TwAAC67AAAVPw==")
	require.NoError(t, err)
	result, err := DecodeFloat32ArrayBytes(decodedBytes)
	require.NoError(t, err)
	expected := []float32{0.1484375, -0.56640625, -0.166015625, 0.1474609375, -0.10986328125, 0.494140625, -0.072265625, 0.0274658203125, -0.002655029296875, 0.58203125}
	require.Equal(t, expected, result)

	decodedBytes, err = base64.StdEncoding.DecodeString("")
	require.NoError(t, err)
	result, err = DecodeFloat32ArrayBytes(decodedBytes)
	require.NoError(t, err)
	require.Empty(t, result)

	decodedBytes, err = base64.StdEncoding.DecodeString("AAAY")
	require.NoError(t, err)
	_, err = DecodeFloat32ArrayBytes(decodedBytes)
	require.Error(t, err)

	_, err = DecodeFloat32ArrayBytes([]byte{0x00, 0x01, 0x02})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid embedding data")

	_, err = DecodeFloat32ArrayBytes([]byte{0x00, 0x01, 0x02, 0x03, 0x04})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid embedding data")
}

func TestJSONFieldsWithOptions(t *testing.T) {
	fixed := map[string]any{
		"model":           "fixed-model",
		"input":           []string{"fixed-input"},
		"encoding_format": "base64",
	}
	opts := map[string]any{
		"model":           "overridden-model",
		"input":           []string{"overridden-input"},
		"encoding_format": "float",
		"dimensions":      512,
	}

	merged := JSONFieldsWithOptions(fixed, opts)
	require.Equal(t, "fixed-model", merged["model"])
	require.Equal(t, []string{"fixed-input"}, merged["input"])
	require.Equal(t, "base64", merged["encoding_format"])
	require.Equal(t, 512, merged["dimensions"])

	encoded, err := MarshalJSONWithOptions(fixed, opts)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"model":"fixed-model",
		"input":["fixed-input"],
		"encoding_format":"base64",
		"dimensions":512
	}`, string(encoded))
}

func TestSanitizeErrorBodyForLog(t *testing.T) {
	body := []byte(`{"authorization":"Bearer secret-token","api_key":"plain-key","message":"Bearer another-secret"}`)
	sanitized := SanitizeErrorBodyForLog(body)
	require.NotContains(t, sanitized, "secret-token")
	require.NotContains(t, sanitized, "plain-key")
	require.NotContains(t, sanitized, "another-secret")
	require.Contains(t, sanitized, "[REDACTED]")

	openAIKey := "sk-proj-super-secret-value"
	sanitized = SanitizeErrorBodyForLog([]byte(`{"message":"Incorrect API key provided: ` + openAIKey + `"}`))
	require.NotContains(t, sanitized, openAIKey)
	require.Contains(t, sanitized, "[REDACTED]")

	longKey := strings.Repeat("s", maxLoggedErrorBodyBytes+128)
	sanitized = SanitizeErrorBodyForLog([]byte(`{"api_key":"` + longKey + `"}`))
	require.NotContains(t, sanitized, longKey[:maxLoggedErrorBodyBytes/2])
	require.Contains(t, sanitized, "[REDACTED]")
	require.LessOrEqual(t, len(sanitized), maxLoggedErrorBodyBytes+len("...[truncated]"))
}
