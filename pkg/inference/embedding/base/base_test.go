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
