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

package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMockEmbedder(t *testing.T) {
	embedder := NewMockEmbedder()
	require.NotNil(t, embedder)
}

func TestMockEmbedder_Basic(t *testing.T) {
	embedder := NewMockEmbedder()
	ctx := context.Background()

	result, err := embedder.CreateEmbeddings(ctx, "json", []string{"[1.0, 2.0]", "[3.0, 4.0, 5.0]"}, nil)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1.0, 2.0}, {3.0, 4.0, 5.0}}, result)

	result, err = embedder.CreateEmbeddings(ctx, "json", []string{"[]"}, nil)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{}}, result)

	result, err = embedder.CreateEmbeddings(ctx, "json", []string{}, nil)
	require.NoError(t, err)
	require.Equal(t, [][]float32{}, result)

	// Failure case: unknown model
	_, err = embedder.CreateEmbeddings(ctx, "xx", []string{"[]"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown model")

	// Failure case: invalid texts
	_, err = embedder.CreateEmbeddings(ctx, "json", []string{"invalid"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid character")
}

func TestMockEmbedder_WithPlus(t *testing.T) {
	embedder := NewMockEmbedder()
	ctx := context.Background()

	opts := map[string]any{"plus": 1.5}
	result, err := embedder.CreateEmbeddings(ctx, "json", []string{"[1.0, 2.0, 3.0]"}, opts)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{2.5, 3.5, 4.5}}, result)
}
