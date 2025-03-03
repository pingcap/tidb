// Copyright 2025 PingCAP, Inc.
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

package remote

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunksCache(t *testing.T) {
	testChunksCache(t, "")
	basePath := os.TempDir()
	testChunksCache(t, basePath)
}

func testChunksCache(t *testing.T, basePath string) {
	// Create a new cache
	taskID := "test-task"
	chunksCache, err := newChunksCache(taskID, 1, basePath)
	require.NoError(t, err)

	chunk0 := []byte("chunk0")
	err = chunksCache.put(0, chunk0)
	require.NoError(t, err)

	chunk1 := []byte("chunk1")
	err = chunksCache.put(1, chunk1)
	require.NoError(t, err)

	// Get the chunks
	chunk0Got, err := chunksCache.get(0)
	require.NoError(t, err)
	require.Equal(t, chunk0, chunk0Got)

	chunk1Got, err := chunksCache.get(1)
	require.NoError(t, err)
	require.Equal(t, chunk1, chunk1Got)

	// Clean the chunks
	err = chunksCache.clean(0)
	require.NoError(t, err)
	_, err = chunksCache.get(0)
	require.Error(t, err)

	err = chunksCache.clean(1)
	require.NoError(t, err)
	_, err = chunksCache.get(1)
	require.Error(t, err)

	// Close the cache
	err = chunksCache.close()
	require.NoError(t, err)
}
