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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkCache(t *testing.T) {
	testChunkCache(t, false)
	testChunkCache(t, true)
}

func testChunkCache(t *testing.T, usingMem bool) {
	// Create a new cache
	taskID := "test-task"
	chunkCache, err := newChunkCache(taskID, 1, "", usingMem)
	require.NoError(t, err)

	chunk0 := []byte("chunk0")
	chunkCache.put(0, chunk0)

	chunk1 := []byte("chunk1")
	chunkCache.put(1, chunk1)

	// Get the chunks
	chunk0Got, err := chunkCache.get(0)
	require.NoError(t, err)
	require.Equal(t, chunk0, chunk0Got)

	chunk1Got, err := chunkCache.get(1)
	require.NoError(t, err)
	require.Equal(t, chunk1, chunk1Got)

	// Clean the chunks
	err = chunkCache.clean(0)
	require.NoError(t, err)
	_, err = chunkCache.get(0)
	require.Error(t, err)

	err = chunkCache.clean(1)
	require.NoError(t, err)
	_, err = chunkCache.get(1)
	require.Error(t, err)

	// Close the cache
	err = chunkCache.close()
	require.NoError(t, err)
}
