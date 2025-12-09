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

func TestChunksStore(t *testing.T) {
	testChunksStore(t, "")
	basePath := os.TempDir()
	testChunksStore(t, basePath)
}

func testChunksStore(t *testing.T, basePath string) {
	// Create a new store
	taskID := "test-task"

	chunksStore, err := newChunkStore(taskID, 1, basePath)
	require.NoError(t, err)

	// Get a non-existent chunk
	_, err = chunksStore.get(0)
	require.Error(t, err)
	_, err = chunksStore.get(1)
	require.Error(t, err)

	chunk0 := []byte("chunk0")
	err = chunksStore.put(0, chunk0)
	require.NoError(t, err)

	chunk1 := []byte("chunk1")
	err = chunksStore.put(1, chunk1)
	require.NoError(t, err)

	// Get the chunks
	chunk0Got, err := chunksStore.get(0)
	require.NoError(t, err)
	require.Equal(t, chunk0, chunk0Got)

	chunk1Got, err := chunksStore.get(1)
	require.NoError(t, err)
	require.Equal(t, chunk1, chunk1Got)

	// Clean the chunks
	err = chunksStore.clean(0)
	require.NoError(t, err)
	_, err = chunksStore.get(0)
	require.Error(t, err)

	err = chunksStore.clean(1)
	require.NoError(t, err)
	_, err = chunksStore.get(1)
	require.Error(t, err)

	// Close the cache
	err = chunksStore.close()
	require.NoError(t, err)
}
