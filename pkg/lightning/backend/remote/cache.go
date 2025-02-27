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
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/errors"
)

// cacheData holds the data and metadata for a chunk.
type cacheData struct {
	chunkData []byte
	size      int
	checksum  uint32
}

// chunksCache is a simple cache for chunks.
type chunksCache struct {
	chunks  map[uint64]cacheData // chunkID -> cacheData
	baseDir string
	// If `useMemory` is true, chunkData will be stored in memory.
	// Otherwise, the chunk data will be stored in a file.
	//
	// We found when the concurrency is high (many `chunk_sender`), writing to the file system is slow.
	// So we add a new parameter `useMemory` to control whether to store the chunk data in memory.
	useMemory bool
}

// newChunksCache creates a new chunksCache.
func newChunksCache(loadDataTaskID string, writerID uint64, basePath string) (*chunksCache, error) {
	if len(basePath) == 0 {
		return &chunksCache{
			chunks:    map[uint64]cacheData{},
			useMemory: true,
		}, nil
	}

	baseDir := filepath.Join(basePath, loadDataTaskID, strconv.FormatUint(writerID, 10))
	// cleanup the directory if it exists
	_ = os.RemoveAll(baseDir)

	err := os.MkdirAll(baseDir, 0o750)
	if err != nil {
		return nil, err
	}
	return &chunksCache{
		chunks:  map[uint64]cacheData{},
		baseDir: baseDir,
	}, nil
}

// get retrieves the chunk data for the given chunkID.
func (c *chunksCache) get(chunkID uint64) ([]byte, error) {
	meta, ok := c.chunks[chunkID]
	if !ok {
		return nil, errors.Errorf("chunk-%d not found", chunkID)
	}
	if c.useMemory {
		return meta.chunkData, nil
	}

	fileName := c.getChunkFilePath(chunkID)

	buf, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	if len(buf) != meta.size {
		return nil, errors.Errorf("chunk-%d size mismatch", chunkID)
	}

	checksum := crc32.ChecksumIEEE(buf)
	if checksum != meta.checksum {
		return nil, errors.Errorf("chunk-%d checksum mismatch", chunkID)
	}
	return buf, nil
}

// put stores the chunk data for the given chunkID.
func (c *chunksCache) put(chunkID uint64, buf []byte) error {
	if c.useMemory {
		c.chunks[chunkID] = cacheData{chunkData: buf}
		return nil
	}

	checksum := crc32.ChecksumIEEE(buf)
	c.chunks[chunkID] = cacheData{size: len(buf), checksum: checksum}

	fileName := c.getChunkFilePath(chunkID)
	return os.WriteFile(fileName, buf, 0o600)
}

// clean removes the chunk data for the given chunkID.
func (c *chunksCache) clean(chunkID uint64) error {
	delete(c.chunks, chunkID)
	if c.useMemory {
		return nil
	}

	fileName := c.getChunkFilePath(chunkID)
	return os.Remove(fileName)
}

// close cleans up the cache.
func (c *chunksCache) close() error {
	c.chunks = nil
	if c.useMemory {
		return nil
	}

	return os.RemoveAll(c.baseDir)
}

// getChunkFilePath returns the file path for the given chunkID.
func (c *chunksCache) getChunkFilePath(chunkID uint64) string {
	return filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
}
