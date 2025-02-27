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

type cacheData struct {
	chunkData []byte
	size      int
	checksum  uint32
}

// chunksCache is a simple cache for chunks.
type chunksCache struct {
	chunks  map[uint64]cacheData // chunkID -> cacheData
	baseDir string
	// If `usingMem`` is true, chunkData will be used, and the chunk data will be stored in memory.
	// Otherwise, the chunk data will be stored in a file.
	//
	// The default value of `usingMem` is false.
	// We found when the concurrency is high(means that there are many `chunk_sender`), writing to the file system is slow.
	// So we add a new parameter `usingMem` to control whether to store the chunk data in memory.
	usingMem bool
}

func newChunksCache(loadDataTaskID string, writerID uint64, basePath string, usingMem bool) (*chunksCache, error) {
	if usingMem {
		return &chunksCache{
			chunks:   map[uint64]cacheData{},
			usingMem: true,
		}, nil
	}

	path := getDefaultTempDir()
	if len(basePath) != 0 {
		path = basePath
	}

	baseDir := filepath.Join(path, loadDataTaskID, strconv.FormatUint(writerID, 10))
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

func (c *chunksCache) get(chunkID uint64) ([]byte, error) {
	meta, ok := c.chunks[chunkID]
	if !ok {
		return nil, errors.Errorf("chunk-%d not found", chunkID)
	}
	if c.usingMem {
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

func (c *chunksCache) put(chunkID uint64, buf []byte) error {
	if c.usingMem {
		c.chunks[chunkID] = cacheData{chunkData: buf}
		return nil
	}

	checksum := crc32.ChecksumIEEE(buf)
	c.chunks[chunkID] = cacheData{size: len(buf), checksum: checksum}

	fileName := c.getChunkFilePath(chunkID)
	return os.WriteFile(fileName, buf, 0o600)
}

func (c *chunksCache) clean(chunkID uint64) error {
	delete(c.chunks, chunkID)
	if c.usingMem {
		return nil
	}

	fileName := c.getChunkFilePath(chunkID)
	return os.Remove(fileName)
}

func (c *chunksCache) close() error {
	c.chunks = nil
	if c.usingMem {
		return nil
	}

	return os.RemoveAll(c.baseDir)
}

func (c *chunksCache) getChunkFilePath(chunkID uint64) string {
	return filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
}

func getDefaultTempDir() string {
	return filepath.Join(os.TempDir(), "lightning", "remote", "chunks")
}
