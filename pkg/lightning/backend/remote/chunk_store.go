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

// chunkInfo holds the data and metadata for a chunk.
type chunkInfo struct {
	// data is empty when we store the chunk into file.
	data     []byte
	size     int
	checksum uint32
}

// chunkStore is a simple store to put/get chunks.
type chunkStore struct {
	chunks  map[uint64]chunkInfo // chunkID -> chunkInfo
	baseDir string
	// If `useMemory` is true, chunkData will be stored in memory.
	// Otherwise, the chunk data will be stored in a file.
	//
	// We found when the concurrency is high (many `chunk_sender`), writing to the file system is slow.
	// So we add a new parameter `useMemory` to control whether to store the chunk data in memory.
	useMemory bool
}

// newChunkStore creates a new chunksStore.
func newChunkStore(loadDataTaskID string, writerID uint64, basePath string) (*chunkStore, error) {
	if len(basePath) == 0 {
		return &chunkStore{
			chunks:    map[uint64]chunkInfo{},
			useMemory: true,
		}, nil
	}

	baseDir := filepath.Join(basePath, loadDataTaskID, strconv.FormatUint(writerID, 10))
	// cleanup the directory if it exists
	err := os.RemoveAll(baseDir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = os.MkdirAll(baseDir, 0o750)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &chunkStore{
		chunks:  map[uint64]chunkInfo{},
		baseDir: baseDir,
	}, nil
}

// get retrieves the chunk data for the given chunkID.
func (c *chunkStore) get(chunkID uint64) ([]byte, error) {
	meta, ok := c.chunks[chunkID]
	if !ok {
		return nil, errors.Errorf("chunk-%d not found", chunkID)
	}
	if c.useMemory {
		return meta.data, nil
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
func (c *chunkStore) put(chunkID uint64, buf []byte) error {
	if c.useMemory {
		c.chunks[chunkID] = chunkInfo{data: buf}
		return nil
	}

	checksum := crc32.ChecksumIEEE(buf)
	c.chunks[chunkID] = chunkInfo{size: len(buf), checksum: checksum}

	fileName := c.getChunkFilePath(chunkID)
	return os.WriteFile(fileName, buf, 0o600)
}

// clean removes the chunk data for the given chunkID.
func (c *chunkStore) clean(chunkID uint64) error {
	delete(c.chunks, chunkID)
	if c.useMemory {
		return nil
	}

	fileName := c.getChunkFilePath(chunkID)
	return os.Remove(fileName)
}

// close cleans up the chunks.
func (c *chunkStore) close() error {
	c.chunks = nil
	if c.useMemory {
		return nil
	}

	return os.RemoveAll(c.baseDir)
}

// getChunkFilePath returns the file path for the given chunkID.
func (c *chunkStore) getChunkFilePath(chunkID uint64) string {
	return filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
}
