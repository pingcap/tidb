package remote

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

const defaultPath = "/tmp/lightning/remote/chunks"

type chunkMeta struct {
	size     int
	checksum uint32
	// if usingMem is true, chunkData will be used.
	chunkData []byte
}

// chunkCache is a simple cache for chunks.
type chunkCache struct {
	chunks   map[uint64]chunkMeta // chunkID -> chunkMeta
	baseDir  string
	usingMem bool
}

func newChunkCache(loadDataTaskID string, writerID uint64, basePath string, usingMem bool) (*chunkCache, error) {
	if usingMem {
		return &chunkCache{
			chunks:   map[uint64]chunkMeta{},
			usingMem: true,
		}, nil
	}
	path := defaultPath
	if basePath != "" {
		path = basePath
	}
	baseDir := filepath.Join(path, loadDataTaskID, fmt.Sprintf("%d", writerID))
	err := os.MkdirAll(baseDir, 0o755)
	if err != nil {
		return nil, err
	}
	return &chunkCache{
		chunks:  map[uint64]chunkMeta{},
		baseDir: baseDir,
	}, nil
}

func (c *chunkCache) get(chunkID uint64) ([]byte, error) {
	meta, ok := c.chunks[chunkID]
	if !ok {
		return nil, errors.Errorf("chunk-%d not found", chunkID)
	}
	if c.usingMem {
		return meta.chunkData, nil
	}

	path := filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, meta.size)
	n, err := file.Read(buf)
	if err != nil {
		return nil, err
	}
	buf = buf[:n]

	checksum := crc32.Checksum(buf, crc32.MakeTable(crc32.IEEE))
	if checksum != meta.checksum {
		return nil, errors.Errorf("chunk-%d checksum mismatch", chunkID)
	}
	return buf, file.Close()
}

func (c *chunkCache) put(chunkID uint64, buf []byte) error {
	if c.usingMem {
		c.chunks[chunkID] = chunkMeta{size: len(buf), chunkData: buf}
		return nil
	}

	fileName := filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	checksum := crc32.Checksum(buf, crc32.MakeTable(crc32.IEEE))

	_, err = file.Write(buf)
	if err != nil {
		file.Close()
		os.Remove(fileName)
		return err
	}

	c.chunks[chunkID] = chunkMeta{size: len(buf), checksum: checksum}
	return file.Close()
}

func (c *chunkCache) clean(chunkID uint64) error {
	if _, ok := c.chunks[chunkID]; !ok {
		return nil
	}
	delete(c.chunks, chunkID)
	if c.usingMem {
		return nil
	}

	fileName := filepath.Join(c.baseDir, fmt.Sprintf("chunk-%d", chunkID))
	return os.Remove(fileName)
}

func (c *chunkCache) close() error {
	c.chunks = nil
	if c.usingMem {
		return nil
	}

	return os.RemoveAll(c.baseDir)
}
