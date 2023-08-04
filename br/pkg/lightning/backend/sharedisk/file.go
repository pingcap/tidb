// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sharedisk

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// KeyValueStore stores key-value pairs and maintains the range properties.
type KeyValueStore struct {
	dataWriter storage.ExternalFileWriter

	rc        *rangePropertiesCollector
	ctx       context.Context
	offset    uint64
	u64Buffer []byte
}

// NewKeyValueStore creates a new KeyValueStore. The data will be written to the
// given dataWriter and range properties will be maintained in the given
// rangePropertiesCollector.
func NewKeyValueStore(
	ctx context.Context,
	dataWriter storage.ExternalFileWriter,
	rangePropertiesCollector *rangePropertiesCollector,
) (*KeyValueStore, error) {
	kvStore := &KeyValueStore{
		dataWriter: dataWriter,
		ctx:        ctx,
		rc:         rangePropertiesCollector,
		u64Buffer:  make([]byte, 8),
	}
	return kvStore, nil
}

// AddKeyValue saves a key-value pair to the KeyValueStore. If the accumulated
// size or key count exceeds the given distance, a new range property will be
// appended to the rangePropertiesCollector with current status.
// Caller should guarantee `writerID` and `seq` are not changed.
func (s *KeyValueStore) AddKeyValue(key, value []byte, writerID, seq int) error {
	kvLen := len(key) + len(value) + 16

	// data layout: keyLen + key + valueLen + value
	_, err := s.dataWriter.Write(
		s.ctx,
		binary.BigEndian.AppendUint64(s.u64Buffer[:0], uint64(len(key))),
	)
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, key)
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(
		s.ctx,
		binary.BigEndian.AppendUint64(s.u64Buffer[:0], uint64(len(value))),
	)
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, value)
	if err != nil {
		return err
	}

	if len(s.rc.currProp.key) == 0 {
		s.rc.currProp.key = key
		s.rc.currProp.writerID = writerID
		s.rc.currProp.dataSeq = seq
	}

	s.offset += uint64(kvLen)
	s.rc.currProp.size += uint64(len(key) + len(value))
	s.rc.currProp.keys++

	if s.rc.currProp.size >= s.rc.propSizeIdxDistance ||
		s.rc.currProp.keys >= s.rc.propKeysIdxDistance {
		newProp := *s.rc.currProp
		s.rc.props = append(s.rc.props, &newProp)

		s.rc.currProp.key = nil
		s.rc.currProp.offset = s.offset
		s.rc.currProp.rangeOffsets = rangeOffsets{}
	}

	return nil
}

var statSuffix = filepath.Join("_stat", "0")

// GetAllFileNames returns a FilePathHandle that contains all data file paths
// and a slice of stat file paths.
func GetAllFileNames(
	ctx context.Context,
	store storage.ExternalStorage,
	subDir string,
) (FilePathHandle, []string, error) {
	var dataFilePaths FilePathHandle
	var stats []string

	err := store.WalkDir(ctx,
		&storage.WalkOption{SubDir: subDir},
		func(path string, size int64) error {
			if strings.HasSuffix(path, statSuffix) {
				stats = append(stats, path)
			} else {
				dir, file := filepath.Split(path)
				writerID, err := strconv.Atoi(filepath.Base(dir))
				if err != nil {
					return err
				}
				seq, err := strconv.Atoi(file)
				if err != nil {
					return err
				}
				dataFilePaths.set(writerID, seq, path)
			}
			return nil
		})
	if err != nil {
		return dataFilePaths, nil, err
	}
	return dataFilePaths, stats, nil
}

// FilePathHandle handles data file paths under a prefix path.
type FilePathHandle struct {
	paths [][]string
}

func (p *FilePathHandle) set(writerID, seq int, path string) {
	if writerID >= len(p.paths) {
		p.paths = append(p.paths, make([][]string, writerID-len(p.paths)+1)...)
	}
	if seq >= len(p.paths[writerID]) {
		p.paths[writerID] = append(p.paths[writerID], make([]string, seq-len(p.paths[writerID])+1)...)
	}
	p.paths[writerID][seq] = path
}

// Get returns the path of the data file with the given writerID and seq.
func (p *FilePathHandle) Get(writerID, seq int) string {
	return p.paths[writerID][seq]
}

// ForEach applies the given function to each data file path.
func (p *FilePathHandle) ForEach(f func(writerID, seq int, path string)) {
	for writerID, paths := range p.paths {
		for seq, path := range paths {
			f(writerID, seq, path)
		}
	}
}

// FlatSlice returns a flat slice of all data file paths.
func (p *FilePathHandle) FlatSlice() []string {
	var paths []string
	p.ForEach(func(writerID, seq int, path string) {
		paths = append(paths, path)
	})
	return paths
}
