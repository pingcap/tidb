// Copyright 2023 PingCAP, Inc.
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

package sharedisk

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type KeyValueStore struct {
	dataWriter storage.ExternalFileWriter

	rc        *RangePropertiesCollector
	ctx       context.Context
	offset    uint64
	keyCnt    uint64
	u64Buffer []byte
}

func Create(ctx context.Context, dataWriter storage.ExternalFileWriter) (*KeyValueStore, error) {
	kvStore := &KeyValueStore{dataWriter: dataWriter, ctx: ctx, u64Buffer: make([]byte, 8)}
	return kvStore, nil
}

func (s *KeyValueStore) AddKeyValue(key, value []byte, writerID, seq int) error {
	kvLen := len(key) + len(value) + 16

	_, err := s.dataWriter.Write(s.ctx, binary.BigEndian.AppendUint64(s.u64Buffer[:0], uint64(len(key))))
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, key)
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, binary.BigEndian.AppendUint64(s.u64Buffer[:0], uint64(len(value))))
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, value)
	if err != nil {
		return err
	}

	if len(s.rc.lastKey) == 0 || s.rc.currProp.Size >= s.rc.propSizeIdxDistance ||
		s.rc.currProp.Keys >= s.rc.propKeysIdxDistance {
		if len(s.rc.lastKey) != 0 {
			newProp := *s.rc.currProp
			s.rc.props = append(s.rc.props, &newProp)
		}
		s.rc.currProp.Key = key
		s.rc.currProp.offset = s.offset
		s.rc.currProp.rangeOffsets = rangeOffsets{}
		s.rc.currProp.WriterID = writerID
		s.rc.currProp.DataSeq = seq
	}

	s.rc.lastKey = key
	s.offset += uint64(kvLen)
	s.keyCnt++

	s.rc.currProp.Size += uint64(len(key) + len(value))
	s.rc.currProp.Keys++

	return nil
}

func GetAllFileNames(ctx context.Context, store storage.ExternalStorage,
	subDir string) (FilePathHandle, []string, error) {
	var dataFilePaths FilePathHandle
	var stats []string

	err := store.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, size int64) error {
		if strings.Contains(path, "_stat") {
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
			dataFilePaths.Set(writerID, seq, path)
		}
		return nil
	})
	if err != nil {
		return dataFilePaths, nil, err
	}
	return dataFilePaths, stats, nil
}

type FilePathHandle struct {
	paths [][]string
}

func (p *FilePathHandle) Set(writerID, seq int, path string) {
	if writerID >= len(p.paths) {
		p.paths = append(p.paths, make([][]string, writerID-len(p.paths)+1)...)
	}
	if seq >= len(p.paths[writerID]) {
		p.paths[writerID] = append(p.paths[writerID], make([]string, seq-len(p.paths[writerID])+1)...)
	}
	p.paths[writerID][seq] = path
}

func (p *FilePathHandle) Get(writerID, seq int) string {
	return p.paths[writerID][seq]
}

func (p *FilePathHandle) ForEach(f func(writerID, seq int, path string)) {
	for writerID, paths := range p.paths {
		for seq, path := range paths {
			f(writerID, seq, path)
		}
	}
}

func (p *FilePathHandle) FlatSlice() []string {
	var paths []string
	p.ForEach(func(writerID, seq int, path string) {
		paths = append(paths, path)
	})
	return paths
}
