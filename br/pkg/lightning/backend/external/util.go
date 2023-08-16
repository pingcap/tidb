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

package external

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// prettyFileNames removes the directory prefix except the last level from the
// file names.
func prettyFileNames(files []string) []string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		dir, file := filepath.Split(f)
		names = append(names, fmt.Sprintf("%s/%s", filepath.Base(dir), file))
	}
	return names
}

// seekPropsOffsets seeks the statistic files to find the largest offset of
// sorted data file offsets such that the key at offset is less than or equal to
// the given start key.
func seekPropsOffsets(
	ctx context.Context,
	start kv.Key,
	paths []string,
	exStorage storage.ExternalStorage,
) ([]uint64, error) {
	iter, err := NewMergePropIter(ctx, paths, exStorage)
	if err != nil {
		return nil, err
	}
	logger := logutil.Logger(ctx)
	defer func() {
		if err := iter.Close(); err != nil {
			logger.Warn("failed to close merge prop iterator", zap.Error(err))
		}
	}()
	offsets := make([]uint64, len(paths))
	for iter.Next() {
		p := iter.prop()
		propKey := kv.Key(p.key)
		if propKey.Cmp(start) > 0 {
			return offsets, nil
		} else {
			offsets[iter.readerIndex()] = p.offset
		}
	}
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	return offsets, nil
}

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
