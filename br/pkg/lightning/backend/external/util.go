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
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	kv2 "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	moved := false
	for iter.Next() {
		p := iter.prop()
		propKey := kv.Key(p.key)
		if propKey.Cmp(start) > 0 {
			if !moved {
				return nil, fmt.Errorf("start key %s is too small for stat files %v",
					start.String(),
					paths,
				)
			}
			return offsets, nil
		}
		moved = true
		offsets[iter.readerIndex()] = p.offset
	}
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	return offsets, nil
}

// GetAllFileNames returns data file paths and stat file paths. Both paths are
// sorted.
func GetAllFileNames(
	ctx context.Context,
	store storage.ExternalStorage,
	subDir string,
) ([]string, []string, error) {
	var data []string
	var stats []string

	err := store.WalkDir(ctx,
		&storage.WalkOption{SubDir: subDir},
		func(path string, size int64) error {
			// path example: /subtask/0_stat/0

			// extract the parent dir
			bs := hack.Slice(path)
			lastIdx := bytes.LastIndexByte(bs, '/')
			secondLastIdx := bytes.LastIndexByte(bs[:lastIdx], '/')
			parentDir := path[secondLastIdx+1 : lastIdx]

			if strings.HasSuffix(parentDir, statSuffix) {
				stats = append(stats, path)
			} else {
				data = append(data, path)
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}
	// in case the external storage does not guarantee the order of walk
	sort.Strings(data)
	sort.Strings(stats)
	return data, stats, nil
}

// CleanUpFiles delete all data and stat files under one subDir.
func CleanUpFiles(ctx context.Context,
	store storage.ExternalStorage,
	subDir string,
	concurrency uint) error {
	dataNames, statNames, err := GetAllFileNames(ctx, store, subDir)
	if err != nil {
		return err
	}

	eg := &errgroup.Group{}
	workerPool := utils.NewWorkerPool(concurrency, "delete global sort files")
	for i := range dataNames {
		data := dataNames[i]
		workerPool.ApplyOnErrorGroup(eg, func() error {
			err := store.DeleteFile(ctx, data)
			if err != nil {
				return err
			}
			return nil
		})
	}
	for i := range statNames {
		stat := statNames[i]
		workerPool.ApplyOnErrorGroup(eg, func() error {
			err := store.DeleteFile(ctx, stat)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return eg.Wait()
}

// MockExternalEngine generates an external engine with the given keys and values.
func MockExternalEngine(
	storage storage.ExternalStorage,
	keys [][]byte,
	values [][]byte,
) (dataFiles []string, statsFiles []string, err error) {
	ctx := context.Background()
	writer := NewWriterBuilder().
		SetMemorySizeLimit(128).
		SetPropSizeDistance(32).
		SetPropKeysDistance(4).
		Build(storage, "/mock-test", 0)
	kvs := make([]common.KvPair, len(keys))
	for i := range keys {
		kvs[i].Key = keys[i]
		kvs[i].Val = values[i]
	}
	rows := kv2.MakeRowsFromKvPairs(kvs)
	err = writer.AppendRows(ctx, nil, rows)
	if err != nil {
		return nil, nil, err
	}
	_, err = writer.Close(ctx)
	if err != nil {
		return nil, nil, err
	}
	return GetAllFileNames(ctx, storage, "/mock-test")
}
