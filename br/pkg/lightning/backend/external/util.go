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
