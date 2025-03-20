// Copyright 2019 PingCAP, Inc.
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

// TODO: Deduplicate this implementation with DM!

package common

import (
	"context"
	"io/fs"
	"path/filepath"

	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/metrics"
	"go.uber.org/zap"
)

// StorageSize represents the storage's capacity and available size
// Learn from tidb-binlog source code.
type StorageSize struct {
	Capacity  uint64
	Available uint64
}

// CountFilesAndSize counts the number of files in a directory
// It is used for monitoring, therefore any error encountered will be ignored
func CountFilesAndSize(root string) (count int, size int) {
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fs.SkipDir
		}

		if info, infoErr := d.Info(); infoErr == nil && !d.IsDir() {
			count++
			size += int(info.Size())
		}
		return nil
	})
	if err != nil {
		log.FromContext(context.Background()).Warn("failed to walk dir", zap.String("dir", root), zap.Error(err))
	}
	return count, size
}

func init() {
	metrics.CountFilesAndSize = CountFilesAndSize
}
