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
	"encoding/hex"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Engine stored sorted key/value pairs in an external storage.
type Engine struct {
	storage    storage.ExternalStorage
	dataFiles  []string
	statsFiles []string
}

func (e *Engine) createMergeIter(ctx context.Context, start kv.Key) (*MergeKVIter, error) {
	logger := logutil.Logger(ctx)

	var offsets []uint64
	if len(e.statsFiles) == 0 {
		offsets = make([]uint64, len(e.dataFiles))
		logger.Info("no stats files",
			zap.String("startKey", hex.EncodeToString(start)))
	} else {
		offs, err := seekPropsOffsets(ctx, start, e.statsFiles, e.storage)
		if err != nil {
			return nil, errors.Trace(err)
		}
		offsets = offs
		logger.Info("seek props offsets",
			zap.Uint64s("offsets", offsets),
			zap.String("startKey", hex.EncodeToString(start)),
			zap.Strings("dataFiles", prettyFileNames(e.dataFiles)),
			zap.Strings("statsFiles", prettyFileNames(e.statsFiles)))
	}
	iter, err := NewMergeKVIter(ctx, e.dataFiles, offsets, e.storage, 64*1024)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return iter, nil
}
