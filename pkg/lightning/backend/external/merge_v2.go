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
	"math"
	"time"

	"github.com/jfcg/sorty/v2"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// MergeOverlappingFilesV2 reads from given files whose key range may overlap
// and writes to new sorted, nonoverlapping files.
// Using 1 readAllData and 1 writer.
func MergeOverlappingFilesV2(
	ctx context.Context,
	multiFileStat []MultipleFilesStat,
	store storage.ExternalStorage,
	startKey []byte,
	endKey []byte,
	partSize int64,
	newFilePrefix string,
	writerID string,
	blockSize int,
	writeBatchCount uint64,
	propSizeDist uint64,
	propKeysDist uint64,
	onClose OnCloseFunc,
	concurrency int,
	checkHotspot bool,
) (err error) {
	fileCnt := 0
	for _, m := range multiFileStat {
		fileCnt += len(m.Filenames)
	}
	task := log.BeginTask(logutil.Logger(ctx).With(
		zap.Int("file-count", fileCnt),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
		zap.String("new-file-prefix", newFilePrefix),
		zap.Int("concurrency", concurrency),
	), "merge overlapping files")
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	rangesGroupSize := 4 * size.GB
	failpoint.Inject("mockRangesGroupSize", func(val failpoint.Value) {
		rangesGroupSize = uint64(val.(int))
	})

	splitter, err := NewRangeSplitter(
		ctx,
		multiFileStat,
		store,
		int64(rangesGroupSize),
		math.MaxInt64,
		int64(4*size.GB),
		math.MaxInt64,
	)
	if err != nil {
		return err
	}

	writer := NewWriterBuilder().
		SetMemorySizeLimit(DefaultMemSizeLimit).
		SetBlockSize(blockSize).
		SetPropKeysDistance(propKeysDist).
		SetPropSizeDistance(propSizeDist).
		SetOnCloseFunc(onClose).
		BuildOneFile(store, newFilePrefix, writerID)
	defer func() {
		err = splitter.Close()
		if err != nil {
			logutil.Logger(ctx).Warn("close range splitter failed", zap.Error(err))
		}
		err = writer.Close(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("close writer failed", zap.Error(err))
		}
	}()

	err = writer.Init(ctx, partSize)
	if err != nil {
		logutil.Logger(ctx).Warn("init writer failed", zap.Error(err))
		return
	}

	bufPool := membuf.NewPool()
	loaded := &memKVsAndBuffers{}
	curStart := kv.Key(startKey).Clone()
	var curEnd kv.Key

	for {
		endKeyOfGroup, dataFilesOfGroup, statFilesOfGroup, _, err1 := splitter.SplitOneRangesGroup()
		if err1 != nil {
			logutil.Logger(ctx).Warn("split one ranges group failed", zap.Error(err1))
			return
		}
		curEnd = kv.Key(endKeyOfGroup).Clone()
		if len(endKeyOfGroup) == 0 {
			curEnd = kv.Key(endKey).Clone()
		}
		now := time.Now()
		err1 = readAllData(
			ctx,
			store,
			dataFilesOfGroup,
			statFilesOfGroup,
			curStart,
			curEnd,
			bufPool,
			bufPool,
			loaded,
		)
		if err1 != nil {
			logutil.Logger(ctx).Warn("read all data failed", zap.Error(err1))
			return
		}
		loaded.build(ctx)
		readTime := time.Since(now)
		now = time.Now()
		sorty.MaxGor = uint64(concurrency)
		sorty.Sort(len(loaded.keys), func(i, k, r, s int) bool {
			if bytes.Compare(loaded.keys[i], loaded.keys[k]) < 0 { // strict comparator like < or >
				if r != s {
					loaded.keys[r], loaded.keys[s] = loaded.keys[s], loaded.keys[r]
					loaded.values[r], loaded.values[s] = loaded.values[s], loaded.values[r]
				}
				return true
			}
			return false
		})
		sortTime := time.Since(now)
		now = time.Now()
		for i, key := range loaded.keys {
			err1 = writer.WriteRow(ctx, key, loaded.values[i])
			if err1 != nil {
				logutil.Logger(ctx).Warn("write one row to writer failed", zap.Error(err1))
				return
			}
		}
		writeTime := time.Since(now)
		logutil.Logger(ctx).Info("sort one group in MergeOverlappingFiles",
			zap.Duration("read time", readTime),
			zap.Duration("sort time", sortTime),
			zap.Duration("write time", writeTime),
			zap.Int("key len", len(loaded.keys)))

		curStart = curEnd.Clone()
		loaded.keys = nil
		loaded.values = nil
		loaded.memKVBuffers = nil
		loaded.size = 0

		if len(endKeyOfGroup) == 0 {
			break
		}
	}
	return
}
