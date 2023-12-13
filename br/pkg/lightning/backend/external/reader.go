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

package external

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func readAllData(
	ctx context.Context,
	storage storage.ExternalStorage,
	dataFiles, statsFiles []string,
	startKey, endKey []byte,
	bufPool *membuf.Pool,
	output *memKVsAndBuffers,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx), "read all data")
	task.Info("arguments",
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statsFiles)),
		zap.Binary("start-key", startKey),
		zap.Binary("end-key", endKey),
	)
	defer func() {
		task.End(zap.ErrorLevel, err)
	}()

	concurrences, startOffsets, err := getFilesReadConcurrency(
		ctx,
		storage,
		statsFiles,
		startKey,
		endKey,
	)
	// TODO(lance6716): refine adjust concurrency
	for i, c := range concurrences {
		if c < readAllDataConcThreshold {
			concurrences[i] = 1
		}
	}

	if err != nil {
		return err
	}
	var eg errgroup.Group
	for i := range dataFiles {
		i := i
		eg.Go(func() error {
			return readOneFile(
				ctx,
				storage,
				dataFiles[i],
				startKey,
				endKey,
				startOffsets[i],
				concurrences[i],
				bufPool,
				output,
			)
		})
	}
	return eg.Wait()
}

func readOneFile(
	ctx context.Context,
	storage storage.ExternalStorage,
	dataFile string,
	startKey, endKey []byte,
	startOffset uint64,
	concurrency uint64,
	bufPool *membuf.Pool,
	output *memKVsAndBuffers,
) error {
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_one_file")

	ts := time.Now()

	rd, err := newKVReader(ctx, dataFile, storage, startOffset, 64*1024)
	if err != nil {
		return err
	}
	defer rd.Close()
	if concurrency > 1 {
		rd.byteReader.enableConcurrentRead(
			storage,
			dataFile,
			int(concurrency),
			ConcurrentReaderBufferSizePerConc,
			bufPool.NewBuffer(),
		)
		err = rd.byteReader.switchConcurrentMode(true)
		if err != nil {
			return err
		}
	}

	// this buffer is associated with data slices and will return to caller
	memBuf := bufPool.NewBuffer()
	keys := make([][]byte, 0, 1024)
	values := make([][]byte, 0, 1024)
	size := 0

	for {
		k, v, err := rd.nextKV()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytes.Compare(k, startKey) < 0 {
			continue
		}
		if bytes.Compare(k, endKey) >= 0 {
			break
		}
		// TODO(lance6716): we are copying every KV from rd's buffer to memBuf, can we
		// directly read into memBuf?
		keys = append(keys, memBuf.AddBytes(k))
		values = append(values, memBuf.AddBytes(v))
		size += len(k) + len(v)
	}
	readAndSortDurHist.Observe(time.Since(ts).Seconds())
	output.mu.Lock()
	output.keys = append(output.keys, keys...)
	output.values = append(output.values, values...)
	output.memKVBuffers = append(output.memKVBuffers, memBuf)
	output.size += size
	output.mu.Unlock()
	return nil
}
