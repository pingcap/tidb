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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func readAllData(
	ctx context.Context,
	store storage.ExternalStorage,
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
		if err != nil {
			output.keysPerFile = nil
			output.valuesPerFile = nil
			for _, b := range output.memKVBuffers {
				b.Destroy()
			}
			output.memKVBuffers = nil
		} else {
			// try to fix a bug that the memory is retained in http2 package
			if gcs, ok := store.(*storage.GCSStorage); ok {
				err = gcs.Reset(ctx)
			}
		}
		task.End(zap.ErrorLevel, err)
	}()

	concurrences, startOffsets, err := getFilesReadConcurrency(
		ctx,
		store,
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
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	// limit the concurrency to avoid open too many connections at the same time
	eg.SetLimit(1000)
	for i := range dataFiles {
		i := i
		eg.Go(func() error {
			err2 := readOneFile(
				egCtx,
				store,
				dataFiles[i],
				startKey,
				endKey,
				startOffsets[i],
				concurrences[i],
				bufPool,
				output,
			)
			return errors.Annotatef(err2, "failed to read file %s", dataFiles[i])
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
	droppedSize := 0

	for {
		k, v, err := rd.nextKV()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytes.Compare(k, startKey) < 0 {
			droppedSize += len(k) + len(v)
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
	output.keysPerFile = append(output.keysPerFile, keys)
	output.valuesPerFile = append(output.valuesPerFile, values)
	output.memKVBuffers = append(output.memKVBuffers, memBuf)
	output.size += size
	output.droppedSizePerFile = append(output.droppedSizePerFile, droppedSize)
	output.mu.Unlock()
	return nil
}
