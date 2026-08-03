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

package globalsort

import (
	"bytes"
	"context"
	"encoding/hex"
	goerrors "errors"
	"io"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

const maxReadersPerCore = 16

// maxLanesPerFile bounds one file's read-ahead. Range-GET throughput on a single
// object peaks around this many concurrent requests and then degrades: measured
// against S3, one file read at 16 lanes reaches 810 MiB/s while the same read at
// 64 lanes drops to 520 MiB/s. Budget left over is better spent on more files.
// It is a var so benchmarks can sweep it.
var maxLanesPerFile = 16

// readerMemoryForRange returns the reader-buffer charge for one file's byte range
// and the number of 8 MiB range-read lanes it buys. Each lane owns a current and a
// prefetched buffer. A range too small for one lane is charged its own size and is
// read as a single prefetched stream instead.
func readerMemoryForRange(rangeSize uint64, memoryLimit int64) (int64, int) {
	if rangeSize == 0 || memoryLimit <= 0 {
		return 0, 0
	}

	laneSize := int64(2 * simplesst.ConcurrentReaderBufferSizePerConc)
	// A single charge must never exceed the budget: semaphore.Acquire blocks
	// until ctx is done when n > size.
	perFileLimit := min(int64(maxLanesPerFile)*laneSize, memoryLimit)
	requested := perFileLimit
	if rangeSize < uint64(perFileLimit) {
		requested = int64(rangeSize)
	}
	if requested < laneSize {
		return requested, 0
	}

	lanes := int(requested / laneSize)
	return int64(lanes) * laneSize, lanes
}

func readAllData(
	ctx context.Context,
	store storeapi.Storage,
	dataFiles, statsFiles []string,
	startKey, endKey []byte,
	startOffsets, estimatedEndOffsets []uint64,
	smallBlockBufPool *membuf.Pool,
	concurrency int,
	output *memKVsAndBuffers,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx), "read all data")
	task.Info("arguments",
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statsFiles)),
		zap.String("start-key", hex.EncodeToString(startKey)),
		zap.String("end-key", hex.EncodeToString(endKey)),
	)
	defer func() {
		if err != nil {
			output.kvs = nil
			output.kvsPerFile = nil
			for _, b := range output.memKVBuffers {
				b.Destroy()
			}
			output.memKVBuffers = nil
			output.size = 0
			output.droppedSize = 0
			output.droppedSizePerFile = nil
		} else {
			// try to fix a bug that the memory is retained in http2 package
			if gcs, ok := store.(*objstore.GCSStorage); ok {
				err = gcs.Reset(ctx)
			}
		}
		task.End(zap.ErrorLevel, err)
	}()

	if concurrency <= 0 {
		return errors.New("reader concurrency must be positive")
	}
	memoryLimit := readerMemoryQuotaPerCore * int64(concurrency)
	maxReaders := maxReadersPerCore * concurrency

	readerMemory := semaphore.NewWeighted(memoryLimit)
	readerMemorySizes := make([]int64, len(dataFiles))
	concurrences := make([]int, len(dataFiles))
	totalFileSize := uint64(0)
	for i := range dataFiles {
		size := estimatedEndOffsets[i] - startOffsets[i]
		totalFileSize += size
		readerMemorySizes[i], concurrences[i] = readerMemoryForRange(size, memoryLimit)
		if concurrences[i] > 0 {
			logutil.Logger(ctx).Info("found hotspot file in readAllData",
				zap.String("filename", dataFiles[i]),
				zap.Uint64("startOffset", startOffsets[i]),
				zap.Uint64("endOffset", estimatedEndOffsets[i]),
				zap.Int64("readerMemory", readerMemorySizes[i]),
				zap.Int("concurrency", concurrences[i]),
			)
		}
	}
	logutil.Logger(ctx).Info("estimated file size of this range group",
		zap.String("totalSize", units.BytesSize(float64(totalFileSize))))

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	readConn := min(maxReaders, len(dataFiles))
	taskCh := make(chan int)
	output.memKVBuffers = make([]*membuf.Buffer, readConn)
	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(simplesst.ConcurrentReaderBufferSizePerConc),
	)
	defer largeBlockBufPool.Destroy()
	for readIdx := range readConn {
		eg.Go(func() error {
			output.memKVBuffers[readIdx] = smallBlockBufPool.NewBuffer()
			smallBlockBuf := output.memKVBuffers[readIdx]

			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case fileIdx, ok := <-taskCh:
					if !ok {
						return nil
					}
					readerMemorySize := readerMemorySizes[fileIdx]
					if err := readerMemory.Acquire(egCtx, readerMemorySize); err != nil {
						return errors.Trace(err)
					}
					err2 := func() error {
						defer readerMemory.Release(readerMemorySize)
						return readOneFile(
							egCtx,
							store,
							dataFiles[fileIdx],
							startKey,
							endKey,
							startOffsets[fileIdx],
							concurrences[fileIdx],
							smallBlockBuf,
							largeBlockBufPool,
							int(readerMemorySize),
							output,
						)
					}()
					if err2 != nil {
						return errors.Annotatef(err2, "failed to read file %s", dataFiles[fileIdx])
					}
				}
			}
		})
	}

	// Dispatch small files first. semaphore.Weighted serves waiters in FIFO
	// order, so putting large readers first could otherwise make small readers
	// wait behind a whole-budget request even when enough memory is available.
	for smallFiles := true; ; smallFiles = false {
		for fileIdx := range dataFiles {
			if (concurrences[fileIdx] == 0) != smallFiles {
				continue
			}
			select {
			case <-egCtx.Done():
				return eg.Wait()
			case taskCh <- fileIdx:
			}
		}
		if !smallFiles {
			break
		}
	}
	close(taskCh)
	return eg.Wait()
}

func readOneFile(
	ctx context.Context,
	storage storeapi.Storage,
	dataFile string,
	startKey, endKey []byte,
	startOffset uint64,
	concurrency int,
	smallBlockBuf *membuf.Buffer,
	largeBlockBufPool *membuf.Pool,
	readerMemorySize int,
	output *memKVsAndBuffers,
) error {
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_one_file")

	ts := time.Now()

	prefetchSize := readerMemorySize
	if concurrency > 0 || prefetchSize < 2 {
		prefetchSize = 0
	}
	rd, err := simplesst.NewKVReaderWithPrefetchSize(
		ctx,
		dataFile,
		storage,
		startOffset,
		simplesst.DefaultReadBufferSize/3,
		prefetchSize,
	)
	if err != nil {
		return err
	}
	defer func() {
		_ = rd.Close()
	}()
	if concurrency > 0 {
		largeBlockBuf := largeBlockBufPool.NewBuffer()
		rd.EnableConcurrentRead(
			storage,
			dataFile,
			concurrency,
			simplesst.ConcurrentReaderBufferSizePerConc,
			largeBlockBuf,
		)
		err = rd.SwitchConcurrentMode(true)
		if err != nil {
			return err
		}
	}

	kvs := make([]simplesst.KVPair, 0, 1024)
	size := 0
	droppedSize := 0

	for {
		k, v, err := rd.NextKV()
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return errors.Trace(err)
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
		key, err := smallBlockBuf.TryAddBytes(k)
		if err != nil {
			return err
		}
		value, err := smallBlockBuf.TryAddBytes(v)
		if err != nil {
			return err
		}
		kvs = append(kvs, simplesst.KVPair{Key: key, Value: value})
		size += len(k) + len(v)
	}
	readAndSortDurHist.Observe(time.Since(ts).Seconds())
	output.mu.Lock()
	output.kvsPerFile = append(output.kvsPerFile, kvs)
	output.size += size
	output.droppedSizePerFile = append(output.droppedSizePerFile, droppedSize)
	output.mu.Unlock()
	return nil
}

// ReadKVFilesAsync reads multiple KV files asynchronously and sends the KV pairs
// to the returned channel, the channel will be closed when finish read.
func ReadKVFilesAsync(ctx context.Context, eg *util.ErrorGroupWithRecover,
	store storeapi.Storage, files []string) chan *simplesst.KVPair {
	pairCh := make(chan *simplesst.KVPair)
	eg.Go(func() error {
		defer close(pairCh)
		for _, file := range files {
			if err := readOneKVFile2Ch(ctx, store, file, pairCh); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return pairCh
}

func readOneKVFile2Ch(ctx context.Context, store storeapi.Storage, file string, outCh chan *simplesst.KVPair) error {
	reader, err := simplesst.NewKVReader(ctx, file, store, 0, 3*simplesst.DefaultReadBufferSize)
	if err != nil {
		return err
	}
	// if we successfully read all data, it's ok to ignore the error of Close
	//nolint: errcheck
	defer reader.Close()
	for {
		key, val, err := reader.NextKV()
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- &simplesst.KVPair{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(val),
		}:
		}
	}
	return nil
}
