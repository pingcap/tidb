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
	"encoding/hex"
	goerrors "errors"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func readAllData(
	ctx context.Context,
	store storeapi.Storage,
	dataFiles, statsFiles []string,
	startKey, endKey []byte,
	smallBlockBufPool *membuf.Pool,
	largeBlockBufPool *membuf.Pool,
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
			output.kvsPerFile = nil
			for _, b := range output.memKVBuffers {
				b.Destroy()
			}
			output.memKVBuffers = nil
		} else {
			// try to fix a bug that the memory is retained in http2 package
			if gcs, ok := store.(*objstore.GCSStorage); ok {
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
	if err != nil {
		return err
	}

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	readConn := 1000
	readConn = min(readConn, len(dataFiles))
	taskCh := make(chan int)
	output.memKVBuffers = make([]*membuf.Buffer, readConn*2)
	for readIdx := range readConn {
		eg.Go(func() error {
			output.memKVBuffers[readIdx] = smallBlockBufPool.NewBuffer()
			output.memKVBuffers[readIdx+readConn] = largeBlockBufPool.NewBuffer()
			smallBlockBuf := output.memKVBuffers[readIdx]
			largeBlockBuf := output.memKVBuffers[readIdx+readConn]

			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case fileIdx, ok := <-taskCh:
					if !ok {
						return nil
					}
					err2 := readOneFile(
						egCtx,
						store,
						dataFiles[fileIdx],
						startKey,
						endKey,
						startOffsets[fileIdx],
						concurrences[fileIdx],
						smallBlockBuf,
						largeBlockBuf,
						output,
					)
					if err2 != nil {
						return errors.Annotatef(err2, "failed to read file %s", dataFiles[fileIdx])
					}
				}
			}
		})
	}

	for fileIdx := range dataFiles {
		select {
		case <-egCtx.Done():
			return eg.Wait()
		case taskCh <- fileIdx:
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
	concurrency uint64,
	smallBlockBuf *membuf.Buffer,
	largeBlockBuf *membuf.Buffer,
	output *memKVsAndBuffers,
) error {
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_one_file")

	ts := time.Now()

	rd, err := NewKVReader(ctx, dataFile, storage, startOffset, 64*1024)
	if err != nil {
		return err
	}
	defer func() {
		rd.Close()
	}()
	if concurrency > 1 {
		rd.byteReader.enableConcurrentRead(
			storage,
			dataFile,
			int(concurrency),
			ConcurrentReaderBufferSizePerConc,
			largeBlockBuf,
		)
		err = rd.byteReader.switchConcurrentMode(true)
		if err != nil {
			return err
		}
	}

	kvs := make([]KVPair, 0, 1024)
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
		kvs = append(kvs, KVPair{Key: smallBlockBuf.AddBytes(k), Value: smallBlockBuf.AddBytes(v)})
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
	store storeapi.Storage, files []string) chan *KVPair {
	pairCh := make(chan *KVPair)
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

func readOneKVFile2Ch(ctx context.Context, store storeapi.Storage, file string, outCh chan *KVPair) error {
	reader, err := NewKVReader(ctx, file, store, 0, 3*DefaultReadBufferSize)
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
		case outCh <- &KVPair{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(val),
		}:
		}
	}
	return nil
}
