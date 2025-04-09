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
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
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
	log.FromContext(ctx).Info("readAllData info",
		zap.String("start-key", string(startKey)),
		zap.String("end-key", string(endKey)),
		zap.String("startKey", hex.EncodeToString(startKey)),
		zap.String("endKey", hex.EncodeToString(endKey)),
		zap.Strings("dataFiles", dataFiles),
		zap.Strings("statsFiles", statsFiles),
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

	concurrences, startOffsets, err, _ := getFilesReadConcurrency(
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
	task.Info("read file go routine num", zap.Int("readConn", readConn))
	for readIdx := 0; readIdx < readConn; readIdx++ {
		readIdx := readIdx
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
					//if !notSkip[fileIdx] {
					// skip the file if it is not in the range
					//continue
					//}
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
	storage storage.ExternalStorage,
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
			largeBlockBuf,
		)
		err = rd.byteReader.switchConcurrentMode(true)
		if err != nil {
			return err
		}
	}

	keys := make([][]byte, 0, 1024)
	values := make([][]byte, 0, 1024)
	size := 0
	droppedSize := 0

	cntAllKeys := 0
	cntDropped := 0
	var firstKey, lastKey []byte
	for {
		k, v, err := rd.nextKV()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if cntAllKeys == 0 {
			firstKey = make([]byte, len(k))
			copy(firstKey, k)
		}
		lastKey = make([]byte, len(k))
		copy(lastKey, k)
		cntAllKeys++
		if bytes.Compare(k, startKey) < 0 {
			droppedSize += len(k) + len(v)
			cntDropped++
			continue
		}
		if bytes.Compare(k, endKey) >= 0 {
			cntDropped++
			continue
		}
		// TODO(lance6716): we are copying every KV from rd's buffer to memBuf, can we
		// directly read into memBuf?
		keys = append(keys, smallBlockBuf.AddBytes(k))
		values = append(values, smallBlockBuf.AddBytes(v))
		size += len(k) + len(v)
	}
	readAndSortDurHist.Observe(time.Since(ts).Seconds())
	output.mu.Lock()
	output.keysPerFile = append(output.keysPerFile, keys)
	output.valuesPerFile = append(output.valuesPerFile, values)
	output.size += size
	output.droppedSizePerFile = append(output.droppedSizePerFile, droppedSize)
	output.mu.Unlock()
	//if droppedSize != 0 {
	//	rd.byteReader.logger.Info("readOneFile",
	//		zap.String("first key", hex.EncodeToString(firstKey)),
	//		zap.String("last key", hex.EncodeToString(lastKey)),
	//		zap.Int("keyNumInFile", cntAllKeys),
	//		zap.Int("keyNumInMemBuf", len(keys)),
	//		zap.Int("dropped num", cntDropped),
	//		zap.Int("dropped size", droppedSize),
	//		zap.String("data-file-name", dataFile),
	//	)
	//}
	return nil
}
