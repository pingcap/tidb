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
	"encoding/json"
	goerrors "errors"
	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
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
		zap.Strings("data-files", dataFiles),
		zap.Strings("stat-files", statsFiles),
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

func HandleIndexStats(sctx sessionctx.Context, ctx context.Context, idxID int64, cloudStoreURI string) error {
	totalCnt := 0
	ts := time.Now()
	sampledKVs := make([]KVPair, 0)
	var dataFiles []string
	defer func() {
		logutil.BgLogger().Info("read all sampled kv files completed",
			zap.Int("total-kv-count", totalCnt),
			zap.Int("data-file-count", len(dataFiles)),
			zap.Int("sampledKVs", len(sampledKVs)),
			zap.Duration("duration", time.Since(ts)))
	}()
	memLimiter := membuf.NewLimiter(5 * units.GB)
	smallBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithPoolMemoryLimiter(memLimiter),
		membuf.WithBlockSize(smallBlockSize))

	largeBlockBufPool := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithPoolMemoryLimiter(memLimiter),
		membuf.WithBlockSize(ConcurrentReaderBufferSizePerConc))

	_, storage, err := handle.NewObjStoreWithRecording(ctx, cloudStoreURI)
	if err != nil {
		return err
	}
	defer func() {
		storage.Close()
	}()

	// get sampled kv file path
	files, err := GetAllFileNames(ctx, storage, disttaskutil.StatsSampled)
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("read sampled kv files", zap.Strings("files", files))

	for _, file := range files {
		if !strings.Contains(file, "stat") {
			dataFiles = append(dataFiles, file)
		}
	}
	for _, dataFile := range dataFiles {
		cnt, kvs, err := readOneSampledFile(ctx, storage, dataFile, smallBlockBufPool.NewBuffer(), largeBlockBufPool.NewBuffer())
		if err != nil {
			return err
		}
		sampledKVs = append(sampledKVs, kvs...)
		totalCnt += cnt
	}

	// decode and calculate stats from sampledKVs
	sampleItems := make([]*statistics.SampleItem, 0, len(sampledKVs))
	for _, kv := range sampledKVs {
		// decode index key
		tableID, idxID, idxVal, err := tablecodec.DecodeIndexKey(kv.Key)
		if err != nil {
			return err
		}
		logutil.BgLogger().Debug("sampled kv example",
			zap.Int64("tableID", tableID),
			zap.Int64("idxID", idxID),
			zap.Strings("idxVal", idxVal),
		)
		i, err := strconv.ParseInt(idxVal[0], 10, 64)
		if err != nil {
			return err
		}
		d := types.NewIntDatum(i)
		b := make([]byte, 0, 8)
		b, err = codec.EncodeKey(time.UTC, b, d)
		if err != nil {
			return err
		}
		sampleItems = append(sampleItems, &statistics.SampleItem{
			Value: types.NewBytesDatum(b),
		})
	}

	// read fms from s3 and merge
	var sub statistics.SubStats
	files, err = GetAllFileNames(ctx, storage, "fms")
	if err != nil {
		return err
	}
	logutil.BgLogger().Info("read fms file", zap.Strings("file", files))
	fullFms := statistics.NewFMSketch(statistics.MaxSketchSize)
	nullCnt := int64(0)
	notNullCnt := int64(0)
	totalSize := int64(0)
	for _, file := range files {
		data, err := storage.ReadFile(ctx, file)
		if err != nil {
			return errors.Trace(err)
		}
		//fms, err := statistics.DecodeFMSketch(data)
		err = json.Unmarshal(data, &sub)
		if err != nil {
			return errors.Trace(err)
		}
		fullFms.MergeFMSketch(sub.Fms)
		nullCnt += sub.NullCnt
		notNullCnt += sub.NotNullCnt
		totalSize += sub.TotalSize
	}
	if fullFms.NDV() < 100 {
		ks, vs := fullFms.KV()
		logutil.BgLogger().Info("fms sketch example", zap.Uint64s("ks", ks), zap.Bools("vs", vs))
	}

	// build hist, topn
	collector := &statistics.SampleCollector{
		Samples:   sampleItems,
		NullCount: nullCnt,
		Count:     notNullCnt,
		FMSketch:  fullFms,
		TotalSize: totalSize,
		MemSize:   int64(len(sampleItems)) * (8 + statistics.EmptySampleItemSize + 8),
	}
	tp := types.NewFieldType(mysql.TypeBlob)
	hist, topn, err := statistics.BuildHistAndTopN(sctx,
		statistics.DefaultHistogramBuckets,
		statistics.DefaultTopNValue,
		idxID, collector, tp, false, nil, false)

	var bounds []int64
	//iter := chunk.NewIterator4Chunk(hist.Bounds)
	//for row := iter.Begin(); row != iter.End(); row = iter.Next() {
	//	remain, d, err := codec.DecodeOne(row.GetBytes(0))
	//	if err != nil {
	//		panic(err)
	//	}
	//	if remain != nil {
	//		panic("should be nil")
	//	}
	//	bounds = append(bounds, d.GetInt64())
	//}
	logutil.BgLogger().Info("build hist, topn",
		zap.Int("items-count", len(sampleItems)),
		zap.Int64("fms-count", fullFms.NDV()),
		zap.Any("hist", hist),
		zap.Any("topn", topn),
		zap.Int("topn", topn.Num()),
		zap.Int64("null-count", nullCnt),
		zap.Int64("not-null-count", notNullCnt),
		zap.Int64("total-size", totalSize),
		zap.Int("bound-len", len(bounds)),
		zap.Int64s("bound", bounds),
	)

	return nil
}

func readOneSampledFile(ctx context.Context, storage storage.ExternalStorage, dataFile string,
	smallBlockBuf *membuf.Buffer, largeBlockBuf *membuf.Buffer) (int, []KVPair, error) {
	kvCnt := 0
	startOffset := uint64(0)
	concurrency := uint64(1)

	rd, err := NewKVReader(ctx, dataFile, storage, startOffset, 64*1024)
	if err != nil {
		return 0, nil, err
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
			return 0, nil, err
		}
	}
	kvs := make([]KVPair, 0, 1024)
	size := 0

	for {
		k, v, err := rd.NextKV()
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return 0, nil, errors.Trace(err)
		}
		kvs = append(kvs, KVPair{Key: smallBlockBuf.AddBytes(k), Value: smallBlockBuf.AddBytes(v)})
		size += len(k) + len(v)
		kvCnt++
	}
	return kvCnt, kvs, nil
}

// ReadKVFilesAsync reads multiple KV files asynchronously and sends the KV pairs
// to the returned channel, the channel will be closed when finish read.
func ReadKVFilesAsync(ctx context.Context, eg *util.ErrorGroupWithRecover,
	store storage.ExternalStorage, files []string) chan *KVPair {
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

func readOneKVFile2Ch(ctx context.Context, store storage.ExternalStorage, file string, outCh chan *KVPair) error {
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
