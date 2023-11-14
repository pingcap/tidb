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
	"encoding/binary"
	"encoding/hex"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// OneFileWriter is used to write data into external storage with one file.
type OneFileWriter struct {
	store          storage.ExternalStorage
	writerID       string
	filenamePrefix string
	keyAdapter     common.KeyAdapter

	kvStore *KeyValueStore
	rc      *rangePropertiesCollector

	kvBuffer    *preAllocKVBuf
	kvLocations []kvLocation
	kvSize      int64

	onClose OnCloseFunc
	closed  bool

	// Statistic information per batch.
	batchSize uint64

	multiFileStat MultipleFilesStat
	// Statistic information per writer.
	minKey    tidbkv.Key
	maxKey    tidbkv.Key
	totalSize uint64

	dataFile   string
	statFile   string
	dataWriter storage.ExternalFileWriter
	statWriter storage.ExternalFileWriter
	logger     *zap.Logger
	useSort    bool
}

func (w *OneFileWriter) initWriter(ctx context.Context) (
	err error,
) {
	w.dataFile = filepath.Join(w.filenamePrefix, strconv.Itoa(0))
	w.dataWriter, err = w.store.Create(ctx, w.dataFile, &storage.WriterOption{Concurrency: 20, PartSize: (int64)(50 * size.MB)})
	if err != nil {
		return err
	}
	w.statFile = filepath.Join(w.filenamePrefix+statSuffix, strconv.Itoa(0))
	w.statWriter, err = w.store.Create(ctx, w.statFile, &storage.WriterOption{Concurrency: 20, PartSize: (int64)(5 * size.MB)})
	if err != nil {
		_ = w.dataWriter.Close(ctx)
		return err
	}
	logutil.BgLogger().Info("one file writer", zap.String("data-file", w.dataFile), zap.String("stat-file", w.statFile))
	return nil
}

func (w *OneFileWriter) Init(ctx context.Context) (err error) {
	err = w.initWriter(ctx)
	w.logger = logutil.Logger(ctx)
	if err != nil {
		return err
	}
	w.kvStore, err = NewKeyValueStore(ctx, w.dataWriter, w.rc)
	return err
}

// WriteRow implements ingest.Writer.
func (w *OneFileWriter) WriteRow(ctx context.Context, idxKey, idxVal []byte, handle tidbkv.Handle) error {
	keyAdapter := w.keyAdapter

	var rowID []byte
	if handle != nil {
		rowID = handle.Encoded()
	}
	encodedKeyLen := keyAdapter.EncodedLen(idxKey, rowID)
	length := encodedKeyLen + len(idxVal) + lengthBytes*2
	blockIdx, dataBuf, off, allocated := w.kvBuffer.Alloc(length)
	if !allocated {
		if err := w.flushKVs(ctx, false); err != nil {
			return err
		}
		blockIdx, dataBuf, off, allocated = w.kvBuffer.Alloc(length)
		// we now don't support KV larger than blockSize
		if !allocated {
			return errors.Errorf("failed to allocate kv buffer: %d", length)
		}
	}
	binary.BigEndian.AppendUint64(dataBuf[:0], uint64(encodedKeyLen))
	keyAdapter.Encode(dataBuf[lengthBytes:lengthBytes:lengthBytes+encodedKeyLen], idxKey, rowID)
	binary.BigEndian.AppendUint64(dataBuf[lengthBytes+encodedKeyLen:lengthBytes+encodedKeyLen], uint64(len(idxVal)))
	copy(dataBuf[lengthBytes*2+encodedKeyLen:], idxVal)

	w.kvLocations = append(w.kvLocations, kvLocation{
		blockIdx: blockIdx,
		offset:   off,
		length:   int32(length)},
	)
	w.kvSize += int64(encodedKeyLen + len(idxVal))
	w.batchSize += uint64(length)
	return nil
}

// LockForWrite implements ingest.Writer.
// Since flushKVs is thread-safe in external storage writer,
// this is implemented as noop.
func (w *OneFileWriter) LockForWrite() func() {
	return func() {}
}

// Close closes the writer.
func (w *OneFileWriter) Close(ctx context.Context) error {
	if w.closed {
		return errors.Errorf("writer %s has been closed", w.writerID)
	}
	w.closed = true
	defer w.kvBuffer.destroy()
	err := w.flushKVs(ctx, true)
	if err != nil {
		return err
	}
	w.logger.Info("close one file writer",
		zap.String("writerID", w.writerID),
		zap.Int("kv-cnt-cap", cap(w.kvLocations)),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	w.kvLocations = nil

	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		Seq:                0,
		Min:                w.minKey,
		Max:                w.maxKey,
		TotalSize:          w.totalSize,
		MultipleFilesStats: []MultipleFilesStat{w.multiFileStat},
	})
	return nil
}

func (w *OneFileWriter) recordMinMax(newMin, newMax tidbkv.Key, size uint64) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
	w.totalSize += size
}

func (w *OneFileWriter) flushKVs(ctx context.Context, fromClose bool) (err error) {
	if len(w.kvLocations) == 0 {
		return nil
	}
	var (
		savedBytes   uint64
		sortDuration time.Duration
	)

	savedBytes = w.batchSize

	if w.useSort {
		sortStart := time.Now()
		slices.SortFunc(w.kvLocations, func(i, j kvLocation) int {
			return bytes.Compare(w.getKeyByLoc(i), w.getKeyByLoc(j))
		})
		sortDuration = time.Since(sortStart)
		metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort").Observe(sortDuration.Seconds())
		metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort").Observe(float64(savedBytes) / 1024.0 / 1024.0 / sortDuration.Seconds())
	}
	for _, pair := range w.kvLocations {
		err = w.kvStore.addEncodedData(w.getEncodedKVData(pair))
		if err != nil {
			return err
		}
	}

	w.kvStore.Close()
	encodedStat := w.rc.encode()
	_, err = w.statWriter.Write(ctx, encodedStat)
	if err != nil {
		return err
	}

	minKey, maxKey := w.getKeyByLoc(w.kvLocations[0]), w.getKeyByLoc(w.kvLocations[len(w.kvLocations)-1])
	w.recordMinMax(minKey, maxKey, uint64(w.kvSize))

	if fromClose {
		w.multiFileStat.Filenames = append(w.multiFileStat.Filenames,
			[2]string{w.dataFile, w.statFile},
		)
		w.multiFileStat.build(
			[]tidbkv.Key{w.minKey},
			[]tidbkv.Key{w.maxKey})

		err1 := w.dataWriter.Close(ctx)
		if err1 != nil {
			w.logger.Error("Close data writer failed", zap.Error(err))
			err = err1
			return
		}

		err2 := w.statWriter.Close(ctx)
		if err2 != nil {
			w.logger.Error("Close stat writer failed", zap.Error(err))
			err = err2
			return
		}
	}
	w.kvLocations = w.kvLocations[:0]
	w.kvSize = 0
	w.kvBuffer.reset()
	w.rc.reset()
	w.batchSize = 0
	return nil
}

func (w *OneFileWriter) getEncodedKVData(pos kvLocation) []byte {
	block := w.kvBuffer.blocks[pos.blockIdx]
	return block[pos.offset : pos.offset+pos.length]
}

func (w *OneFileWriter) getKeyByLoc(pos kvLocation) []byte {
	block := w.kvBuffer.blocks[pos.blockIdx]
	keyLen := binary.BigEndian.Uint64(block[pos.offset : pos.offset+lengthBytes])
	return block[pos.offset+lengthBytes : uint64(pos.offset)+lengthBytes+keyLen]
}
