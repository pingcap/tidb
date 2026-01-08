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
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"slices"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	// defaultOneWriterMemSizeLimit is the memory size limit for one writer. OneWriter can write
	// data in stream, this memory limit is only used to avoid allocating too many times
	// for each KV pair.
	defaultOneWriterMemSizeLimit uint64 = 128 * units.MiB
	// DefaultOneWriterBlockSize is the default block size for one writer.
	// TODO currently we don't have per-writer mem size limit, we always use the
	// default mem size limit as the block size.
	// it's ok for now, we can make it configurable in the future.
	DefaultOneWriterBlockSize = int(defaultOneWriterMemSizeLimit)
)

const (
	// MaxUploadPartCount defines the divisor used when calculating the size of each uploaded part.
	// Setting it from 10000 to 5000 increases the part size so that the total number of parts stays well below
	// the S3 multipart upload limit of 10,000 parts, to avoiding the error "TotalPartsExceeded: exceeded total allowed configured MaxUploadParts (10000)".
	MaxUploadPartCount = 5000
	logPartNumInterval = 999 // log the part num every 999 parts.
)

// OneFileWriter is used to write data into external storage
// with only one file for data and stat.
type OneFileWriter struct {
	// storage related.
	store    objstore.Storage
	kvStore  *KeyValueStore
	kvBuffer *membuf.Buffer

	// Statistic information per writer.
	totalSize uint64
	totalCnt  uint64
	rc        *rangePropertiesCollector

	// file information.
	writerID       string
	filenamePrefix string
	rnd            *rand.Rand
	dataFile       string
	statFile       string
	dataWriter     objstore.FileWriter
	statWriter     objstore.FileWriter

	onClose OnWriterCloseFunc
	closed  bool

	// for duplicate detection.
	onDup      engineapi.OnDuplicateKey
	pivotKey   []byte
	pivotValue []byte
	// number of key that duplicate with pivotKey, include pivotKey itself, so it
	// always >= 1 after pivotKey is set.
	currDupCnt int
	// below fields are only used when onDup is OnDuplicateKeyRecord.
	recordedDupCnt int
	dupFile        string
	dupWriter      objstore.FileWriter
	dupKVStore     *KeyValueStore

	minKey []byte
	maxKey []byte

	logger           *zap.Logger
	partSize         int64
	writtenBytes     int64
	lastLogWriteSize uint64
}

// lazyInitWriter inits the underlying dataFile/statFile path, dataWriter/statWriter
// for OneFileWriter lazily, as when OnDup=remove, the target file might be empty.
func (w *OneFileWriter) lazyInitWriter(ctx context.Context) (err error) {
	if w.dataWriter != nil {
		return nil
	}

	dataFile := filepath.Join(w.getPartitionedPrefix(), "one-file")
	dataWriter, err := w.store.Create(ctx, dataFile, &objstore.WriterOption{
		Concurrency: maxUploadWorkersPerThread,
		PartSize:    w.partSize,
	})
	if err != nil {
		return err
	}
	statFile := filepath.Join(w.getPartitionedPrefix()+statSuffix, "one-file")
	statWriter, err := w.store.Create(ctx, statFile, &objstore.WriterOption{
		Concurrency: maxUploadWorkersPerThread,
		PartSize:    MinUploadPartSize,
	})
	if err != nil {
		w.logger.Info("create stat writer failed", zap.Error(err))
		_ = dataWriter.Close(ctx)
		return err
	}
	w.logger.Info("one file writer", zap.String("data-file", dataFile),
		zap.String("stat-file", statFile), zap.Stringer("on-dup", w.onDup))

	w.dataFile, w.dataWriter = dataFile, dataWriter
	w.statFile, w.statWriter = statFile, statWriter
	w.kvStore = NewKeyValueStore(ctx, w.dataWriter, w.rc)
	return nil
}

func (w *OneFileWriter) lazyInitDupFile(ctx context.Context) error {
	if w.dupWriter != nil {
		return nil
	}

	dupFile := filepath.Join(w.getPartitionedPrefix()+dupSuffix, "one-file")
	dupWriter, err := w.store.Create(ctx, dupFile, &objstore.WriterOption{
		// too many duplicates will cause duplicate resolution part very slow,
		// we temporarily use 1 as we don't expect too many duplicates, if there
		// are, it will be slow anyway.
		// we also need to consider memory usage if we want to increase it later.
		Concurrency: 1,
		PartSize:    w.partSize})
	if err != nil {
		w.logger.Info("create dup writer failed", zap.Error(err))
		return err
	}
	w.dupFile = dupFile
	w.dupWriter = dupWriter
	w.dupKVStore = NewKeyValueStore(ctx, w.dupWriter, nil)
	return nil
}

// InitPartSizeAndLogger inits the OneFileWriter and its underlying KeyValueStore.
func (w *OneFileWriter) InitPartSizeAndLogger(ctx context.Context, partSize int64) {
	w.logger = logutil.Logger(ctx)
	w.partSize = partSize
}

// WriteRow implements ingest.Writer.
func (w *OneFileWriter) WriteRow(ctx context.Context, idxKey, idxVal []byte) error {
	defer func() {
		if (w.totalSize-w.lastLogWriteSize)/uint64(w.partSize) >= logPartNumInterval {
			w.logger.Info("one file writer progress",
				zap.String("writerID", w.writerID),
				zap.Int64("partSize", w.partSize),
				zap.Uint64("totalSize", w.totalSize),
				zap.Uint64("estimatePartNum", w.totalSize/uint64(w.partSize)),
			)
			w.lastLogWriteSize = w.totalSize
		}
	}()
	if w.onDup != engineapi.OnDuplicateKeyIgnore {
		// must be Record or Remove right now
		return w.handleDupAndWrite(ctx, idxKey, idxVal)
	}
	return w.doWriteRow(ctx, idxKey, idxVal)
}

func (w *OneFileWriter) handleDupAndWrite(ctx context.Context, idxKey, idxVal []byte) error {
	if w.currDupCnt == 0 {
		return w.onNextPivot(ctx, idxKey, idxVal)
	}
	if slices.Compare(w.pivotKey, idxKey) == 0 {
		w.currDupCnt++
		switch w.onDup {
		case engineapi.OnDuplicateKeyRecord:
			// record first 2 duplicate to data file, others to dup file.
			if w.currDupCnt == 2 {
				if err := w.doWriteRow(ctx, w.pivotKey, w.pivotValue); err != nil {
					return err
				}
				if err := w.doWriteRow(ctx, idxKey, idxVal); err != nil {
					return err
				}
			} else {
				// w.currDupCnt > 2
				if err := w.lazyInitDupFile(ctx); err != nil {
					return err
				}
				if err := w.dupKVStore.addRawKV(idxKey, idxVal); err != nil {
					return err
				}
				w.recordedDupCnt++
			}
		case engineapi.OnDuplicateKeyError:
			return common.ErrFoundDuplicateKeys.FastGenByArgs(idxKey, idxVal)
			// default is OnDuplicateKeyRemove, we will not write for duplicates.
		}
	} else {
		return w.onNextPivot(ctx, idxKey, idxVal)
	}
	return nil
}

func (w *OneFileWriter) onNextPivot(ctx context.Context, idxKey, idxVal []byte) error {
	if w.currDupCnt == 1 {
		// last pivot has no duplicate.
		if err := w.doWriteRow(ctx, w.pivotKey, w.pivotValue); err != nil {
			return err
		}
	}
	if idxKey != nil {
		w.pivotKey = slices.Clone(idxKey)
		w.pivotValue = slices.Clone(idxVal)
		w.currDupCnt = 1
	} else {
		w.pivotKey, w.pivotValue = nil, nil
		w.currDupCnt = 0
	}
	return nil
}

func (w *OneFileWriter) handlePivotOnClose(ctx context.Context) error {
	return w.onNextPivot(ctx, nil, nil)
}

func (w *OneFileWriter) doWriteRow(ctx context.Context, idxKey, idxVal []byte) error {
	if w.minKey == nil {
		w.minKey = slices.Clone(idxKey)
	}
	if err := w.lazyInitWriter(ctx); err != nil {
		return err
	}
	// 1. encode data and write to kvStore.
	keyLen := len(idxKey)
	length := len(idxKey) + len(idxVal) + lengthBytes*2
	buf, _ := w.kvBuffer.AllocBytesWithSliceLocation(length)
	if buf == nil {
		w.kvBuffer.Reset()
		buf, _ = w.kvBuffer.AllocBytesWithSliceLocation(length)
		// we now don't support KV larger than blockSize
		if buf == nil {
			return errors.Errorf("failed to allocate kv buffer: %d", length)
		}
		// 2. write statistics if one kvBuffer is used.
		w.kvStore.finish()
		encodedStat := w.rc.encode()
		_, err := w.statWriter.Write(ctx, encodedStat)
		if err != nil {
			return err
		}
		w.rc.reset()
		// the new prop should have the same offset with kvStore.
		w.rc.currProp.offset = w.kvStore.offset
	}
	encodeToBuf(buf, idxKey, idxVal)
	w.maxKey = buf[lengthBytes*2 : lengthBytes*2+keyLen]
	err := w.kvStore.addEncodedData(buf[:length])
	if err != nil {
		return err
	}
	w.totalCnt += 1
	w.totalSize += uint64(keyLen + len(idxVal))
	w.writtenBytes += int64(length)
	if w.writtenBytes >= 16*units.MiB {
		metrics.MergeSortWriteBytes.Add(float64(w.writtenBytes))
		w.writtenBytes = 0
	}
	return nil
}

// Close closes the writer.
func (w *OneFileWriter) Close(ctx context.Context) error {
	if w.closed {
		return errors.Errorf("writer %s has been closed", w.writerID)
	}
	err := w.closeImpl(ctx)
	if err != nil {
		return err
	}
	w.logger.Info("close one file writer", zap.String("writerID", w.writerID),
		zap.Uint64("totalCnt", w.totalCnt),
		zap.Uint64("totalSize", w.totalSize),
		zap.Int("recordedDupCnt", w.recordedDupCnt))

	var minKey, maxKey []byte
	mStats := make([]MultipleFilesStat, 0, 1)
	if w.totalCnt > 0 {
		// it's possible that all KV pairs are duplicates and removed.
		minKey = w.minKey
		maxKey = slices.Clone(w.maxKey)
		var stat MultipleFilesStat
		stat.Filenames = append(stat.Filenames, [2]string{w.dataFile, w.statFile})
		stat.build([]tidbkv.Key{w.minKey}, []tidbkv.Key{maxKey})
		mStats = append(mStats, stat)
	}
	conflictInfo := engineapi.ConflictInfo{}
	if w.recordedDupCnt > 0 {
		conflictInfo.Count = uint64(w.recordedDupCnt)
		conflictInfo.Files = []string{w.dupFile}
	}
	w.onClose(&WriterSummary{
		WriterID:  w.writerID,
		Seq:       0,
		Min:       minKey,
		Max:       maxKey,
		TotalSize: w.totalSize,
		TotalCnt:  w.totalCnt,
		// we only write 1 file in OneFileWriter.
		KVFileCount:        1,
		MultipleFilesStats: mStats,
		ConflictInfo:       conflictInfo,
	})
	w.totalCnt = 0
	w.totalSize = 0
	w.closed = true
	return nil
}

func (w *OneFileWriter) closeImpl(ctx context.Context) (err error) {
	if err = w.handlePivotOnClose(ctx); err != nil {
		return
	}
	if w.dataWriter != nil {
		// 1. write remaining statistic.
		w.kvStore.finish()
		encodedStat := w.rc.encode()
		_, err = w.statWriter.Write(ctx, encodedStat)
		if err != nil {
			return err
		}
		w.rc.reset()
		// 2. close data writer.
		err1 := w.dataWriter.Close(ctx)
		if err1 != nil {
			err = err1
			w.logger.Error("Close data writer failed", zap.Error(err))
			return
		}
		// 3. close stat writer.
		err2 := w.statWriter.Close(ctx)
		if err2 != nil {
			err = err2
			w.logger.Error("Close stat writer failed", zap.Error(err))
			return
		}
	}
	if w.dupWriter != nil {
		w.dupKVStore.finish()
		if err3 := w.dupWriter.Close(ctx); err3 != nil {
			err = err3
			w.logger.Error("Close dup writer failed", zap.Error(err))
			return
		}
	}
	return nil
}

func (w *OneFileWriter) getPartitionedPrefix() string {
	return randPartitionedPrefix(w.filenamePrefix, w.rnd)
}

// caller should make sure the buf is large enough to hold the encoded data.
func encodeToBuf(buf, key, value []byte) {
	intest.Assert(len(buf) == lengthBytes*2+len(key)+len(value))
	keyLen := len(key)
	binary.BigEndian.AppendUint64(buf[:0], uint64(keyLen))
	binary.BigEndian.AppendUint64(buf[lengthBytes:lengthBytes], uint64(len(value)))
	copy(buf[lengthBytes*2:], key)
	copy(buf[lengthBytes*2+keyLen:], value)
}
