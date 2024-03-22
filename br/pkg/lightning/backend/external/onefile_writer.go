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
	"path/filepath"
	"slices"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// defaultOneWriterMemSizeLimit is the memory size limit for one writer. OneWriter can write
// data in stream, this memory limit is only used to avoid allocating too many times
// for each KV pair.
var defaultOneWriterMemSizeLimit uint64 = 128 * units.MiB

// OneFileWriter is used to write data into external storage
// with only one file for data and stat.
type OneFileWriter struct {
	// storage related.
	store    storage.ExternalStorage
	kvStore  *KeyValueStore
	kvBuffer *membuf.Buffer

	// Statistic information per writer.
	totalSize uint64
	totalCnt  uint64
	rc        *rangePropertiesCollector

	// file information.
	writerID       string
	filenamePrefix string
	dataFile       string
	statFile       string
	dataWriter     storage.ExternalFileWriter
	statWriter     storage.ExternalFileWriter

	onClose OnCloseFunc
	closed  bool

	minKey []byte
	maxKey []byte

	logger *zap.Logger
}

// initWriter inits the underlying dataFile/statFile path, dataWriter/statWriter for OneFileWriter.
func (w *OneFileWriter) initWriter(ctx context.Context, partSize int64) (
	err error,
) {
	w.dataFile = filepath.Join(w.filenamePrefix, "one-file")
	w.dataWriter, err = w.store.Create(ctx, w.dataFile, &storage.WriterOption{
		Concurrency: maxUploadWorkersPerThread,
		PartSize:    partSize})
	if err != nil {
		return err
	}
	w.statFile = filepath.Join(w.filenamePrefix+statSuffix, "one-file")
	w.statWriter, err = w.store.Create(ctx, w.statFile, &storage.WriterOption{
		Concurrency: maxUploadWorkersPerThread,
		PartSize:    MinUploadPartSize})
	if err != nil {
		w.logger.Info("create stat writer failed",
			zap.Error(err))
		err = w.dataWriter.Close(ctx)
		return err
	}
	w.logger.Info("one file writer", zap.String("data-file", w.dataFile), zap.String("stat-file", w.statFile))
	return nil
}

// Init inits the OneFileWriter and its underlying KeyValueStore.
func (w *OneFileWriter) Init(ctx context.Context, partSize int64) (err error) {
	w.logger = logutil.Logger(ctx)
	err = w.initWriter(ctx, partSize)
	if err != nil {
		return err
	}
	w.kvStore, err = NewKeyValueStore(ctx, w.dataWriter, w.rc)
	return err
}

// WriteRow implements ingest.Writer.
func (w *OneFileWriter) WriteRow(ctx context.Context, idxKey, idxVal []byte) error {
	if w.minKey == nil {
		w.minKey = slices.Clone(idxKey)
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
		w.kvStore.Close()
		encodedStat := w.rc.encode()
		_, err := w.statWriter.Write(ctx, encodedStat)
		if err != nil {
			return err
		}
		w.rc.reset()
		// the new prop should have the same offset with kvStore.
		w.rc.currProp.offset = w.kvStore.offset
	}
	binary.BigEndian.AppendUint64(buf[:0], uint64(keyLen))
	binary.BigEndian.AppendUint64(buf[lengthBytes:lengthBytes], uint64(len(idxVal)))
	copy(buf[lengthBytes*2:], idxKey)
	w.maxKey = buf[lengthBytes*2 : lengthBytes*2+keyLen]
	copy(buf[lengthBytes*2+keyLen:], idxVal)
	err := w.kvStore.addEncodedData(buf[:length])
	if err != nil {
		return err
	}
	w.totalCnt += 1
	w.totalSize += uint64(keyLen + len(idxVal))
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
	w.logger.Info("close one file writer",
		zap.String("writerID", w.writerID))

	maxKey := slices.Clone(w.maxKey)
	var stat MultipleFilesStat
	stat.Filenames = append(stat.Filenames,
		[2]string{w.dataFile, w.statFile})
	stat.build([]tidbkv.Key{w.minKey}, []tidbkv.Key{maxKey})
	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		Seq:                0,
		Min:                w.minKey,
		Max:                maxKey,
		TotalSize:          w.totalSize,
		TotalCnt:           w.totalCnt,
		MultipleFilesStats: []MultipleFilesStat{stat},
	})
	w.totalCnt = 0
	w.totalSize = 0
	w.closed = true
	return nil
}

func (w *OneFileWriter) closeImpl(ctx context.Context) (err error) {
	// 1. write remaining statistic.
	w.kvStore.Close()
	encodedStat := w.rc.encode()
	_, err = w.statWriter.Write(ctx, encodedStat)
	if err != nil {
		return err
	}
	w.rc.reset()
	// 2. close data writer.
	err1 := w.dataWriter.Close(ctx)
	if err1 != nil {
		w.logger.Error("Close data writer failed", zap.Error(err))
		err = err1
		return
	}
	// 3. close stat writer.
	err2 := w.statWriter.Close(ctx)
	if err2 != nil {
		w.logger.Error("Close stat writer failed", zap.Error(err))
		err = err2
		return
	}
	return nil
}
