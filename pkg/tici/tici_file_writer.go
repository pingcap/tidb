// Copyright 2025 PingCAP, Inc.
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

package tici

import (
	"context"
	"encoding/binary"
	"net/url"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// maxUploadWorkersPerThread defines the maximum number of upload workers per thread.
// This variable refers to maxUploadWorkersPerThread in the external package.
var maxUploadWorkersPerThread = 8

// ticiFileWriterMemSizeLimit is the memory buffer size limit for TICIFileWriter.
// This buffer is used to avoid frequent allocations for each KV pair.
var ticiFileWriterMemSizeLimit uint64 = 128 * units.MiB

// TiCIMinUploadPartSize defines the minimum upload part size for external storage multipart uploads.
// Both S3 and GCS require a minimum part size of 5MiB.
var TiCIMinUploadPartSize int64 = 5 * units.MiB

const (
	// ticiFileFormatVersion defines the version of the TiCI file format.
	// This is used to ensure compatibility with future versions of the TiCI file format.
	// If the format changes, this version should be incremented.
	ticiFileFormatVersion uint8 = 1

	// we use uint64 to store the length of key and value.
	lengthBytes = 8

	// DefaultBlockSize is the default block size for writer.
	DefaultBlockSize = 16 * units.MiB
)

// FileWriter writes data to a fixed S3 location.
type FileWriter struct {
	store         storage.ExternalStorage
	dataFile      string
	dataWriter    storage.ExternalFileWriter
	kvBuffer      *membuf.Buffer
	totalSize     uint64
	totalCnt      uint64
	closed        bool
	logger        *zap.Logger
	partSize      int64
	headerWritten bool
}

// NewTICIFileWriter creates a new TICIFileWriter.
// dataFile is the file name we are writing into.
func NewTICIFileWriter(ctx context.Context, store storage.ExternalStorage, dataFile string, partSize int64, logger *zap.Logger) (*FileWriter, error) {
	dataWriter, err := store.Create(ctx, dataFile, &storage.WriterOption{
		// TODO: maxUploadWorkersPerThread is subject to change up to the test results and performance tuning with TiCI worker.
		// For now, we set it to maxUploadWorkersPerThread by default.
		Concurrency: maxUploadWorkersPerThread,
		PartSize:    partSize,
	})
	if err != nil {
		return nil, err
	}
	logger.Info("create TiCI file writer", zap.String("dataFile", dataFile))
	p := membuf.NewPool(membuf.WithBlockNum(0), membuf.WithBlockSize(int(DefaultBlockSize)))
	return &FileWriter{
		store:      store,
		dataFile:   dataFile,
		dataWriter: dataWriter,
		kvBuffer:   p.NewBuffer(membuf.WithBufferMemoryLimit(ticiFileWriterMemSizeLimit)),
		logger:     logger,
		partSize:   partSize,
	}, nil
}

// URI returns the URI of the key stored in external storage.
func (w *FileWriter) URI() (string, error) {
	return url.JoinPath(w.store.URI(), w.dataFile)
}

// WriteRow writes a key-value pair to the S3 file.
func (w *FileWriter) WriteRow(ctx context.Context, idxKey, idxVal []byte) error {
	length := len(idxKey) + len(idxVal) + lengthBytes*2
	buf, _ := w.kvBuffer.AllocBytesWithSliceLocation(length)
	if buf == nil {
		w.kvBuffer.Reset()
		buf, _ = w.kvBuffer.AllocBytesWithSliceLocation(length)
		if buf == nil {
			return errors.Errorf("tici_writer failed to allocate kv buffer: %d", length)
		}
	}

	encodeKVForTICI(buf, idxKey, idxVal)
	writeStartTime := time.Now()
	_, err := w.dataWriter.Write(ctx, buf[:length])
	if err != nil {
		return err
	}
	w.totalCnt++
	w.totalSize += uint64(len(idxKey) + len(idxVal))
	writeDuration := time.Since(writeStartTime)

	// Set up metrics for the tici write operation. Use the label "tici_file_write" to distinguish it from other write operations.
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("tici_file_write").Observe(writeDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("tici_file_write").
		Observe(float64(length) / 1024.0 / 1024.0 / writeDuration.Seconds())
	return nil
}

// Close closes the writer.
func (w *FileWriter) Close(ctx context.Context) error {
	if w.closed {
		return errors.Errorf("TICIFileWriter already closed")
	}
	err := w.dataWriter.Close(ctx)
	if err != nil {
		return err
	}
	w.logger.Info("close tici file writer", zap.String("dataFile", w.dataFile),
		zap.Uint64("totalCnt", w.totalCnt),
		zap.Uint64("totalSize", w.totalSize))
	w.closed = true
	return nil
}

// WriteHeader serializes and writes the header containing TableInfo, IndexInfo,
// PKIndexInfo, and CommitTS.
// The format is:
// [1 byte]  Format version
// [8 bytes] Length of TableInfo (big endian uint64)
// [N bytes] TableInfo (serialized)
// [8 bytes] CommitTS (big endian uint64)
func (w *FileWriter) WriteHeader(
	ctx context.Context,
	tblInBytes []byte,
	commitTS uint64,
) error {
	if w.headerWritten {
		return errors.New("TICIFileWriter header already written")
	}
	if w.dataWriter == nil {
		return errors.New("TICIFileWriter dataWriter is nil")
	}

	headerLen := 1 + 8 + len(tblInBytes) + 8
	header := make([]byte, headerLen)
	off := 0

	header[off] = ticiFileFormatVersion
	off += 1

	binary.BigEndian.PutUint64(header[off:], uint64(len(tblInBytes)))
	off += 8
	copy(header[off:], tblInBytes)
	off += len(tblInBytes)

	binary.BigEndian.PutUint64(header[off:], commitTS)
	// off += 8 // for the commitTS, we are using another 8 bytes

	_, err := w.dataWriter.Write(ctx, header)
	if err != nil {
		return errors.New("write header: " + err.Error())
	}
	w.headerWritten = true
	return nil
}

// encodeKVForTICI encodes a key-value pair for TICIFileWriter.
// This encode process is subject to future modification and improvement.
// Format: [keyLen(8)][valLen(8)][key][value]
func encodeKVForTICI(buf, key, value []byte) {
	intest.Assert(len(buf) == lengthBytes*2+len(key)+len(value))
	keyLen := len(key)
	binary.BigEndian.AppendUint64(buf[:0], uint64(keyLen))
	binary.BigEndian.AppendUint64(buf[lengthBytes:lengthBytes], uint64(len(value)))
	copy(buf[lengthBytes*2:], key)
	copy(buf[lengthBytes*2+keyLen:], value)
}
