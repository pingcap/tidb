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
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (w *Writer) flushSortedKVs(ctx context.Context, dupLocs []membuf.SliceLocation) (string, string, string, error) {
	logger := logutil.Logger(ctx).With(
		zap.String("writer-id", w.writerID),
		zap.Int("sequence-number", w.currentSeq),
	)
	writeStartTime := time.Now()
	dataFile, statFile, dataWriter, statWriter, err := w.createStorageWriter(ctx)
	if err != nil {
		return "", "", "", err
	}
	defer func() {
		// close the writers when meet error. If no error happens, writers will
		// be closed outside and assigned to nil.
		if dataWriter != nil {
			_ = dataWriter.Close(ctx)
		}
		if statWriter != nil {
			_ = statWriter.Close(ctx)
		}
	}()
	w.rc.reset()
	kvStore := NewKeyValueStore(ctx, dataWriter, w.rc)

	for _, pair := range w.kvLocations {
		err = kvStore.addEncodedData(w.kvBuffer.GetSlice(&pair))
		if err != nil {
			return "", "", "", err
		}
	}

	kvStore.finish()
	encodedStat := w.rc.encode()
	statSize := len(encodedStat)
	_, err = statWriter.Write(ctx, encodedStat)
	if err != nil {
		return "", "", "", err
	}
	err = dataWriter.Close(ctx)
	dataWriter = nil
	if err != nil {
		return "", "", "", err
	}
	err = statWriter.Close(ctx)
	statWriter = nil
	if err != nil {
		return "", "", "", err
	}

	var dupPath string
	if len(dupLocs) > 0 {
		dupPath, err = w.writeDupKVs(ctx, dupLocs)
		if err != nil {
			return "", "", "", err
		}
	}

	writeDuration := time.Since(writeStartTime)
	logger.Info("flush sorted kv",
		zap.Uint64("bytes", w.batchSize),
		zap.Int("stat-size", statSize),
		zap.Duration("write-time", writeDuration),
		zap.String("write-speed(bytes/s)", getSpeed(w.batchSize, writeDuration.Seconds(), true)),
	)
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("write").Observe(writeDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("write").Observe(float64(w.batchSize) / 1024.0 / 1024.0 / writeDuration.Seconds())

	return dataFile, statFile, dupPath, nil
}

func (w *Writer) writeDupKVs(ctx context.Context, kvLocs []membuf.SliceLocation) (string, error) {
	dupPath, dupWriter, err := w.createDupWriter(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		// close the writers when meet error. If no error happens, writers will
		// be closed outside and assigned to nil.
		if dupWriter != nil {
			_ = dupWriter.Close(ctx)
		}
	}()
	dupStore := NewKeyValueStore(ctx, dupWriter, nil)
	for _, pair := range kvLocs {
		err = dupStore.addEncodedData(w.kvBuffer.GetSlice(&pair))
		if err != nil {
			return "", err
		}
	}
	dupStore.finish()
	err = dupWriter.Close(ctx)
	dupWriter = nil
	if err != nil {
		return "", err
	}
	return dupPath, nil
}

func (w *Writer) getKeyByLoc(loc *membuf.SliceLocation) []byte {
	block := w.kvBuffer.GetSlice(loc)
	keyLen := binary.BigEndian.Uint64(block[:lengthBytes])
	return block[2*lengthBytes : 2*lengthBytes+keyLen]
}

func (w *Writer) getValueByLoc(loc *membuf.SliceLocation) []byte {
	block := w.kvBuffer.GetSlice(loc)
	keyLen := binary.BigEndian.Uint64(block[:lengthBytes])
	return block[2*lengthBytes+keyLen:]
}

func (w *Writer) reCalculateKVSize() int64 {
	s := int64(0)
	for _, loc := range w.kvLocations {
		s += int64(loc.Length) - 2*lengthBytes
	}
	return s
}

func (w *Writer) createStorageWriter(ctx context.Context) (
	dataFile, statFile string,
	data, stats objectio.Writer,
	err error,
) {
	dataPath := filepath.Join(w.getPartitionedPrefix(), strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, &storeapi.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize,
	})
	if err != nil {
		return "", "", nil, nil, err
	}
	statPath := filepath.Join(w.getPartitionedPrefix()+statSuffix, strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(ctx, statPath, &storeapi.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize,
	})
	if err != nil {
		_ = dataWriter.Close(ctx)
		return "", "", nil, nil, err
	}
	return dataPath, statPath, dataWriter, statsWriter, nil
}

func (w *Writer) createDupWriter(ctx context.Context) (string, objectio.Writer, error) {
	path := filepath.Join(w.getPartitionedPrefix()+dupSuffix, strconv.Itoa(w.currentSeq))
	writer, err := w.store.Create(ctx, path, &storeapi.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize})
	return path, writer, err
}

func (w *Writer) getPartitionedPrefix() string {
	return randPartitionedPrefix(w.filenamePrefix, w.rnd)
}

// when importing large mount of data, during merge-sort and ingest, it's possible
// we need to read many files in parallel, but for Object Storage like S3, it will
// partition all object keys by prefix and each partition have its own request
// quota. Initially, each bucket only have one partition, and the auto-partition
// of object storage is mostly slow, so we might be throttled for some time to wait
// S3 server do auto-partition.
// to mitigate this issue, we design the file prefix in a way which is easy to
// be partitioned, and let the user file a ticket to let cloud provider partition
// by prefix manually before import large dataset.
//
// the rule is: generate a random byte in range [0, 256) and encode to binary
// string, and use it as the partitioned prefix.
func randPartitionedPrefix(prefix string, rnd *rand.Rand) string {
	partitionPrefix := fmt.Sprintf("%s%08b", partitionHeader, rnd.Intn(math.MaxUint8+1))
	return filepath.Join(partitionPrefix, prefix)
}

func isValidPartition(in []byte) bool {
	if len(in) != 9 || in[0] != partitionHeaderChar {
		return false
	}
	for _, c := range in[1:] {
		if c != '0' && c != '1' {
			return false
		}
	}
	return true
}

// EngineWriter implements backend.EngineWriter interface.
type EngineWriter struct {
	w *Writer
}

// NewEngineWriter creates a new EngineWriter.
func NewEngineWriter(w *Writer) *EngineWriter {
	return &EngineWriter{w: w}
}

// AppendRows implements backend.EngineWriter interface.
func (e *EngineWriter) AppendRows(ctx context.Context, _ []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}
	for _, item := range kvs {
		err := e.w.WriteRow(ctx, item.Key, item.Val, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsSynced implements backend.EngineWriter interface.
func (e *EngineWriter) IsSynced() bool {
	// only used when saving checkpoint
	return true
}

// Close implements backend.EngineWriter interface.
func (e *EngineWriter) Close(ctx context.Context) (common.ChunkFlushStatus, error) {
	return nil, e.w.Close(ctx)
}
