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
	"encoding/hex"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/size"
	"go.uber.org/zap"
)

// rangePropertiesCollector collects range properties for each range. The zero
// value of rangePropertiesCollector is not ready to use, should call reset()
// first.
type rangePropertiesCollector struct {
	props        []*rangeProperty
	currProp     *rangeProperty
	propSizeDist uint64
	propKeysDist uint64
}

func (rc *rangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &rangeProperty{}
}

// encode encodes rc.props to a byte slice.
func (rc *rangePropertiesCollector) encode() []byte {
	b := make([]byte, 0, 1024)
	return encodeMultiProps(b, rc.props)
}

// WriterSummary is the summary of a writer.
type WriterSummary struct {
	WriterID  int
	Seq       int
	Min       tidbkv.Key
	Max       tidbkv.Key
	TotalSize uint64
}

// OnCloseFunc is the callback function when a writer is closed.
type OnCloseFunc func(summary *WriterSummary)

// DummyOnCloseFunc is a dummy OnCloseFunc.
func DummyOnCloseFunc(*WriterSummary) {}

// WriterBuilder builds a new Writer.
type WriterBuilder struct {
	memSizeLimit      uint64
	writeBatchCount   uint64
	propSizeDist      uint64
	propKeysDist      uint64
	onClose           OnCloseFunc
	dupeDetectEnabled bool

	bufferPool *membuf.Pool
}

// NewWriterBuilder creates a WriterBuilder.
func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit:    256 * size.MB,
		writeBatchCount: 8 * 1024,
		propSizeDist:    1 * size.MB,
		propKeysDist:    8 * 1024,
		onClose:         DummyOnCloseFunc,
	}
}

// SetMemorySizeLimit sets the memory size limit of the writer. When accumulated
// data size exceeds this limit, the writer will flush data as a file to external
// storage.
func (b *WriterBuilder) SetMemorySizeLimit(size uint64) *WriterBuilder {
	b.memSizeLimit = size
	return b
}

// SetWriterBatchCount sets the batch count of the writer.
func (b *WriterBuilder) SetWriterBatchCount(count uint64) *WriterBuilder {
	b.writeBatchCount = count
	return b
}

// SetPropSizeDistance sets the distance of range size for each property.
func (b *WriterBuilder) SetPropSizeDistance(dist uint64) *WriterBuilder {
	b.propSizeDist = dist
	return b
}

// SetPropKeysDistance sets the distance of range keys for each property.
func (b *WriterBuilder) SetPropKeysDistance(dist uint64) *WriterBuilder {
	b.propKeysDist = dist
	return b
}

// SetOnCloseFunc sets the callback function when a writer is closed.
func (b *WriterBuilder) SetOnCloseFunc(onClose OnCloseFunc) *WriterBuilder {
	b.onClose = onClose
	return b
}

// SetBufferPool sets the buffer pool of the writer.
func (b *WriterBuilder) SetBufferPool(bufferPool *membuf.Pool) *WriterBuilder {
	b.bufferPool = bufferPool
	return b
}

// EnableDuplicationDetection enables the duplication detection of the writer.
func (b *WriterBuilder) EnableDuplicationDetection() *WriterBuilder {
	b.dupeDetectEnabled = true
	return b
}

// Build builds a new Writer. The files writer will create are under the prefix
// of "{prefix}/{writerID}".
func (b *WriterBuilder) Build(
	store storage.ExternalStorage,
	prefix string,
	writerID int,
) *Writer {
	bp := b.bufferPool
	if bp == nil {
		bp = membuf.NewPool()
	}
	filenamePrefix := filepath.Join(prefix, strconv.Itoa(writerID))
	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if b.dupeDetectEnabled {
		keyAdapter = common.DupDetectKeyAdapter{}
	}
	return &Writer{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		memSizeLimit:   b.memSizeLimit,
		store:          store,
		kvBuffer:       bp.NewBuffer(),
		writeBatch:     make([]common.KvPair, 0, b.writeBatchCount),
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		keyAdapter:     keyAdapter,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
	}
}

// Writer is used to write data into external storage.
type Writer struct {
	store          storage.ExternalStorage
	writerID       int
	currentSeq     int
	filenamePrefix string
	keyAdapter     common.KeyAdapter

	kvStore *KeyValueStore
	rc      *rangePropertiesCollector

	memSizeLimit uint64

	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair

	onClose OnCloseFunc
	closed  bool

	// Statistic information per batch.
	batchSize uint64

	// Statistic information per writer.
	minKey    tidbkv.Key
	maxKey    tidbkv.Key
	totalSize uint64
}

// AppendRows appends rows to the external storage.
// Note that this method is NOT thread-safe.
func (w *Writer) AppendRows(ctx context.Context, _ []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	keyAdapter := w.keyAdapter
	for _, pair := range kvs {
		w.batchSize += uint64(len(pair.Key) + len(pair.Val))

		buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(pair.Key, pair.RowID))
		key := keyAdapter.Encode(buf[:0], pair.Key, pair.RowID)
		val := w.kvBuffer.AddBytes(pair.Val)

		w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		if w.batchSize >= w.memSizeLimit {
			if err := w.flushKVs(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// IsSynced implements the backend.EngineWriter interface.
func (w *Writer) IsSynced() bool {
	return false
}

// Close closes the writer.
func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if w.closed {
		return status(false), errors.Errorf("writer %d has been closed", w.writerID)
	}
	w.closed = true
	defer w.kvBuffer.Destroy()
	err := w.flushKVs(ctx)
	if err != nil {
		return status(false), err
	}

	logutil.Logger(ctx).Info("close writer",
		zap.Int("writerID", w.writerID),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	w.writeBatch = nil

	w.onClose(&WriterSummary{
		WriterID:  w.writerID,
		Seq:       w.currentSeq,
		Min:       w.minKey,
		Max:       w.maxKey,
		TotalSize: w.totalSize,
	})
	return status(true), nil
}

func (w *Writer) recordMinMax(newMin, newMax tidbkv.Key, size uint64) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
	w.totalSize += size
}

type status bool

// Flushed implements the backend.ChunkFlushStatus interface.
func (s status) Flushed() bool {
	return bool(s)
}

func (w *Writer) flushKVs(ctx context.Context) (err error) {
	if len(w.writeBatch) == 0 {
		return nil
	}

	logger := logutil.Logger(ctx)
	dataWriter, statWriter, err := w.createStorageWriter(ctx)
	if err != nil {
		return err
	}

	ts := time.Now()
	var savedBytes uint64

	defer func() {
		w.currentSeq++
		err1, err2 := dataWriter.Close(ctx), statWriter.Close(ctx)
		if err != nil {
			return
		}
		if err1 != nil {
			logger.Error("close data writer failed", zap.Error(err))
			err = err1
			return
		}
		if err2 != nil {
			logger.Error("close stat writer failed", zap.Error(err))
			err = err2
			return
		}
		logger.Info("flush kv",
			zap.Duration("time", time.Since(ts)),
			zap.Uint64("bytes", savedBytes),
			zap.Any("rate", float64(savedBytes)/1024.0/1024.0/time.Since(ts).Seconds()))
	}()

	slices.SortFunc(w.writeBatch[:], func(i, j common.KvPair) int {
		return bytes.Compare(i.Key, j.Key)
	})

	w.kvStore, err = NewKeyValueStore(ctx, dataWriter, w.rc, w.writerID, w.currentSeq)
	if err != nil {
		return err
	}

	var kvSize uint64
	for _, pair := range w.writeBatch {
		err = w.kvStore.AddKeyValue(pair.Key, pair.Val)
		if err != nil {
			return err
		}
		kvSize += uint64(len(pair.Key)) + uint64(len(pair.Val))
	}

	w.kvStore.Close()
	_, err = statWriter.Write(ctx, w.rc.encode())
	if err != nil {
		return err
	}

	w.recordMinMax(w.writeBatch[0].Key, w.writeBatch[len(w.writeBatch)-1].Key, kvSize)

	w.writeBatch = w.writeBatch[:0]
	w.rc.reset()
	w.kvBuffer.Reset()
	savedBytes = w.batchSize
	w.batchSize = 0
	return nil
}

func (w *Writer) createStorageWriter(ctx context.Context) (data, stats storage.ExternalFileWriter, err error) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, nil)
	if err != nil {
		return nil, nil, err
	}
	statPath := filepath.Join(w.filenamePrefix+statSuffix, strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(ctx, statPath, nil)
	if err != nil {
		return nil, nil, err
	}
	return dataWriter, statsWriter, nil
}
