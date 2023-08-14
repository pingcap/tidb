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
	"strconv"
	"time"

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
	"golang.org/x/exp/slices"
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

type WriterSummary struct {
	WriterID  int
	Seq       int
	Min       tidbkv.Key
	Max       tidbkv.Key
	TotalSize uint64
}

type OnCloseFunc func(summary *WriterSummary)

func DummyOnCloseFunc(*WriterSummary) {}

type WriterBuilder struct {
	ctx   context.Context
	store storage.ExternalStorage

	memSizeLimit   uint64
	writeBatchSize uint64
	propSizeDist   uint64
	propKeysDist   uint64
	onClose        OnCloseFunc

	bufferPool *membuf.Pool
}

func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit:   256 * size.MB,
		writeBatchSize: 8 * 1024,
		propSizeDist:   1 * size.MB,
		propKeysDist:   8 * 1024,
		onClose:        DummyOnCloseFunc,
	}
}

func (b *WriterBuilder) SetMemorySizeLimit(size uint64) *WriterBuilder {
	b.memSizeLimit = size
	return b
}

func (b *WriterBuilder) SetWriterBatchSize(size uint64) *WriterBuilder {
	b.writeBatchSize = size
	return b
}

func (b *WriterBuilder) SetPropSizeDistance(dist uint64) *WriterBuilder {
	b.propSizeDist = dist
	return b
}

func (b *WriterBuilder) SetPropKeysDistance(dist uint64) *WriterBuilder {
	b.propKeysDist = dist
	return b
}

func (b *WriterBuilder) SetOnCloseFunc(onClose OnCloseFunc) *WriterBuilder {
	b.onClose = onClose
	return b
}

func (b *WriterBuilder) SetBufferPool(bufferPool *membuf.Pool) *WriterBuilder {
	b.bufferPool = bufferPool
	return b
}

func (b *WriterBuilder) Build(
	ctx context.Context,
	store storage.ExternalStorage,
	writerID int,
	filenamePrefix string,
) *Writer {
	bp := b.bufferPool
	if bp == nil {
		bp = membuf.NewPool()
	}
	return &Writer{
		ctx: ctx,
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		memSizeLimit:   b.memSizeLimit,
		store:          store,
		kvBuffer:       bp.NewBuffer(),
		writeBatch:     make([]common.KvPair, 0, b.writeBatchSize),
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
	}
}

// Writer is used to write data into external storage.
type Writer struct {
	ctx            context.Context
	store          storage.ExternalStorage
	writerID       int
	currentSeq     int
	filenamePrefix string

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
	if len(kvs) == 0 {
		return nil
	}
	for _, pair := range kvs {
		w.batchSize += uint64(len(pair.Key) + len(pair.Val))
		buf := w.kvBuffer.AllocBytes(len(pair.Key))
		key := append(buf[:0], pair.Key...)
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

func (w *Writer) IsSynced() bool {
	return false
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if w.closed {
		return status(true), nil
	}
	w.closed = true
	defer w.kvBuffer.Destroy()
	err := w.flushKVs(ctx)
	if err != nil {
		return status(false), err
	}

	logutil.BgLogger().Info("close writer",
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

func (s status) Flushed() bool {
	return bool(s)
}

func (w *Writer) flushKVs(ctx context.Context) error {
	if len(w.writeBatch) == 0 {
		return nil
	}

	dataWriter, statWriter, err := w.createStorageWriter()
	if err != nil {
		return err
	}

	ts := time.Now()
	var saveBytes uint64

	defer func() {
		w.currentSeq++
		err := dataWriter.Close(w.ctx)
		if err != nil {
			logutil.BgLogger().Error("close data writer failed", zap.Error(err))
		}
		err = statWriter.Close(w.ctx)
		if err != nil {
			logutil.BgLogger().Error("close stat writer failed", zap.Error(err))
		}
		logutil.BgLogger().Info("flush kv",
			zap.Duration("time", time.Since(ts)),
			zap.Uint64("bytes", saveBytes),
			zap.Any("rate", float64(saveBytes)/1024.0/1024.0/time.Since(ts).Seconds()))
	}()

	slices.SortFunc(w.writeBatch[:], func(i, j common.KvPair) bool {
		return bytes.Compare(i.Key, j.Key) < 0
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

	if w.rc.currProp.keys > 0 {
		newProp := *w.rc.currProp
		w.rc.props = append(w.rc.props, &newProp)
	}
	_, err = statWriter.Write(w.ctx, w.rc.encode())
	if err != nil {
		return err
	}

	w.recordMinMax(w.writeBatch[0].Key, w.writeBatch[len(w.writeBatch)-1].Key, kvSize)

	w.writeBatch = w.writeBatch[:0]
	w.rc.reset()
	w.kvBuffer.Reset()
	saveBytes = w.batchSize
	w.batchSize = 0
	return nil
}

func (w *Writer) createStorageWriter() (data, stats storage.ExternalFileWriter, err error) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(w.ctx, dataPath, nil)
	if err != nil {
		return nil, nil, err
	}
	statPath := filepath.Join(w.filenamePrefix+"_stat", strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(w.ctx, statPath, nil)
	if err != nil {
		return nil, nil, err
	}
	return dataWriter, statsWriter, nil
}
