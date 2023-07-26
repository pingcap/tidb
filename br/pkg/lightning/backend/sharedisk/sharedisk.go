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

package sharedisk

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/keyspace"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var ReadByteForTest atomic.Uint64
var ReadTimeForTest atomic.Uint64
var ReadIOCnt atomic.Uint64

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type RangeProperty struct {
	Key      []byte
	offset   uint64
	WriterID int
	DataSeq  int
	rangeOffsets
}

// RangePropertiesCollector collects range properties for each range.
type RangePropertiesCollector struct {
	props               []*RangeProperty
	currProp            *RangeProperty
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func (rc *RangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &RangeProperty{
		rangeOffsets: rangeOffsets{},
	}
	rc.lastOffsets = rangeOffsets{}
	rc.lastKey = nil
	rc.currentOffsets = rangeOffsets{}
}

// keyLen + p.size + p.keys + p.offset + p.WriterID + p.DataSeq
const propertyLengthExceptKey = 4 + 8 + 8 + 8 + 4 + 4

func (rc *RangePropertiesCollector) Encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range rc.props {
		// Size.
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(propertyLengthExceptKey+len(p.Key)))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.Key)))
		idx += 4
		b = append(b, p.Key...)
		idx += len(p.Key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Keys)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.offset)
		idx += 8

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(p.WriterID))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(p.DataSeq))
		idx += 4
	}
	return b
}

func NewEngine(sizeDist, keyDist uint64) *Engine {
	return &Engine{
		rc: &RangePropertiesCollector{
			// TODO(tangenta): decide the preserved size of props.
			props:               nil,
			currProp:            &RangeProperty{},
			propSizeIdxDistance: sizeDist,
			propKeysIdxDistance: keyDist,
		},
	}
}

type Engine struct {
	rc *RangePropertiesCollector
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

func NewWriter(ctx context.Context, externalStorage storage.ExternalStorage,
	prefix string, writerID int, bufferPool *membuf.Pool,
	memSizeLimit uint64, keyDist int64, sizeDist uint64, writeBatchSize int64,
	onClose OnCloseFunc) *Writer {
	engine := NewEngine(sizeDist, uint64(keyDist))
	return &Writer{
		ctx:            ctx,
		engine:         engine,
		memSizeLimit:   memSizeLimit,
		exStorage:      externalStorage,
		kvBuffer:       bufferPool.NewBuffer(),
		writeBatch:     make([]common.KvPair, writeBatchSize),
		currentSeq:     0,
		tikvCodec:      keyspace.CodecV1,
		filenamePrefix: prefix,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        onClose,
		closed:         false,
	}
}

// Writer is used to write data into external storage.
type Writer struct {
	ctx context.Context
	sync.Mutex
	engine       *Engine
	memSizeLimit uint64
	exStorage    storage.ExternalStorage

	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair
	batchCount int
	batchSize  uint64

	currentSeq int
	onClose    OnCloseFunc
	closed     bool

	tikvCodec      tikv.Codec
	filenamePrefix string
	writerID       int
	minKey         tidbkv.Key
	maxKey         tidbkv.Key
	totalSize      uint64

	kvStore *KeyValueStore
}

// AppendRows appends rows to the external storage.
func (w *Writer) AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}

	//if w.engine.closed.Load() {
	//	return errorEngineClosed
	//}

	for i := range kvs {
		kvs[i].Key = w.tikvCodec.EncodeKey(kvs[i].Key)
	}

	w.Lock()
	defer w.Unlock()

	for _, pair := range kvs {
		w.batchSize += uint64(len(pair.Key) + len(pair.Val))
		buf := w.kvBuffer.AllocBytes(len(pair.Key))
		key := append(buf[:0], pair.Key...)
		val := w.kvBuffer.AddBytes(pair.Val)
		w.batchCount++
		if w.batchCount > len(w.writeBatch) {
			w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		} else {
			w.writeBatch[w.batchCount-1] = common.KvPair{Key: key, Val: val}
		}
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

	logutil.BgLogger().Info("close writer", zap.Int("writerID", w.writerID),
		zap.String("minKey", hex.EncodeToString(w.minKey)), zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	// Write the stat information to the storage.
	if w.kvStore != nil && len(w.kvStore.rc.props) > 0 {
		statPath := filepath.Join(w.filenamePrefix+"_stat", "0")
		stat, err := w.exStorage.Create(w.ctx, statPath)
		_, err = stat.Write(w.ctx, w.kvStore.rc.Encode())
		if err != nil {
			return status(false), err
		}
		err = stat.Close(w.ctx)
		if err != nil {
			return status(false), err
		}
	}
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

func CheckDataCnt(file string, exStorage storage.ExternalStorage) error {
	iter, err := NewMergeIter(context.Background(), []string{file}, []uint64{0}, exStorage, 4096)
	if err != nil {
		return err
	}
	var cnt int
	var firstKey, lastKey tidbkv.Key
	for iter.Next() {
		cnt++
		if len(firstKey) == 0 {
			firstKey = iter.Key()
			firstKey = firstKey.Clone()
		}
		lastKey = iter.Key()
	}
	lastKey = lastKey.Clone()
	logutil.BgLogger().Info("check data cnt", zap.Int("cnt", cnt),
		zap.Strings("name", PrettyFileNames([]string{file})),
		zap.String("first", hex.EncodeToString(firstKey)), zap.String("last", hex.EncodeToString(lastKey)))
	return iter.Error()
}

func (w *Writer) flushKVs(ctx context.Context) error {
	logutil.BgLogger().Info("debug flush", zap.Any("len", len(w.writeBatch)), zap.Any("cap", cap(w.writeBatch)))
	if w.batchCount == 0 {
		return nil
	}

	dataWriter, err := w.createStorageWriter()
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
		metrics.GlobalSortSharedDiskRate.WithLabelValues("write").Observe(float64(saveBytes) / 1024.0 / 1024.0 / (float64(time.Since(ts).Microseconds()) / 1000000.0))
		logutil.BgLogger().Info("flush kv", zap.Any("time", time.Since(ts)), zap.Any("bytes", saveBytes), zap.Any("rate", float64(saveBytes)/1024.0/1024.0/time.Since(ts).Seconds()))
	}()

	slices.SortFunc(w.writeBatch[:w.batchCount], func(i, j common.KvPair) bool {
		return bytes.Compare(i.Key, j.Key) < 0
	})

	ts = time.Now()

	w.kvStore, err = Create(w.ctx, dataWriter)

	ts = time.Now()

	w.kvStore.rc = w.engine.rc

	var size uint64
	for i := 0; i < w.batchCount; i++ {
		//logutil.BgLogger().Info("flush kv", zap.Int("writerID", w.writerID),
		//	zap.String("key", hex.EncodeToString(w.writeBatch[i].Key)),
		//	zap.String("val", hex.EncodeToString(w.writeBatch[i].Val)))
		err = w.kvStore.AddKeyValue(w.writeBatch[i].Key, w.writeBatch[i].Val, w.writerID, w.currentSeq)
		if err != nil {
			return err
		}
		size += uint64(len(w.writeBatch[i].Key)) + uint64(len(w.writeBatch[i].Val))
	}

	//if w.engine.rc.currProp.Keys > 0 {
	//	newProp := *w.engine.rc.currProp
	//	w.engine.rc.props = append(w.engine.rc.props, &newProp)
	//}
	//_, err = statWriter.Write(w.ctx, w.engine.rc.Encode())
	//if err != nil {
	//	return err
	//}
	w.recordMinMax(w.writeBatch[0].Key, w.writeBatch[w.batchCount-1].Key, size)

	w.batchCount = 0
	w.kvBuffer.Reset()
	saveBytes = w.batchSize
	w.batchSize = 0
	return nil
}

func (w *Writer) createStorageWriter() (storage.ExternalFileWriter, error) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.exStorage.Create(w.ctx, dataPath)
	logutil.BgLogger().Debug("new data writer", zap.Any("name", dataPath))
	if err != nil {
		return nil, err
	}

	return dataWriter, nil
}

func PrettyFileNames(files []string) []string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		dir, file := filepath.Split(f)
		names = append(names, fmt.Sprintf("%s/%s", filepath.Base(dir), file))
	}
	return names
}
