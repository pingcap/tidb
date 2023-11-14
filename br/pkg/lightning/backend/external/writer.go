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

	"github.com/pingcap/tidb/br/pkg/membuf"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

var (
	multiFileStatNum = 500

	// MergeSortOverlapThreshold is the threshold of overlap between sorted kv files.
	// if the overlap ratio is greater than this threshold, we will merge the files.
	MergeSortOverlapThreshold int64 = 1000
	// MergeSortFileCountStep is the step of file count when we split the sorted kv files.
	MergeSortFileCountStep = 1000
)

const (
	// DefaultMemSizeLimit is the default memory size limit for writer.
	DefaultMemSizeLimit = 256 * size.MB
	// DefaultBlockSize is the default block size for writer.
	DefaultBlockSize = 16 * units.MiB
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
	WriterID string
	Seq      int
	// Min and Max are the min and max key written by this writer, both are
	// inclusive, i.e. [Min, Max].
	// will be empty if no key is written.
	Min                tidbkv.Key
	Max                tidbkv.Key
	TotalSize          uint64
	MultipleFilesStats []MultipleFilesStat
}

// OnCloseFunc is the callback function when a writer is closed.
type OnCloseFunc func(summary *WriterSummary)

// dummyOnCloseFunc is a dummy OnCloseFunc.
func dummyOnCloseFunc(*WriterSummary) {}

// WriterBuilder builds a new Writer.
type WriterBuilder struct {
	memSizeLimit    uint64
	blockSize       int
	writeBatchCount uint64
	propSizeDist    uint64
	propKeysDist    uint64
	onClose         OnCloseFunc
	keyDupeEncoding bool
}

// NewWriterBuilder creates a WriterBuilder.
func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit:    DefaultMemSizeLimit,
		blockSize:       DefaultBlockSize,
		writeBatchCount: 8 * 1024,
		propSizeDist:    1 * size.MB,
		propKeysDist:    8 * 1024,
		onClose:         dummyOnCloseFunc,
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
	if onClose == nil {
		onClose = dummyOnCloseFunc
	}
	b.onClose = onClose
	return b
}

// SetKeyDuplicationEncoding sets if the writer can distinguish duplicate key.
func (b *WriterBuilder) SetKeyDuplicationEncoding(val bool) *WriterBuilder {
	b.keyDupeEncoding = val
	return b
}

// SetBlockSize sets the block size of pre-allocated buf in the writer.
func (b *WriterBuilder) SetBlockSize(blockSize int) *WriterBuilder {
	b.blockSize = blockSize
	return b
}

// Build builds a new Writer. The files writer will create are under the prefix
// of "{prefix}/{writerID}".
func (b *WriterBuilder) Build(
	store storage.ExternalStorage,
	prefix string,
	writerID string,
) *Writer {
	filenamePrefix := filepath.Join(prefix, writerID)
	keyAdapter := common.KeyAdapter(common.NoopKeyAdapter{})
	if b.keyDupeEncoding {
		keyAdapter = common.DupDetectKeyAdapter{}
	}
	ret := &Writer{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		memSizeLimit: b.memSizeLimit,
		store:        store,
		kvBuffer: membuf.NewPool(
			membuf.WithBlockSize(b.blockSize),
			membuf.WithBlockNum(int(b.memSizeLimit)/b.blockSize),
		).PreAllocPoolSize(int(b.memSizeLimit)).NewBuffer(),
		writeBatch:     make([]simpleKV, 256*1024),
		writeCnt:       0,
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		keyAdapter:     keyAdapter,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
		multiFileStats: make([]MultipleFilesStat, 1),
		fileMinKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		fileMaxKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
	}
	ret.multiFileStats[0].Filenames = make([][2]string, 0, multiFileStatNum)
	return ret
}

// MultipleFilesStat is the statistic information of multiple files (currently
// every 500 files). It is used to estimate the data overlapping, and per-file
// statistic information maybe too big to loaded into memory.
type MultipleFilesStat struct {
	MinKey            tidbkv.Key  `json:"min-key"`
	MaxKey            tidbkv.Key  `json:"max-key"`
	Filenames         [][2]string `json:"filenames"` // [dataFile, statFile]
	MaxOverlappingNum int64       `json:"max-overlapping-num"`
}

func (m *MultipleFilesStat) build(startKeys, endKeys []tidbkv.Key) {
	if len(startKeys) == 0 {
		return
	}
	m.MinKey = startKeys[0]
	m.MaxKey = endKeys[0]
	for i := 1; i < len(startKeys); i++ {
		if m.MinKey.Cmp(startKeys[i]) > 0 {
			m.MinKey = startKeys[i]
		}
		if m.MaxKey.Cmp(endKeys[i]) < 0 {
			m.MaxKey = endKeys[i]
		}
	}

	points := make([]Endpoint, 0, len(startKeys)*2)
	for _, k := range startKeys {
		points = append(points, Endpoint{Key: k, Tp: InclusiveStart, Weight: 1})
	}
	for _, k := range endKeys {
		points = append(points, Endpoint{Key: k, Tp: InclusiveEnd, Weight: 1})
	}
	m.MaxOverlappingNum = GetMaxOverlapping(points)
}

// GetMaxOverlappingTotal assume the most overlapping case from given stats and
// returns the overlapping level.
func GetMaxOverlappingTotal(stats []MultipleFilesStat) int64 {
	points := make([]Endpoint, 0, len(stats)*2)
	for _, stat := range stats {
		points = append(points, Endpoint{Key: stat.MinKey, Tp: InclusiveStart, Weight: stat.MaxOverlappingNum})
	}
	for _, stat := range stats {
		points = append(points, Endpoint{Key: stat.MaxKey, Tp: InclusiveEnd, Weight: stat.MaxOverlappingNum})
	}

	return GetMaxOverlapping(points)
}

type simpleKV struct {
	key []byte
	val []byte
}

// Writer is used to write data into external storage.
type Writer struct {
	store          storage.ExternalStorage
	writerID       string
	currentSeq     int
	filenamePrefix string
	keyAdapter     common.KeyAdapter

	kvStore *KeyValueStore
	rc      *rangePropertiesCollector

	memSizeLimit uint64

	kvBuffer   *membuf.Buffer
	writeBatch []simpleKV
	writeCnt   int

	onClose OnCloseFunc
	closed  bool

	// Statistic information per batch.
	batchSize uint64

	// Statistic information per 500 batches.
	multiFileStats []MultipleFilesStat
	fileMinKeys    []tidbkv.Key
	fileMaxKeys    []tidbkv.Key

	// Statistic information per writer.
	minKey    tidbkv.Key
	maxKey    tidbkv.Key
	totalSize uint64
}

// WriteRow implements ingest.Writer.
func (w *Writer) WriteRow(ctx context.Context, idxKey, idxVal []byte, handle tidbkv.Handle) error {
	keyAdapter := w.keyAdapter
	w.batchSize += uint64(len(idxKey) + len(idxVal) + 2*lengthBytes)

	var rowID []byte
	if handle != nil {
		rowID = handle.Encoded()
	}
	buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(idxKey, rowID))
	key := keyAdapter.Encode(buf[:0], idxKey, rowID)
	val := w.kvBuffer.AddBytes(idxVal)

	if w.writeCnt == len(w.writeBatch) {
		w.writeBatch = append(w.writeBatch, make([]simpleKV, len(w.writeBatch))...)
	}

	w.writeBatch[w.writeCnt].key = key
	w.writeBatch[w.writeCnt].val = val
	w.writeCnt++
	if w.batchSize >= w.memSizeLimit {
		if err := w.flushKVs(ctx, false); err != nil {
			return err
		}
	}
	return nil
}

// LockForWrite implements ingest.Writer.
// Since flushKVs is thread-safe in external storage writer,
// this is implemented as noop.
func (w *Writer) LockForWrite() func() {
	return func() {}
}

// Close closes the writer.
func (w *Writer) Close(ctx context.Context) error {
	if w.closed {
		return errors.Errorf("writer %s has been closed", w.writerID)
	}
	w.closed = true
	defer w.kvBuffer.Destroy()
	err := w.flushKVs(ctx, true)
	if err != nil {
		return err
	}
	// remove the trailing empty MultipleFilesStat
	w.multiFileStats = w.multiFileStats[:len(w.multiFileStats)-1]

	logutil.Logger(ctx).Info("close writer",
		zap.String("writerID", w.writerID),
		zap.Int("kv-cnt-cap", cap(w.writeBatch)),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	w.writeBatch = nil

	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		Seq:                w.currentSeq,
		Min:                w.minKey,
		Max:                w.maxKey,
		TotalSize:          w.totalSize,
		MultipleFilesStats: w.multiFileStats,
	})
	return nil
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

func (w *Writer) flushKVs(ctx context.Context, fromClose bool) (err error) {
	if w.writeCnt == 0 {
		return nil
	}

	logger := logutil.Logger(ctx)
	dataFile, statFile, dataWriter, statWriter, err := w.createStorageWriter(ctx)
	if err != nil {
		return err
	}

	var (
		savedBytes                  uint64
		statSize                    int
		sortDuration, writeDuration time.Duration
		writeStartTime              time.Time
	)
	savedBytes = w.batchSize
	startTs := time.Now()

	getSpeed := func(n uint64, dur float64, isBytes bool) string {
		if dur == 0 {
			return "-"
		}
		if isBytes {
			return units.BytesSize(float64(n) / dur)
		}
		return units.HumanSize(float64(n) / dur)
	}
	kvCnt := w.writeCnt
	defer func() {
		w.currentSeq++
		err1, err2 := dataWriter.Close(ctx), statWriter.Close(ctx)
		if err != nil {
			return
		}
		if err1 != nil {
			logger.Error("close data writer failed", zap.Error(err1))
			err = err1
			return
		}
		if err2 != nil {
			logger.Error("close stat writer failed", zap.Error(err2))
			err = err2
			return
		}
		writeDuration = time.Since(writeStartTime)
		logger.Info("flush kv",
			zap.Uint64("bytes", savedBytes),
			zap.Int("kv-cnt", kvCnt),
			zap.Int("stat-size", statSize),
			zap.Duration("sort-time", sortDuration),
			zap.Duration("write-time", writeDuration),
			zap.String("sort-speed(kv/s)", getSpeed(uint64(kvCnt), sortDuration.Seconds(), false)),
			zap.String("write-speed(bytes/s)", getSpeed(savedBytes, writeDuration.Seconds(), true)),
			zap.String("writer-id", w.writerID),
		)
		metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("write").Observe(writeDuration.Seconds())
		metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("write").Observe(float64(savedBytes) / 1024.0 / 1024.0 / writeDuration.Seconds())
		metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort_and_write").Observe(time.Since(startTs).Seconds())
		metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort_and_write").Observe(float64(savedBytes) / 1024.0 / 1024.0 / time.Since(startTs).Seconds())
	}()

	sortStart := time.Now()
	slices.SortFunc(w.writeBatch[:w.writeCnt], func(i, j simpleKV) int {
		return bytes.Compare(i.key, j.key)
	})
	sortDuration = time.Since(sortStart)

	writeStartTime = time.Now()
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort").Observe(sortDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort").Observe(float64(savedBytes) / 1024.0 / 1024.0 / sortDuration.Seconds())
	w.kvStore, err = NewKeyValueStore(ctx, dataWriter, w.rc)
	if err != nil {
		return err
	}

	for _, pair := range w.writeBatch[:w.writeCnt] {
		err = w.kvStore.addEncodedData(pair.key, pair.val)
		if err != nil {
			return err
		}
	}

	err = w.kvStore.Close()
	if err != nil {
		return err
	}
	encodedStat := w.rc.encode()
	statSize = len(encodedStat)
	_, err = statWriter.Write(ctx, encodedStat)
	if err != nil {
		return err
	}

	minKey := tidbkv.Key(w.writeBatch[0].key).Clone()
	maxKey := tidbkv.Key(w.writeBatch[w.writeCnt-1].key).Clone()
	w.recordMinMax(minKey, maxKey, w.batchSize)

	// maintain 500-batch statistics

	l := len(w.multiFileStats)
	w.multiFileStats[l-1].Filenames = append(w.multiFileStats[l-1].Filenames,
		[2]string{dataFile, statFile},
	)
	w.fileMinKeys = append(w.fileMinKeys, tidbkv.Key(minKey).Clone())
	w.fileMaxKeys = append(w.fileMaxKeys, tidbkv.Key(maxKey).Clone())
	if fromClose || len(w.multiFileStats[l-1].Filenames) == multiFileStatNum {
		w.multiFileStats[l-1].build(w.fileMinKeys, w.fileMaxKeys)
		w.multiFileStats = append(w.multiFileStats, MultipleFilesStat{
			Filenames: make([][2]string, 0, multiFileStatNum),
		})
		w.fileMinKeys = w.fileMinKeys[:0]
		w.fileMaxKeys = w.fileMaxKeys[:0]
	}

	w.kvBuffer.Reset()
	w.rc.reset()
	w.writeCnt = 0
	w.batchSize = 0
	return nil
}

func (w *Writer) createStorageWriter(ctx context.Context) (
	dataFile, statFile string,
	data, stats storage.ExternalFileWriter,
	err error,
) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, &storage.WriterOption{Concurrency: 20})
	if err != nil {
		return "", "", nil, nil, err
	}
	statPath := filepath.Join(w.filenamePrefix+statSuffix, strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(ctx, statPath, &storage.WriterOption{Concurrency: 20})
	if err != nil {
		_ = dataWriter.Close(ctx)
		return "", "", nil, nil, err
	}
	return dataPath, statPath, dataWriter, statsWriter, nil
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
func (e *EngineWriter) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	return nil, e.w.Close(ctx)
}
