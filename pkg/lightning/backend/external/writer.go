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
	"sort"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

var (
	multiFileStatNum           = 500
	defaultPropSizeDist        = 1 * size.MB
	defaultPropKeysDist uint64 = 8 * 1024
	// Tested on GCP 16c/32c node, 32~64 workers used up all network bandwidth for
	// part-size in range 5~20M, but not all thread will upload at same time.
	// this value might not be optimal.
	// TODO need data on AWS and other machine types
	maxUploadWorkersPerThread = 8

	// MergeSortOverlapThreshold is the threshold of overlap between sorted kv files.
	// if the overlap ratio is greater than this threshold, we will merge the files.
	MergeSortOverlapThreshold int64 = 4000
	// MergeSortFileCountStep is the step of file count when we split the sorted kv files.
	MergeSortFileCountStep = 4000
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
	WriterID    string
	GroupOffset int
	Seq         int
	// Min and Max are the min and max key written by this writer, both are
	// inclusive, i.e. [Min, Max].
	// will be empty if no key is written.
	Min                tidbkv.Key
	Max                tidbkv.Key
	TotalSize          uint64
	TotalCnt           uint64
	MultipleFilesStats []MultipleFilesStat
}

// OnCloseFunc is the callback function when a writer is closed.
type OnCloseFunc func(summary *WriterSummary)

// dummyOnCloseFunc is a dummy OnCloseFunc.
func dummyOnCloseFunc(*WriterSummary) {}

// WriterBuilder builds a new Writer.
type WriterBuilder struct {
	groupOffset     int
	memSizeLimit    uint64
	blockSize       int
	propSizeDist    uint64
	propKeysDist    uint64
	onClose         OnCloseFunc
	keyDupeEncoding bool
}

// NewWriterBuilder creates a WriterBuilder.
func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit: DefaultMemSizeLimit,
		blockSize:    DefaultBlockSize,
		propSizeDist: defaultPropSizeDist,
		propKeysDist: defaultPropKeysDist,
		onClose:      dummyOnCloseFunc,
	}
}

// SetMemorySizeLimit sets the memory size limit of the writer. When accumulated
// data size exceeds this limit, the writer will flush data as a file to external
// storage.
// When the writer is OneFileWriter SetMemorySizeLimit sets the preAllocated memory buffer size.
func (b *WriterBuilder) SetMemorySizeLimit(size uint64) *WriterBuilder {
	b.memSizeLimit = size
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

// SetGroupOffset set the group offset of a writer.
// This can be used to group the summaries from different writers.
// For example, for adding multiple indexes with multi-schema-change,
// we use to distinguish the summaries from different indexes.
func (b *WriterBuilder) SetGroupOffset(offset int) *WriterBuilder {
	b.groupOffset = offset
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
	p := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(b.blockSize),
	)
	ret := &Writer{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		memSizeLimit:   b.memSizeLimit,
		store:          store,
		kvBuffer:       p.NewBuffer(membuf.WithBufferMemoryLimit(b.memSizeLimit)),
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		keyAdapter:     keyAdapter,
		writerID:       writerID,
		groupOffset:    b.groupOffset,
		onClose:        b.onClose,
		closed:         false,
		multiFileStats: make([]MultipleFilesStat, 1),
		fileMinKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		fileMaxKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
	}
	ret.multiFileStats[0].Filenames = make([][2]string, 0, multiFileStatNum)

	return ret
}

// BuildOneFile builds a new one file Writer. The writer will create only one
// file under the prefix of "{prefix}/{writerID}".
func (b *WriterBuilder) BuildOneFile(
	store storage.ExternalStorage,
	prefix string,
	writerID string,
) *OneFileWriter {
	filenamePrefix := filepath.Join(prefix, writerID)
	p := membuf.NewPool(membuf.WithBlockNum(0), membuf.WithBlockSize(b.blockSize))

	ret := &OneFileWriter{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		kvBuffer:       p.NewBuffer(membuf.WithBufferMemoryLimit(b.memSizeLimit)),
		store:          store,
		filenamePrefix: filenamePrefix,
		writerID:       writerID,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
	}
	return ret
}

// MultipleFilesStat is the statistic information of multiple files (currently
// every 500 files). It is used to estimate the data overlapping, and per-file
// statistic information maybe too big to loaded into memory.
type MultipleFilesStat struct {
	MinKey tidbkv.Key `json:"min-key"`
	MaxKey tidbkv.Key `json:"max-key"`
	// Filenames is a list of [dataFile, statFile] paris, and it's sorted by the
	// first key of the data file.
	Filenames         [][2]string `json:"filenames"`
	MaxOverlappingNum int64       `json:"max-overlapping-num"`
}

type startKeysAndFiles struct {
	startKeys []tidbkv.Key
	files     [][2]string
}

func (s *startKeysAndFiles) Len() int {
	return len(s.startKeys)
}

func (s *startKeysAndFiles) Less(i, j int) bool {
	return s.startKeys[i].Cmp(s.startKeys[j]) < 0
}

func (s *startKeysAndFiles) Swap(i, j int) {
	s.startKeys[i], s.startKeys[j] = s.startKeys[j], s.startKeys[i]
	s.files[i], s.files[j] = s.files[j], s.files[i]
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
	// make Filenames sorted by startKeys
	s := &startKeysAndFiles{startKeys, m.Filenames}
	sort.Sort(s)

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

// Writer is used to write data into external storage.
type Writer struct {
	store          storage.ExternalStorage
	writerID       string
	groupOffset    int
	currentSeq     int
	filenamePrefix string
	keyAdapter     common.KeyAdapter

	rc *rangePropertiesCollector

	memSizeLimit uint64

	kvBuffer    *membuf.Buffer
	kvLocations []membuf.SliceLocation
	kvSize      int64

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
	totalCnt  uint64
}

// WriteRow implements ingest.Writer.
func (w *Writer) WriteRow(ctx context.Context, key, val []byte, handle tidbkv.Handle) error {
	keyAdapter := w.keyAdapter

	var rowID []byte
	if handle != nil {
		rowID = handle.Encoded()
	}
	encodedKeyLen := keyAdapter.EncodedLen(key, rowID)
	length := encodedKeyLen + len(val) + lengthBytes*2
	dataBuf, loc := w.kvBuffer.AllocBytesWithSliceLocation(length)
	if dataBuf == nil {
		if err := w.flushKVs(ctx, false); err != nil {
			return err
		}
		dataBuf, loc = w.kvBuffer.AllocBytesWithSliceLocation(length)
		// we now don't support KV larger than blockSize
		if dataBuf == nil {
			return errors.Errorf("failed to allocate kv buffer: %d", length)
		}
	}
	binary.BigEndian.AppendUint64(dataBuf[:0], uint64(encodedKeyLen))
	binary.BigEndian.AppendUint64(dataBuf[:lengthBytes], uint64(len(val)))
	keyAdapter.Encode(dataBuf[2*lengthBytes:2*lengthBytes:2*lengthBytes+encodedKeyLen], key, rowID)
	copy(dataBuf[2*lengthBytes+encodedKeyLen:], val)

	w.kvLocations = append(w.kvLocations, loc)
	w.kvSize += int64(encodedKeyLen + len(val))
	w.batchSize += uint64(length)
	w.totalCnt += 1
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
		zap.Int("kv-cnt-cap", cap(w.kvLocations)),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)))

	w.kvLocations = nil
	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		GroupOffset:        w.groupOffset,
		Seq:                w.currentSeq,
		Min:                w.minKey,
		Max:                w.maxKey,
		TotalSize:          w.totalSize,
		TotalCnt:           w.totalCnt,
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

const flushKVsRetryTimes = 3

func (w *Writer) flushKVs(ctx context.Context, fromClose bool) (err error) {
	if len(w.kvLocations) == 0 {
		return nil
	}

	logger := logutil.Logger(ctx).With(
		zap.String("writer-id", w.writerID),
		zap.Int("sequence-number", w.currentSeq),
	)
	sortStart := time.Now()
	slices.SortFunc(w.kvLocations, func(i, j membuf.SliceLocation) int {
		return bytes.Compare(w.getKeyByLoc(i), w.getKeyByLoc(j))
	})
	sortDuration := time.Since(sortStart)
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort").Observe(sortDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort").Observe(float64(w.batchSize) / 1024.0 / 1024.0 / sortDuration.Seconds())

	writeStartTime := time.Now()
	var dataFile, statFile string
	for i := 0; i < flushKVsRetryTimes; i++ {
		dataFile, statFile, err = w.flushSortedKVs(ctx)
		if err == nil {
			break
		}
		logger.Warn("flush sorted kv failed",
			zap.Error(err),
			zap.Int("retry-count", i),
		)
	}
	if err != nil {
		return err
	}
	writeDuration := time.Since(writeStartTime)
	kvCnt := len(w.kvLocations)
	logger.Info("flush kv",
		zap.Uint64("bytes", w.batchSize),
		zap.Int("kv-cnt", kvCnt),
		zap.Duration("sort-time", sortDuration),
		zap.Duration("write-time", writeDuration),
		zap.String("sort-speed(kv/s)", getSpeed(uint64(kvCnt), sortDuration.Seconds(), false)),
		zap.String("writer-id", w.writerID),
	)
	totalDuration := time.Since(sortStart)
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort_and_write").Observe(totalDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort_and_write").Observe(float64(w.batchSize) / 1024.0 / 1024.0 / totalDuration.Seconds())

	minKey, maxKey := w.getKeyByLoc(w.kvLocations[0]), w.getKeyByLoc(w.kvLocations[len(w.kvLocations)-1])
	w.recordMinMax(minKey, maxKey, uint64(w.kvSize))

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

	w.kvLocations = w.kvLocations[:0]
	w.kvSize = 0
	w.kvBuffer.Reset()
	w.batchSize = 0
	w.currentSeq++
	return nil
}

func (w *Writer) flushSortedKVs(ctx context.Context) (string, string, error) {
	logger := logutil.Logger(ctx).With(
		zap.String("writer-id", w.writerID),
		zap.Int("sequence-number", w.currentSeq),
	)
	writeStartTime := time.Now()
	dataFile, statFile, dataWriter, statWriter, err := w.createStorageWriter(ctx)
	if err != nil {
		return "", "", err
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
	kvStore, err := NewKeyValueStore(ctx, dataWriter, w.rc)
	if err != nil {
		return "", "", err
	}

	for _, pair := range w.kvLocations {
		err = kvStore.addEncodedData(w.kvBuffer.GetSlice(pair))
		if err != nil {
			return "", "", err
		}
	}

	kvStore.Close()
	encodedStat := w.rc.encode()
	statSize := len(encodedStat)
	_, err = statWriter.Write(ctx, encodedStat)
	if err != nil {
		return "", "", err
	}
	err = dataWriter.Close(ctx)
	dataWriter = nil
	if err != nil {
		return "", "", err
	}
	err = statWriter.Close(ctx)
	statWriter = nil
	if err != nil {
		return "", "", err
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

	return dataFile, statFile, nil
}

func (w *Writer) getKeyByLoc(loc membuf.SliceLocation) []byte {
	block := w.kvBuffer.GetSlice(loc)
	keyLen := binary.BigEndian.Uint64(block[:lengthBytes])
	return block[2*lengthBytes : 2*lengthBytes+keyLen]
}

func (w *Writer) createStorageWriter(ctx context.Context) (
	dataFile, statFile string,
	data, stats storage.ExternalFileWriter,
	err error,
) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, &storage.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize,
	})
	if err != nil {
		return "", "", nil, nil, err
	}
	statPath := filepath.Join(w.filenamePrefix+statSuffix, strconv.Itoa(w.currentSeq))
	statsWriter, err := w.store.Create(ctx, statPath, &storage.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize,
	})
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
