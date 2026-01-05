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
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/tikv/client-go/v2/tikv"
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
	// we use hex of 0-256 as partition prefix, it might duplicate with task ID,
	// so we add a header to it.
	partitionHeader     = "p"
	partitionHeaderChar = partitionHeader[0]

	// maxMergeSortOverlapThreshold is the maximum threshold of overlap between sorted kv files.
	// if the overlap ratio is greater than this threshold, we will merge the files. Note: Use GetAdjustedMergeSortOverlapThreshold() instead.
	maxMergeSortOverlapThreshold int64 = 4000
	// MaxMergeSortFileCountStep is the maximum step of file count when we split the sorted kv files. Note: Use GetAdjustedMergeSortFileCountStep() instead.
	MaxMergeSortFileCountStep = 4000
	// MergeSortMaxSubtaskTargetFiles assumes each merge sort subtask generates 16 files.
	MergeSortMaxSubtaskTargetFiles = 16
)

const (
	// DefaultMemSizeLimit is the default memory size limit for writer.
	DefaultMemSizeLimit = 256 * size.MB
	// DefaultBlockSize is the default block size for writer.
	DefaultBlockSize = 16 * units.MiB
)

func commonGetAdjustCount(isOverlapThreshold bool, concurrency int) int64 {
	intest.Assert(concurrency > 0, "concurrency must be greater than 0, got %d", concurrency)
	if concurrency <= 0 {
		// Even though we check it use intest.Assert, it may still goto here in the prod environment with bug.
		logutil.BgLogger().Error("concurrency is less than 0 or equal to 0, set to 1", zap.Int("concurrency", concurrency))
		concurrency = 1
	}
	cnt := 250 * int64(concurrency)
	if isOverlapThreshold {
		cnt = min(cnt, maxMergeSortOverlapThreshold)
	} else {
		cnt = min(cnt, int64(MaxMergeSortFileCountStep))
	}
	return cnt
}

// GetAdjustedMergeSortOverlapThreshold adjusts the merge sort overlap threshold based on concurrency.
// The bigger the threshold, the bigger the statistical bias. In CPU:Memory = 1:2 machine, if the concurrency
// is less than 8, the memory can be used to load data is small, and may get blocked by the memory limiter.
// So we lower the threshold here if concurrency too low.
func GetAdjustedMergeSortOverlapThreshold(concurrency int) int64 {
	return commonGetAdjustCount(true, concurrency)
}

// GetAdjustedMergeSortFileCountStep adjusts the merge sort file count step based on concurrency.
func GetAdjustedMergeSortFileCountStep(concurrency int) int {
	return int(commonGetAdjustCount(false, concurrency))
}

// GetAdjustedBlockSize gets the block size after alignment.
func GetAdjustedBlockSize(totalBufSize uint64, defBlockSize int) int {
	// In the case of table with many indexes, the buffer size may be much
	// smaller than the block size, so aligning size to block size will make
	// the memory size of each writer too large and cause OOM.
	// So we adjust the block size when the aligned size is 1.1 times larger
	// than memSizePerWriter to prevent OOM.
	alignedSize := membuf.GetAlignedSize(totalBufSize, uint64(defBlockSize))
	if float64(alignedSize)/float64(totalBufSize) > 1.1 {
		return int(totalBufSize)
	}
	return defBlockSize
}

// rangePropertiesCollector collects range properties for each range. The zero
// value of rangePropertiesCollector is not ready to use, should call reset()
// first.
type rangePropertiesCollector struct {
	props        []*rangeProperty
	currProp     *rangeProperty
	propSizeDist uint64
	propKeysDist uint64
}

// size: the file size after adding 'data'
func (rc *rangePropertiesCollector) onNextEncodedData(data []byte, size uint64) {
	keyLen := binary.BigEndian.Uint64(data)
	key := data[2*lengthBytes : 2*lengthBytes+keyLen]

	if len(rc.currProp.firstKey) == 0 {
		rc.currProp.firstKey = key
	}
	rc.currProp.lastKey = key

	rc.currProp.size += uint64(len(data) - 2*lengthBytes)
	rc.currProp.keys++

	if rc.currProp.size >= rc.propSizeDist ||
		rc.currProp.keys >= rc.propKeysDist {
		newProp := *rc.currProp
		rc.props = append(rc.props, &newProp)
		// reset currProp, and start to update this prop.
		rc.currProp.firstKey = nil
		rc.currProp.offset = size
		rc.currProp.keys = 0
		rc.currProp.size = 0
	}
}

func (rc *rangePropertiesCollector) onFileEnd() {
	if rc.currProp.keys > 0 {
		newProp := *rc.currProp
		rc.props = append(rc.props, &newProp)
	}
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
	Min tidbkv.Key
	Max tidbkv.Key
	// TotalSize is the total size of the KV written by this writer.
	// depends on onDup setting, duplicates might not be included.
	TotalSize uint64
	// TotalCnt is the total count of the KV written by this writer.
	// depends on onDup setting, duplicates might not be included.
	TotalCnt uint64
	// KVFileCount is the total count of the KV files written by this writer.
	KVFileCount        int
	MultipleFilesStats []MultipleFilesStat
	ConflictInfo       engineapi.ConflictInfo
}

// OnWriterCloseFunc is the callback function when a writer is closed.
type OnWriterCloseFunc func(summary *WriterSummary)

// dummyOnWriterCloseFunc is a dummy OnWriterCloseFunc.
func dummyOnWriterCloseFunc(*WriterSummary) {}

// WriterBuilder builds a new Writer.
type WriterBuilder struct {
	groupOffset  int
	memSizeLimit uint64
	blockSize    int
	propSizeDist uint64
	propKeysDist uint64
	onClose      OnWriterCloseFunc
	tikvCodec    tikv.Codec
	onDup        engineapi.OnDuplicateKey
}

// NewWriterBuilder creates a WriterBuilder.
func NewWriterBuilder() *WriterBuilder {
	return &WriterBuilder{
		memSizeLimit: DefaultMemSizeLimit,
		blockSize:    DefaultBlockSize,
		propSizeDist: defaultPropSizeDist,
		propKeysDist: defaultPropKeysDist,
		onClose:      dummyOnWriterCloseFunc,
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
func (b *WriterBuilder) SetOnCloseFunc(onClose OnWriterCloseFunc) *WriterBuilder {
	if onClose == nil {
		onClose = dummyOnWriterCloseFunc
	}
	b.onClose = onClose
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

// SetTiKVCodec sets the tikv codec of the writer.
func (b *WriterBuilder) SetTiKVCodec(codec tikv.Codec) *WriterBuilder {
	b.tikvCodec = codec
	return b
}

// SetOnDup sets the action when checkDup enabled and a duplicate key is found.
func (b *WriterBuilder) SetOnDup(onDup engineapi.OnDuplicateKey) *WriterBuilder {
	b.onDup = onDup
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
	p := membuf.NewPool(
		membuf.WithBlockNum(0),
		membuf.WithBlockSize(b.blockSize),
	)
	rnd := rand.New(rand.NewSource(getHash(filenamePrefix)))
	ret := &Writer{
		rc: &rangePropertiesCollector{
			props:        make([]*rangeProperty, 0, 1024),
			currProp:     &rangeProperty{},
			propSizeDist: b.propSizeDist,
			propKeysDist: b.propKeysDist,
		},
		store:          store,
		kvBuffer:       p.NewBuffer(membuf.WithBufferMemoryLimit(b.memSizeLimit)),
		currentSeq:     0,
		filenamePrefix: filenamePrefix,
		rnd:            rnd,
		writerID:       writerID,
		groupOffset:    b.groupOffset,
		onClose:        b.onClose,
		onDup:          b.onDup,
		closed:         false,
		multiFileStats: make([]MultipleFilesStat, 0),
		fileMinKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		fileMaxKeys:    make([]tidbkv.Key, 0, multiFileStatNum),
		tikvCodec:      b.tikvCodec,
	}

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

	rnd := rand.New(rand.NewSource(getHash(filenamePrefix)))

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
		rnd:            rnd,
		kvStore:        nil,
		onClose:        b.onClose,
		closed:         false,
		onDup:          b.onDup,
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
	rnd            *rand.Rand

	rc *rangePropertiesCollector

	kvBuffer    *membuf.Buffer
	kvLocations []membuf.SliceLocation
	kvSize      int64

	onClose OnWriterCloseFunc
	onDup   engineapi.OnDuplicateKey
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
	// since we have 1 stat file per kv file, so no need to count it separately.
	kvFileCount int

	tikvCodec tikv.Codec
	// duplicate key's statistics.
	conflictInfo engineapi.ConflictInfo
}

// WriteRow implements ingest.Writer.
func (w *Writer) WriteRow(ctx context.Context, key, val []byte, handle tidbkv.Handle) error {
	if w.tikvCodec != nil {
		key = w.tikvCodec.EncodeKey(key)
	}

	keyLen := len(key)
	length := keyLen + len(val) + lengthBytes*2
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
	binary.BigEndian.AppendUint64(dataBuf[:0], uint64(keyLen))
	binary.BigEndian.AppendUint64(dataBuf[:lengthBytes], uint64(len(val)))
	copy(dataBuf[2*lengthBytes:], key)
	copy(dataBuf[2*lengthBytes+keyLen:], val)

	w.kvLocations = append(w.kvLocations, loc)
	// TODO: maybe we can unify the size calculation during write to store.
	w.kvSize += int64(keyLen + len(val))
	w.batchSize += uint64(length)
	return nil
}

// LockForWrite implements ingest.Writer.
// Since flushKVs is thread-safe in external storage writer,
// this is implemented as noop.
func (w *Writer) LockForWrite() func() {
	return func() {}
}

// WrittenBytes returns the number of bytes written by this writer.
func (w *Writer) WrittenBytes() int64 {
	return int64(w.totalSize)
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

	logutil.Logger(ctx).Info("close writer",
		zap.String("writerID", w.writerID),
		zap.Int("kv-cnt-cap", cap(w.kvLocations)),
		zap.String("minKey", hex.EncodeToString(w.minKey)),
		zap.String("maxKey", hex.EncodeToString(w.maxKey)),
		zap.Int("kv-file-count", w.kvFileCount),
		zap.Int("dup-file-count", len(w.conflictInfo.Files)),
		zap.String("total-size", units.BytesSize(float64(w.totalSize))),
		zap.Uint64("total-kv-cnt", w.totalCnt),
	)

	w.kvLocations = nil
	w.onClose(&WriterSummary{
		WriterID:           w.writerID,
		GroupOffset:        w.groupOffset,
		Seq:                w.currentSeq,
		Min:                w.minKey,
		Max:                w.maxKey,
		TotalSize:          w.totalSize,
		TotalCnt:           w.totalCnt,
		KVFileCount:        w.kvFileCount,
		MultipleFilesStats: w.multiFileStats,
		ConflictInfo:       w.conflictInfo,
	})
	return nil
}

func (w *Writer) recordMinMax(newMin, newMax tidbkv.Key) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
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
	var (
		dupFound bool
		dupLoc   membuf.SliceLocation
	)
	slices.SortFunc(w.kvLocations, func(i, j membuf.SliceLocation) int {
		res := bytes.Compare(w.getKeyByLoc(&i), w.getKeyByLoc(&j))
		if res == 0 && !dupFound {
			dupFound = true
			dupLoc = i
		}
		return res
	})
	sortDuration := time.Since(sortStart)
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort").Observe(sortDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort").Observe(float64(w.batchSize) / 1024.0 / 1024.0 / sortDuration.Seconds())

	batchKVCnt := len(w.kvLocations)
	var (
		dupLocs []membuf.SliceLocation
		dupCnt  int
	)
	if dupFound {
		switch w.onDup {
		case engineapi.OnDuplicateKeyIgnore:
		case engineapi.OnDuplicateKeyRecord:
			// we don't have a global view, so need to keep duplicates with duplicate
			// count <= 2, so later we can find them.
			w.kvLocations, dupLocs, dupCnt = removeDuplicatesMoreThanTwo(w.kvLocations, w.getKeyByLoc)
			w.kvSize = w.reCalculateKVSize()
		case engineapi.OnDuplicateKeyRemove:
			w.kvLocations, _, dupCnt = removeDuplicates(w.kvLocations, w.getKeyByLoc, false)
			w.kvSize = w.reCalculateKVSize()
		case engineapi.OnDuplicateKeyError:
			dupKey := slices.Clone(w.getKeyByLoc(&dupLoc))
			dupValue := slices.Clone(w.getValueByLoc(&dupLoc))
			return common.ErrFoundDuplicateKeys.FastGenByArgs(dupKey, dupValue)
		}
	}

	writeStartTime := time.Now()
	var dataFile, statFile, dupFile string
	// due to current semantic of OnDuplicateKeyRecord, if len(w.kvLocations) = 0,
	// len(dupLocs) is also 0
	if len(w.kvLocations) > 0 {
		for i := range flushKVsRetryTimes {
			dataFile, statFile, dupFile, err = w.flushSortedKVs(ctx, dupLocs)
			if err == nil || ctx.Err() != nil {
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
	}
	writeDuration := time.Since(writeStartTime)
	logger.Info("flush kv",
		zap.Uint64("bytes", w.batchSize),
		zap.Int("kv-cnt", batchKVCnt),
		zap.Duration("sort-time", sortDuration),
		zap.Duration("write-time", writeDuration),
		zap.String("sort-speed(kv/s)", getSpeed(uint64(batchKVCnt), sortDuration.Seconds(), false)),
		zap.String("writer-id", w.writerID),
		zap.Stringer("on-dup", w.onDup),
		zap.Int("dup-cnt", dupCnt),
		zap.Int("recorded-dup-cnt", len(dupLocs)),
	)
	totalDuration := time.Since(sortStart)
	metrics.GlobalSortWriteToCloudStorageDuration.WithLabelValues("sort_and_write").Observe(totalDuration.Seconds())
	metrics.GlobalSortWriteToCloudStorageRate.WithLabelValues("sort_and_write").Observe(float64(w.batchSize) / 1024.0 / 1024.0 / totalDuration.Seconds())

	// maintain 500-batch statistics
	if len(w.kvLocations) > 0 {
		w.totalCnt += uint64(len(w.kvLocations))
		w.totalSize += uint64(w.kvSize)
		w.kvFileCount++

		minKey, maxKey := w.getKeyByLoc(&w.kvLocations[0]), w.getKeyByLoc(&w.kvLocations[len(w.kvLocations)-1])
		w.recordMinMax(minKey, maxKey)

		w.addNewKVFile2MultiFileStats(dataFile, statFile, minKey, maxKey)
	}
	if fromClose && len(w.multiFileStats) > 0 {
		w.multiFileStats[len(w.multiFileStats)-1].build(w.fileMinKeys, w.fileMaxKeys)
	}

	// maintain dup statistics
	if len(dupLocs) > 0 {
		w.conflictInfo.Merge(&engineapi.ConflictInfo{
			Count: uint64(len(dupLocs)),
			Files: []string{dupFile},
		})
	}

	w.kvLocations = w.kvLocations[:0]
	w.kvSize = 0
	w.kvBuffer.Reset()
	w.batchSize = 0
	w.currentSeq++
	return nil
}

func (w *Writer) addNewKVFile2MultiFileStats(dataFile, statFile string, minKey, maxKey []byte) {
	l := len(w.multiFileStats)
	if l == 0 || len(w.multiFileStats[l-1].Filenames) == multiFileStatNum {
		if l > 0 {
			w.multiFileStats[l-1].build(w.fileMinKeys, w.fileMaxKeys)
		}
		w.multiFileStats = append(w.multiFileStats, MultipleFilesStat{
			Filenames: make([][2]string, 0, multiFileStatNum),
		})
		w.fileMinKeys = w.fileMinKeys[:0]
		w.fileMaxKeys = w.fileMaxKeys[:0]

		l = len(w.multiFileStats)
	}

	w.multiFileStats[l-1].Filenames = append(w.multiFileStats[l-1].Filenames,
		[2]string{dataFile, statFile},
	)
	w.fileMinKeys = append(w.fileMinKeys, tidbkv.Key(minKey).Clone())
	w.fileMaxKeys = append(w.fileMaxKeys, tidbkv.Key(maxKey).Clone())
}

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
	data, stats storage.ExternalFileWriter,
	err error,
) {
	dataPath := filepath.Join(w.getPartitionedPrefix(), strconv.Itoa(w.currentSeq))
	dataWriter, err := w.store.Create(ctx, dataPath, &storage.WriterOption{
		Concurrency: 20,
		PartSize:    MinUploadPartSize,
	})
	if err != nil {
		return "", "", nil, nil, err
	}
	statPath := filepath.Join(w.getPartitionedPrefix()+statSuffix, strconv.Itoa(w.currentSeq))
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

func (w *Writer) createDupWriter(ctx context.Context) (string, storage.ExternalFileWriter, error) {
	path := filepath.Join(w.getPartitionedPrefix()+dupSuffix, strconv.Itoa(w.currentSeq))
	writer, err := w.store.Create(ctx, path, &storage.WriterOption{
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
