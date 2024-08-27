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

package extsort

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	sstFileSuffix            = ".sst"
	tmpFileSuffix            = ".tmp"
	kvStatsPropKey           = "extsort.kvstats"
	defaultKVStatsBucketSize = 1 << 20

	// If the file exists, it means the disk sorter is sorted.
	diskSorterSortedFile   = "sorted"
	diskSorterStateWriting = 0
	diskSorterStateSorting = 1
	diskSorterStateSorted  = 2
)

type fileMetadata struct {
	fileNum  int
	startKey []byte // inclusive
	endKey   []byte // exclusive
	lastKey  []byte // the last key in the file
	kvStats  kvStats
}

type kvStats struct {
	Histogram []kvStatsBucket `json:"histogram"`
}

type kvStatsBucket struct {
	// Size is the kv size between the upper bound of previous bucket
	// (exclusive) and the upper bound of current bucket (inclusive).
	Size int `json:"size"`
	// UpperBound is the upper inclusive bound of the bucket.
	UpperBound []byte `json:"upperBound"`
}

// kvStatsCollector implements sstable.TablePropertyCollector.
// It collects kv stats of the sstables.
type kvStatsCollector struct {
	bucketSize int
	buckets    []kvStatsBucket
	curSize    int
	lastKey    []byte
}

func newKVStatsCollector(bucketSize int) *kvStatsCollector {
	return &kvStatsCollector{
		bucketSize: bucketSize,
	}
}

// Add is called when a key-value pair is added to the sstable.
// In our case, it's guaranteed that the key-value pairs are added in order,
// and key.Trailer must be sstable.InternalKeyKindSet.
func (c *kvStatsCollector) Add(key sstable.InternalKey, value []byte) error {
	c.curSize += len(key.UserKey) + len(value)
	c.lastKey = append(c.lastKey[:0], key.UserKey...)
	if c.curSize >= c.bucketSize {
		c.addBucket()
	}
	return nil
}

func (c *kvStatsCollector) addBucket() {
	c.buckets = append(c.buckets, kvStatsBucket{
		Size:       c.curSize,
		UpperBound: slices.Clone(c.lastKey),
	})
	c.curSize = 0
}

func (c *kvStatsCollector) Finish(userProps map[string]string) error {
	if c.curSize > 0 {
		c.addBucket()
	}
	stats := kvStats{Histogram: c.buckets}
	data, err := json.Marshal(&stats)
	if err != nil {
		return errors.Trace(err)
	}
	userProps[kvStatsPropKey] = string(data)
	return nil
}

func (*kvStatsCollector) Name() string {
	return kvStatsPropKey
}

func makeFilename(fs vfs.FS, dirname string, fileNum int) string {
	return fs.PathJoin(dirname, fmt.Sprintf("%06d%s", fileNum, sstFileSuffix))
}

func parseFilename(fs vfs.FS, filename string) (fileNum int, ok bool) {
	filename = fs.PathBase(filename)
	if !strings.HasSuffix(filename, sstFileSuffix) {
		return 0, false
	}
	v, err := strconv.ParseInt(filename[:len(filename)-len(sstFileSuffix)], 10, 64)
	if err != nil {
		return 0, false
	}
	return int(v), true
}

type sstWriter struct {
	w         *sstable.Writer
	fs        vfs.FS
	fileNum   int
	tmpPath   string
	destPath  string
	err       error
	onSuccess func(meta *fileMetadata)
}

func newSSTWriter(
	fs vfs.FS,
	dirname string,
	fileNum int,
	kvStatsBucketSize int,
	onSuccess func(meta *fileMetadata),
) (*sstWriter, error) {
	destPath := makeFilename(fs, dirname, fileNum)
	tmpPath := destPath + tmpFileSuffix
	f, err := vfs.Default.Create(tmpPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	writable := objstorageprovider.NewFileWritable(f)
	w := sstable.NewWriter(writable, sstable.WriterOptions{
		TablePropertyCollectors: []func() sstable.TablePropertyCollector{
			func() sstable.TablePropertyCollector {
				return newKVStatsCollector(kvStatsBucketSize)
			},
		},
	})

	sw := &sstWriter{
		w:         w,
		fs:        fs,
		fileNum:   fileNum,
		tmpPath:   tmpPath,
		destPath:  destPath,
		onSuccess: onSuccess,
	}
	return sw, nil
}

func (sw *sstWriter) Set(key, value []byte) error {
	sw.err = sw.w.Set(key, value)
	return errors.Trace(sw.err)
}

func (sw *sstWriter) Close() (retErr error) {
	defer func() {
		if retErr == nil {
			retErr = sw.fs.Rename(sw.tmpPath, sw.destPath)
		} else {
			_ = sw.fs.Remove(sw.tmpPath)
		}
	}()
	closeErr := sw.w.Close()
	if err := goerrors.Join(sw.err, closeErr); err != nil {
		return errors.Trace(err)
	}

	if sw.onSuccess != nil {
		writerMeta, err := sw.w.Metadata()
		if err != nil {
			return errors.Trace(err)
		}
		meta := &fileMetadata{
			fileNum: sw.fileNum,
		}
		meta.startKey = slices.Clone(writerMeta.SmallestPoint.UserKey)
		meta.lastKey = slices.Clone(writerMeta.LargestPoint.UserKey)
		// Make endKey is exclusive. To avoid unnecessary overlap,
		// we append 0 to make endKey is the smallest key which is
		// greater than the last key.
		meta.endKey = append(meta.lastKey, 0)
		prop, ok := writerMeta.Properties.UserProperties[kvStatsPropKey]
		if ok {
			if err := json.Unmarshal([]byte(prop), &meta.kvStats); err != nil {
				return errors.Trace(err)
			}
		}
		sw.onSuccess(meta)
	}
	return nil
}

type sstReaderPool struct {
	fs      vfs.FS
	dirname string
	cache   *pebble.Cache
	mu      struct {
		syncutil.RWMutex
		readers map[int]struct {
			reader *sstable.Reader
			refs   *atomic.Int32
		} // fileNum -> reader and its refs
	}
}

func newSSTReaderPool(fs vfs.FS, dirname string, cache *pebble.Cache) *sstReaderPool {
	pool := &sstReaderPool{
		fs:      fs,
		dirname: dirname,
		cache:   cache,
	}
	pool.mu.readers = make(map[int]struct {
		reader *sstable.Reader
		refs   *atomic.Int32
	})
	return pool
}

func (p *sstReaderPool) get(fileNum int) (*sstable.Reader, error) {
	var reader *sstable.Reader
	withLock(p.mu.RLocker(), func() {
		if v, ok := p.mu.readers[fileNum]; ok {
			v.refs.Add(1)
			reader = v.reader
		}
	})
	if reader != nil {
		return reader, nil
	}

	// Create a new reader. Note that we don't hold the lock here,
	// since creating a reader can be expensive, and we don't want
	// to block other readers' creation.
	f, err := p.fs.Open(makeFilename(p.fs, p.dirname, fileNum))
	if err != nil {
		return nil, errors.Trace(err)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return nil, errors.Trace(err)
	}
	reader, err = sstable.NewReader(readable, sstable.ReaderOptions{
		Cache: p.cache,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	var toRet, toClose *sstable.Reader
	withLock(&p.mu, func() {
		// Someone might have added the reader to the pool.
		if v, ok := p.mu.readers[fileNum]; ok {
			toClose = reader
			v.refs.Add(1)
			toRet = v.reader
			return
		}
		refs := new(atomic.Int32)
		refs.Store(1)
		p.mu.readers[fileNum] = struct {
			reader *sstable.Reader
			refs   *atomic.Int32
		}{reader: reader, refs: refs}
		toRet = reader
	})
	if toClose != nil {
		if err := toClose.Close(); err != nil {
			_ = p.unref(fileNum)
			return nil, errors.Trace(err)
		}
	}
	return toRet, nil
}

func (p *sstReaderPool) unref(fileNum int) error {
	var closer io.Closer
	withLock(&p.mu, func() {
		v, ok := p.mu.readers[fileNum]
		if !ok {
			panic(fmt.Sprintf("sstReaderPool: unref a reader that does not exist: %d", fileNum))
		}
		if v.refs.Add(-1) == 0 {
			delete(p.mu.readers, fileNum)
			closer = v.reader
		}
	})
	if closer != nil {
		return errors.Trace(closer.Close())
	}
	return nil
}

func withLock(l sync.Locker, f func()) {
	l.Lock()
	defer l.Unlock()
	f()
}

type sstIter struct {
	iter    sstable.Iterator
	key     *sstable.InternalKey
	value   []byte
	onClose func() error
}

func (si *sstIter) Seek(key []byte) bool {
	k, v := si.iter.SeekGE(key, 0)
	si.key = k
	si.value, _, _ = v.Value(nil)
	return si.key != nil
}

func (si *sstIter) First() bool {
	k, v := si.iter.SeekGE(nil, 0)
	si.key = k
	si.value, _, _ = v.Value(nil)
	return si.key != nil
}

func (si *sstIter) Next() bool {
	k, v := si.iter.Next()
	si.key = k
	si.value, _, _ = v.Value(nil)
	return si.key != nil
}

func (si *sstIter) Last() bool {
	k, v := si.iter.Last()
	si.key = k
	si.value, _, _ = v.Value(nil)
	return si.key != nil
}

func (si *sstIter) Valid() bool {
	return si.key != nil
}

func (si *sstIter) Error() error {
	return errors.Trace(si.iter.Error())
}

func (si *sstIter) UnsafeKey() []byte {
	return si.key.UserKey
}

func (si *sstIter) UnsafeValue() []byte {
	return si.value
}

func (si *sstIter) Close() error {
	err := si.iter.Close()
	if si.onClose != nil {
		err = goerrors.Join(err, si.onClose())
	}
	return errors.Trace(err)
}

type mergingIter struct {
	heap mergingIterHeap
	// orderedFiles is the files to be merged. It is ordered by start key.
	orderedFiles []*fileMetadata
	// nextFileIndex is the index of the next file to be read.
	// mergingIter only maintains minimum set of opened files.
	// If current key is less than the start key of next file,
	// the next file does not need to be opened.
	nextFileIndex int
	openIter      openIterFunc
	curKey        []byte // only used in Next() to avoid alloc
	err           error
}

type mergingIterItem struct {
	Iterator
	index int // index in orderedFiles
}

type mergingIterHeap []mergingIterItem

func (h mergingIterHeap) Len() int {
	return len(h)
}

func (h mergingIterHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h mergingIterHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].UnsafeKey(), h[j].UnsafeKey()) < 0
}

func (h *mergingIterHeap) Push(x any) {
	*h = append(*h, x.(mergingIterItem))
}

func (h *mergingIterHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type openIterFunc func(file *fileMetadata) (Iterator, error)

// newMergingIter returns an iterator that merges the given files.
// orderedFiles must be ordered by start key, otherwise it panics.
func newMergingIter(orderedFiles []*fileMetadata, openIter openIterFunc) *mergingIter {
	if !slices.IsSortedFunc(orderedFiles, func(a, b *fileMetadata) int {
		return bytes.Compare(a.startKey, b.startKey)
	}) {
		panic("newMergingIter: orderedFiles are not ordered by start key")
	}
	return &mergingIter{
		orderedFiles: orderedFiles,
		openIter:     openIter,
	}
}

func (m *mergingIter) Seek(key []byte) bool {
	m.err = nil
	oldHeap := m.heap
	m.heap = nil

	var toClose []mergingIterItem
	openedIters := make(map[int]struct{})
	for i, item := range oldHeap {
		file := m.orderedFiles[item.index]
		if bytes.Compare(key, file.startKey) >= 0 &&
			bytes.Compare(key, file.endKey) < 0 &&
			item.Seek(key) {
			openedIters[item.index] = struct{}{}
			heap.Push(&m.heap, item)
		} else {
			m.err = item.Error()
			if m.err != nil {
				// Iterators in m.heap should be closed by mergingIter.Close().
				toClose = append(toClose, oldHeap[i:]...)
				break
			}
			// The seek key is not in the range of [startKey, endKey), close it.
			toClose = append(toClose, item)
		}
	}
	for _, item := range toClose {
		if err := item.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	if m.err != nil {
		return false
	}

	m.nextFileIndex = len(m.orderedFiles)
	for i, file := range m.orderedFiles {
		if bytes.Compare(file.startKey, key) > 0 {
			m.nextFileIndex = i
			break
		}
		if _, ok := openedIters[i]; ok {
			continue
		}
		if bytes.Compare(file.endKey, key) <= 0 {
			continue
		}
		iter, err := m.openIter(file)
		if err != nil {
			m.err = err
			return false
		}
		if iter.Seek(key) {
			heap.Push(&m.heap, mergingIterItem{
				Iterator: iter,
				index:    i,
			})
		} else {
			m.err = iter.Error()
			closeErr := iter.Close()
			m.err = goerrors.Join(m.err, closeErr)
			if m.err != nil {
				return false
			}
		}
	}
	// Although we have opened all files whose range contains the seek key,
	// it is possible the seeked key is greater than the original seek key.
	// So we need to check if the next file needs to be opened.
	//
	// Consider the following case:
	//
	// file 1: a--------f
	// file 2:   b------f
	// file 3:       d--f
	//
	// If we seek to "c", we will open file 1 and file 2 first. However, the
	// smallest key which is greater than "c" is "e", so we need to open file 3
	// to check if it has keys less than "e".
	return m.maybeOpenNextFiles()
}

func (m *mergingIter) First() bool {
	return m.Seek(nil)
}

func (m *mergingIter) Next() bool {
	m.err = nil
	m.curKey = append(m.curKey[:0], m.heap[0].UnsafeKey()...)
	//revive:disable:empty-block
	for m.next() && bytes.Equal(m.heap[0].UnsafeKey(), m.curKey) {
	}
	//revive:enable:empty-block
	return m.Valid()
}

func (m *mergingIter) next() bool {
	if len(m.heap) == 0 {
		return false
	}
	if m.heap[0].Next() {
		heap.Fix(&m.heap, 0)
	} else {
		m.err = m.heap[0].Error()
		if m.err != nil {
			return false
		}
		x := heap.Pop(&m.heap)
		item := x.(mergingIterItem)
		m.err = item.Close()
		if m.err != nil {
			return false
		}
	}
	return m.maybeOpenNextFiles()
}

func (m *mergingIter) maybeOpenNextFiles() bool {
	for m.nextFileIndex < len(m.orderedFiles) {
		file := m.orderedFiles[m.nextFileIndex]
		if len(m.heap) > 0 && bytes.Compare(m.heap[0].UnsafeKey(), file.startKey) < 0 {
			break
		}
		// The next file may overlap with the min key in the heap, so we need to
		// open it and push it into the heap.
		iter, err := m.openIter(file)
		if err != nil {
			m.err = err
			return false
		}
		if !iter.First() {
			m.err = iter.Error()
			closeErr := iter.Close()
			m.err = goerrors.Join(m.err, closeErr)
			if m.err != nil {
				return false
			}
		}
		heap.Push(&m.heap, mergingIterItem{
			Iterator: iter,
			index:    m.nextFileIndex,
		})
		m.nextFileIndex++
	}
	return len(m.heap) > 0
}

func (m *mergingIter) Last() bool {
	m.err = nil
	// Close all opened iterators.
	for _, item := range m.heap {
		if err := item.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	m.heap = nil
	m.nextFileIndex = len(m.orderedFiles)
	if m.err != nil {
		return false
	}

	// Sort files by last key in reverse order.
	files := slices.Clone(m.orderedFiles)
	slices.SortFunc(files, func(a, b *fileMetadata) int {
		return bytes.Compare(b.lastKey, a.lastKey)
	})

	// Since we don't need to implement Prev() method,
	// we can just open the file with the largest last key.
	for _, file := range files {
		iter, err := m.openIter(file)
		if err != nil {
			m.err = err
			return false
		}
		if iter.Last() {
			index := -1
			for i := range m.orderedFiles {
				if m.orderedFiles[i] == file {
					index = i
					break
				}
			}
			heap.Push(&m.heap, mergingIterItem{
				Iterator: iter,
				index:    index,
			})
			break
		}
		m.err = iter.Error()
		// If the file is empty, we can close it and continue to open the next file.
		closeErr := iter.Close()
		m.err = goerrors.Join(m.err, closeErr)
		if m.err != nil {
			return false
		}
	}
	return m.Valid()
}

func (m *mergingIter) Valid() bool {
	return m.err == nil && len(m.heap) > 0
}

func (m *mergingIter) Error() error {
	return m.err
}

func (m *mergingIter) UnsafeKey() []byte {
	return m.heap[0].UnsafeKey()
}

func (m *mergingIter) UnsafeValue() []byte {
	return m.heap[0].UnsafeValue()
}

func (m *mergingIter) Close() error {
	m.err = nil
	for _, item := range m.heap {
		if err := item.Close(); err != nil && m.err == nil {
			m.err = err
		}
	}
	return m.err
}

// DiskSorterOptions holds the optional parameters for DiskSorter.
type DiskSorterOptions struct {
	// Concurrency is the maximum number of goroutines that can be used to
	// sort data in parallel.
	//
	// The default value is runtime.GOMAXPROCS(0).
	Concurrency int

	// WriterBufferSize is the size of the buffer used by the writer.
	// Larger buffer size can improve the write and sort performance,
	// and reduce the number of disk operations.
	//
	// The default value is 128MB.
	WriterBufferSize int

	// CompactionThreshold is maximum overlap depth necessary to trigger a
	// compaction. The overlap depth is the number of files that overlap at
	// same interval.
	//
	// For example, consider the following files:
	//
	// file 0: a-----d
	// file 1:   b-----e
	// file 2:     c-------g
	// file 3:       d---f
	//
	// The overlap depth of these files is 3, because file 0, 1, 2 overlap at
	// the interval [c, d), and file 1, 2, 3 overlap at the interval [d, e).
	//
	// If the overlap depth reached CompactionThreshold, the sorter will compact
	// files to reduce the overlap depth during sorting. The larger the overlap
	// depth, the larger read amplification will be during iteration. This is a
	// trade-off between read amplification and sorting cost. Setting this value
	// to math.MaxInt will disable the compaction.
	//
	// The default value is 16.
	CompactionThreshold int

	// MaxCompactionDepth is the maximum files involved in a single compaction.
	// The minimum value is 2. Any value less than 2 will be treated as not set.
	//
	// The default value is 64.
	MaxCompactionDepth int

	// MaxCompactionSize is the maximum size of key-value pairs involved in a
	// single compaction.
	//
	// The default value is 512MB.
	MaxCompactionSize int

	// Logger is used to write log messages.
	//
	// The default value is log.L().
	Logger *zap.Logger
}

func (o *DiskSorterOptions) ensureDefaults() {
	if o.Concurrency == 0 {
		o.Concurrency = runtime.GOMAXPROCS(0)
	}
	if o.WriterBufferSize == 0 {
		o.WriterBufferSize = 128 << 20
	}
	if o.CompactionThreshold == 0 {
		o.CompactionThreshold = 16
	}
	if o.MaxCompactionDepth < 2 {
		o.MaxCompactionDepth = 64
	}
	if o.MaxCompactionSize == 0 {
		o.MaxCompactionSize = 512 << 20
	}
	if o.Logger == nil {
		o.Logger = log.L()
	}
}

// DiskSorter is an external sorter that sorts data on disk.
type DiskSorter struct {
	opts    *DiskSorterOptions
	fs      vfs.FS
	dirname string
	// cache is shared by all sst readers.
	cache        *pebble.Cache
	readerPool   *sstReaderPool
	idAlloc      atomic.Int64
	state        atomic.Int32
	pendingFiles struct {
		syncutil.Mutex
		files []*fileMetadata
	}
	// The list of files that have been sorted.
	// They must be ordered by its start key.
	orderedFiles []*fileMetadata
}

// OpenDiskSorter opens a DiskSorter with the given directory.
func OpenDiskSorter(dirname string, opts *DiskSorterOptions) (*DiskSorter, error) {
	if opts == nil {
		opts = &DiskSorterOptions{}
	}
	opts.ensureDefaults()

	fs := vfs.Default
	if dirname == "" {
		fs = vfs.NewMem()
	}
	if err := fs.MkdirAll(dirname, 0755); err != nil {
		return nil, errors.Trace(err)
	}
	cache := pebble.NewCache(8 << 20)
	readerPool := newSSTReaderPool(fs, dirname, cache)
	d := &DiskSorter{
		opts:       opts,
		fs:         fs,
		dirname:    dirname,
		cache:      cache,
		readerPool: readerPool,
	}
	if err := d.init(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *DiskSorter) init() error {
	list, err := d.fs.List(d.dirname)
	if err != nil {
		return errors.Trace(err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	outputCh := make(chan *fileMetadata, len(list))
	for _, name := range list {
		if strings.HasSuffix(name, tmpFileSuffix) {
			_ = d.fs.Remove(d.fs.PathJoin(d.dirname, name))
			continue
		}
		fileNum, ok := parseFilename(d.fs, name)
		if !ok {
			continue
		}
		if int64(fileNum) > d.idAlloc.Load() {
			d.idAlloc.Store(int64(fileNum))
		}
		g.Go(func() error {
			if ctx.Err() != nil {
				return errors.Trace(ctx.Err())
			}
			file, err := d.readFileMetadata(fileNum)
			if err != nil {
				return err
			}
			outputCh <- file
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	close(outputCh)
	files := make([]*fileMetadata, 0, len(outputCh))
	for file := range outputCh {
		files = append(files, file)
	}
	if _, err := d.fs.Stat(d.fs.PathJoin(d.dirname, diskSorterSortedFile)); err == nil {
		slices.SortFunc(files, func(a, b *fileMetadata) int {
			return bytes.Compare(a.startKey, b.startKey)
		})
		d.orderedFiles = files
		d.state.Store(diskSorterStateSorted)
	} else {
		d.pendingFiles.files = files
		d.state.Store(diskSorterStateWriting)
	}
	return nil
}

func (d *DiskSorter) readFileMetadata(fileNum int) (*fileMetadata, error) {
	reader, err := d.readerPool.get(fileNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = d.readerPool.unref(fileNum)
	}()

	meta := &fileMetadata{
		fileNum: fileNum,
	}
	if prop, ok := reader.Properties.UserProperties[kvStatsPropKey]; ok {
		if err := json.Unmarshal([]byte(prop), &meta.kvStats); err != nil {
			return nil, errors.Trace(err)
		}
	}

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		_ = iter.Close()
	}()

	firstKey, _ := iter.First()
	if firstKey != nil {
		meta.startKey = slices.Clone(firstKey.UserKey)
	} else if err := iter.Error(); err != nil {
		return nil, errors.Trace(err)
	}

	lastKey, _ := iter.Last()
	if lastKey != nil {
		meta.lastKey = slices.Clone(lastKey.UserKey)
		// Make endKey is exclusive. To avoid unnecessary overlap,
		// we append 0 to make endKey is the smallest key which is
		// greater than the last key.
		meta.endKey = append(meta.lastKey, 0)
	} else if err := iter.Error(); err != nil {
		return nil, errors.Trace(err)
	}
	return meta, nil
}

// NewWriter implements the ExternalSorter.NewWriter.
func (d *DiskSorter) NewWriter(_ context.Context) (Writer, error) {
	if d.state.Load() > diskSorterStateWriting {
		return nil, errors.Errorf("diskSorter started sorting, cannot write more data")
	}
	return &diskSorterWriter{
		buf: make([]byte, d.opts.WriterBufferSize),
		newSSTWriter: func() (*sstWriter, error) {
			fileNum := int(d.idAlloc.Add(1))
			return newSSTWriter(d.fs, d.dirname, fileNum, defaultKVStatsBucketSize,
				func(meta *fileMetadata) {
					d.pendingFiles.Lock()
					d.pendingFiles.files = append(d.pendingFiles.files, meta)
					d.pendingFiles.Unlock()
				})
		},
	}, nil
}

// Sort implements the ExternalSorter.Sort.
func (d *DiskSorter) Sort(ctx context.Context) error {
	if d.state.Load() == diskSorterStateSorted {
		return nil
	}
	d.state.Store(diskSorterStateSorting)
	if err := d.doSort(ctx); err != nil {
		return err
	}
	d.state.Store(diskSorterStateSorted)

	// Persist the sorted state.
	f, err := d.fs.Create(d.fs.PathJoin(d.dirname, diskSorterSortedFile))
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(f.Close())
}

func (d *DiskSorter) doSort(ctx context.Context) error {
	d.pendingFiles.Lock()
	defer d.pendingFiles.Unlock()
	if len(d.pendingFiles.files) == 0 {
		return nil
	}
	d.orderedFiles = d.pendingFiles.files
	d.pendingFiles.files = nil
	slices.SortFunc(d.orderedFiles, func(a, b *fileMetadata) int {
		return bytes.Compare(a.startKey, b.startKey)
	})
	files := pickCompactionFiles(d.orderedFiles, d.opts.CompactionThreshold, d.opts.Logger)
	for len(files) > 0 {
		if err := d.compactFiles(ctx, files); err != nil {
			return err
		}
		files = pickCompactionFiles(d.orderedFiles, d.opts.CompactionThreshold, d.opts.Logger)
	}
	return nil
}

func (d *DiskSorter) compactFiles(ctx context.Context, files []*fileMetadata) error {
	// Split files into multiple compaction groups.
	// Each group will be compacted independently.
	groups := splitCompactionFiles(files, d.opts.MaxCompactionDepth)
	compactions := make([]*compaction, 0, len(groups))
	for _, group := range groups {
		compactions = append(compactions, buildCompactions(group, d.opts.MaxCompactionSize)...)
	}

	// Build file references.
	fileRefs := make(map[int]*atomic.Int32)
	for _, file := range files {
		fileRefs[file.fileNum] = new(atomic.Int32)
	}
	for _, c := range compactions {
		for _, file := range c.overlapFiles {
			fileRefs[file.fileNum].Add(1)
		}
	}
	d.opts.Logger.Info("total compactions", zap.Int("count", len(compactions)))

	// Run all compactions.
	removedFileNums := generic.NewSyncMap[int, struct{}](len(files))
	outputCh := make(chan *fileMetadata, len(compactions))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(d.opts.Concurrency)
	for _, c := range compactions {
		c := c
		g.Go(func() error {
			if gCtx.Err() != nil {
				return errors.Trace(gCtx.Err())
			}
			output, err := d.runCompaction(gCtx, c)
			if err != nil {
				return err
			}
			outputCh <- output
			for _, file := range c.overlapFiles {
				if fileRefs[file.fileNum].Add(-1) == 0 {
					_ = d.fs.Remove(makeFilename(d.fs, d.dirname, file.fileNum))
					removedFileNums.Store(file.fileNum, struct{}{})
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Update d.orderedFiles.
	close(outputCh)
	for output := range outputCh {
		d.orderedFiles = append(d.orderedFiles, output)
	}
	var newOrderedFiles []*fileMetadata
	for _, file := range d.orderedFiles {
		if _, ok := removedFileNums.Load(file.fileNum); !ok {
			newOrderedFiles = append(newOrderedFiles, file)
		}
	}
	slices.SortFunc(newOrderedFiles, func(a, b *fileMetadata) int {
		return bytes.Compare(a.startKey, b.startKey)
	})
	d.orderedFiles = newOrderedFiles
	return nil
}

func pickCompactionFiles(
	allFiles []*fileMetadata,
	compactionThreshold int,
	logger *zap.Logger,
) []*fileMetadata {
	type interval struct {
		key   []byte
		depth int
	}
	// intervals is a list of intervals that perfectly overlap file boundaries.
	// For example, if we have two files [a, c] and [b, d], the intervals will be
	// [a, b), [b, c), [c, d).
	intervals := make([]interval, 0, len(allFiles)*2)
	for _, file := range allFiles {
		intervals = append(intervals, interval{
			key:   file.startKey,
			depth: 1,
		})
		intervals = append(intervals, interval{
			key:   file.endKey,
			depth: -1,
		})
	}
	slices.SortFunc(intervals, func(a, b interval) int {
		return bytes.Compare(a.key, b.key)
	})

	// Compute the maximum overlap depth of each interval.
	// See https://en.wikipedia.org/wiki/Sweep_line_algorithm.
	maxDepth := 0
	n := 0
	for i := 1; i < len(intervals); i++ {
		intervals[i].depth += intervals[n].depth
		if intervals[i].depth > maxDepth {
			maxDepth = intervals[i].depth
		}
		// Merge adjacent intervals with the same key.
		if bytes.Equal(intervals[i].key, intervals[n].key) {
			intervals[n] = intervals[i]
		} else {
			n++
			intervals[n] = intervals[i]
		}
	}
	intervals = intervals[:n+1]
	if maxDepth < compactionThreshold {
		return nil
	}

	var files []*fileMetadata
	for _, file := range allFiles {
		minIntervalIndex := sort.Search(len(intervals), func(i int) bool {
			return bytes.Compare(intervals[i].key, file.startKey) >= 0
		})
		maxIntervalIndex := sort.Search(len(intervals), func(i int) bool {
			return bytes.Compare(intervals[i].key, file.endKey) >= 0
		})
		for i := minIntervalIndex; i < maxIntervalIndex; i++ {
			if intervals[i].depth >= compactionThreshold {
				files = append(files, file)
				break
			}
		}
	}
	logger.Info(
		"max overlap depth reached the compaction threshold, pick files to compact",
		zap.Int("maxDepth", maxDepth),
		zap.Int("threshold", compactionThreshold),
		zap.Int("fileCount", len(files)),
	)
	return files
}

func splitCompactionFiles(files []*fileMetadata, maxCompactionDepth int) [][]*fileMetadata {
	// Split files into non-overlapping groups.
	slices.SortFunc(files, func(a, b *fileMetadata) int {
		return bytes.Compare(a.startKey, b.startKey)
	})
	var groups [][]*fileMetadata
	curGroup := []*fileMetadata{files[0]}
	maxEndKey := files[0].endKey
	for _, file := range files[1:] {
		if bytes.Compare(file.startKey, maxEndKey) >= 0 {
			groups = append(groups, curGroup)
			curGroup = []*fileMetadata{file}
		} else {
			curGroup = append(curGroup, file)
		}
		if bytes.Compare(file.endKey, maxEndKey) > 0 {
			maxEndKey = file.endKey
		}
	}
	if len(curGroup) > 0 {
		groups = append(groups, curGroup)
	}

	var finalGroups [][]*fileMetadata
	// Compact each group of files.
	for _, group := range groups {
		numSubGroups := (len(group)-1)/maxCompactionDepth + 1
		subGroupSize := (len(group)-1)/numSubGroups + 1
		for i := 0; i < len(group); i += subGroupSize {
			j := i + subGroupSize
			if j > len(group) {
				j = len(group)
			}
			finalGroups = append(finalGroups, group[i:j])
		}
	}
	return finalGroups
}

type compaction struct {
	startKey []byte
	endKey   []byte
	// overlapFiles are files that overlap with the compaction range.
	// They are ordered by their start keys.
	overlapFiles []*fileMetadata
}

func buildCompactions(files []*fileMetadata, maxCompactionSize int) []*compaction {
	var startKey, endKey []byte
	buckets := make([]kvStatsBucket, 0, len(files))
	for _, file := range files {
		buckets = append(buckets, file.kvStats.Histogram...)
		if startKey == nil || bytes.Compare(file.startKey, startKey) < 0 {
			startKey = file.startKey
		}
		if endKey == nil || bytes.Compare(file.endKey, endKey) > 0 {
			endKey = file.endKey
		}
	}
	// If there is no kv stats, return a single compaction for all files.
	if len(buckets) == 0 {
		overlapFiles := slices.Clone(files)
		slices.SortFunc(overlapFiles, func(a, b *fileMetadata) int {
			return bytes.Compare(a.startKey, b.startKey)
		})
		return []*compaction{{
			startKey:     startKey,
			endKey:       endKey,
			overlapFiles: overlapFiles,
		}}
	}

	slices.SortFunc(buckets, func(a, b kvStatsBucket) int {
		return bytes.Compare(a.UpperBound, b.UpperBound)
	})
	// Merge buckets with the same upper bound.
	n := 0
	for i := 1; i < len(buckets); i++ {
		if bytes.Equal(buckets[n].UpperBound, buckets[i].UpperBound) {
			buckets[n].Size += buckets[i].Size
		} else {
			n++
			buckets[n] = buckets[i]
		}
	}
	buckets = buckets[:n+1]

	var (
		kvSize      int
		compactions []*compaction
	)
	// We assume that data is uniformly distributed in each file as well as
	// each bucket range. So we can simply merge buckets of different files.
	// Although the adjacent buckets may not be of the same file, the current
	// bucket size is approximately the size of kv pairs between the previous
	// bucket's upper bound and the current bucket's upper bound.
	//
	// Consider the following example:
	//
	// file 1 buckets: (a, 10),          (c, 10),          (e, 10)
	// file 2 buckets:          (b, 10),          (d, 10),          (f, 10)
	// merged buckets: (a, 10), (b, 10), (c, 10), (d, 10), (e, 10), (f, 10).
	//
	// For the adjacent buckets (b, 10) and (c, 10), the size of kv pairs
	// between b and c is approximately 10 (5 from file 1 and 5 from file 2).
	for i, bucket := range buckets {
		if i+1 == len(buckets) {
			compactions = append(compactions, &compaction{
				startKey: startKey,
				endKey:   endKey,
			})
			break
		}
		kvSize += bucket.Size
		if kvSize >= maxCompactionSize {
			compactions = append(compactions, &compaction{
				startKey: startKey,
				endKey:   bucket.UpperBound,
			})
			startKey = bucket.UpperBound
			kvSize = 0
		}
	}

	for _, c := range compactions {
		for _, file := range files {
			if !(bytes.Compare(file.endKey, c.startKey) <= 0 ||
				bytes.Compare(file.startKey, c.endKey) >= 0) {
				c.overlapFiles = append(c.overlapFiles, file)
			}
		}
		slices.SortFunc(c.overlapFiles, func(a, b *fileMetadata) int {
			return bytes.Compare(a.startKey, b.startKey)
		})
	}
	return compactions
}

func (d *DiskSorter) runCompaction(ctx context.Context, c *compaction) (*fileMetadata, error) {
	d.opts.Logger.Debug(
		"run compaction",
		zap.Binary("startKey", c.startKey),
		zap.Binary("endKey", c.endKey),
		zap.Int("fileCount", len(c.overlapFiles)),
	)

	var merged *fileMetadata
	onSuccess := func(meta *fileMetadata) { merged = meta }

	fileNum := int(d.idAlloc.Add(1))
	sw, err := newSSTWriter(d.fs, d.dirname, fileNum, defaultKVStatsBucketSize, onSuccess)
	if err != nil {
		return nil, err
	}
	swClosed := false
	defer func() {
		if !swClosed {
			_ = sw.Close()
		}
	}()

	iter := newMergingIter(c.overlapFiles, d.openIter)
	defer func() {
		_ = iter.Close()
	}()

	iterations := 0
	for iter.Seek(c.startKey); iter.Valid(); iter.Next() {
		if bytes.Compare(iter.UnsafeKey(), c.endKey) >= 0 {
			break
		}
		iterations++
		if iterations%1000 == 0 && ctx.Err() != nil {
			return nil, errors.Trace(ctx.Err())
		}
		if err := sw.Set(iter.UnsafeKey(), iter.UnsafeValue()); err != nil {
			return nil, err
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	err = sw.Close()
	swClosed = true
	if err != nil {
		return nil, err
	}
	return merged, nil
}

func (d *DiskSorter) openIter(file *fileMetadata) (Iterator, error) {
	reader, err := d.readerPool.get(file.fileNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		_ = reader.Close()
		return nil, errors.Trace(err)
	}
	return &sstIter{
		iter: iter,
		onClose: func() error {
			return d.readerPool.unref(file.fileNum)
		},
	}, nil
}

// IsSorted implements the ExternalSorter.IsSorted.
func (d *DiskSorter) IsSorted() bool {
	return d.state.Load() == diskSorterStateSorted
}

// NewIterator implements the ExternalSorter.NewIterator.
func (d *DiskSorter) NewIterator(_ context.Context) (Iterator, error) {
	if d.state.Load() != diskSorterStateSorted {
		return nil, errors.Errorf("diskSorter is not sorted")
	}
	return newMergingIter(d.orderedFiles, d.openIter), nil
}

// Close implements the ExternalSorter.Close.
func (d *DiskSorter) Close() error {
	d.cache.Unref()
	return nil
}

// CloseAndCleanup implements the ExternalSorter.CloseAndCleanup.
func (d *DiskSorter) CloseAndCleanup() error {
	d.cache.Unref()
	err := d.fs.RemoveAll(d.dirname)
	return errors.Trace(err)
}

type keyValue struct {
	key   []byte
	value []byte
}

type diskSorterWriter struct {
	kvs          []keyValue
	buf          []byte
	off          int
	newSSTWriter func() (*sstWriter, error)
}

func (w *diskSorterWriter) Put(key, value []byte) error {
	if w.off+len(key)+len(value) > len(w.buf) {
		if err := w.flush(); err != nil {
			return errors.Trace(err)
		}
		// The default buffer is too small, enlarge it to fit the key and value.
		// w.off must be 0 after flush.
		if len(key)+len(value) > len(w.buf) {
			w.buf = make([]byte, len(key)+len(value))
		}
	}

	var kv keyValue
	kv.key = w.buf[w.off : w.off+len(key)]
	w.off += len(key)
	copy(kv.key, key)
	if len(value) > 0 {
		kv.value = w.buf[w.off : w.off+len(value)]
		w.off += len(value)
		copy(kv.value, value)
	}
	w.kvs = append(w.kvs, kv)
	return nil
}

func (w *diskSorterWriter) Flush() error {
	if len(w.kvs) == 0 {
		return nil
	}
	return w.flush()
}

func (w *diskSorterWriter) flush() error {
	sw, err := w.newSSTWriter()
	if err != nil {
		return err
	}

	slices.SortFunc(w.kvs, func(a, b keyValue) int {
		return bytes.Compare(a.key, b.key)
	})

	// To dedup keys before write them into the SST file.
	// NOTE: keys should be sorted and deduped when construct one SST file.
	var lastKey []byte
	for _, kv := range w.kvs {
		if bytes.Equal(lastKey, kv.key) {
			continue
		}
		lastKey = kv.key
		if err := sw.Set(kv.key, kv.value); err != nil {
			_ = sw.Close()
			return err
		}
	}
	if err := sw.Close(); err != nil {
		return err
	}
	w.kvs = w.kvs[:0]
	w.off = 0
	return nil
}

func (w *diskSorterWriter) Close() error {
	return w.Flush()
}
