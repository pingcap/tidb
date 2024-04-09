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
	"container/heap"
	"context"
	"io"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type heapElem interface {
	sortKey() []byte
	// cloneInnerFields should clone the fields of this struct to let all fields uses
	// owned memory. Sometimes to reduce allocation the memory is shared between
	// multiple elements and it's needed to call it before we free the shared memory.
	cloneInnerFields()
	len() int
}

type sortedReader[T heapElem] interface {
	path() string
	// next returns the next element in the reader. If there is no more element,
	// it returns io.EOF.
	next() (T, error)
	// When `need` is changed from false to true, the reader should prefetch more
	// data than usual when local cache is used up. It's used when one reader is more
	// frequently accessed than others and usual prefetching strategy is the
	// bottleneck.
	// When `need` is changed from true to false, the reader should
	// immediately release the memory for large prefetching to avoid OOM.
	// TODO(lance6716): learn more about external merge sort prefetch strategy.
	switchConcurrentMode(useConcurrent bool) error
	close() error
}

type mergeHeapElem[T heapElem] struct {
	elem      T
	readerIdx int
}

type mergeHeap[T heapElem] []mergeHeapElem[T]

func (h mergeHeap[T]) Len() int {
	return len(h)
}

func (h mergeHeap[T]) Less(i, j int) bool {
	return bytes.Compare(h[i].elem.sortKey(), h[j].elem.sortKey()) < 0
}

func (h mergeHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap[T]) Push(x any) {
	*h = append(*h, x.(mergeHeapElem[T]))
}

func (h *mergeHeap[T]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergeIter[T heapElem, R sortedReader[T]] struct {
	h             mergeHeap[T]
	readers       []*R
	curr          T // TODO(lance6716): why we don't use h[0] as curr?
	lastReaderIdx int
	err           error

	// determines whether to check reader hotspot, if hotspot is detected, we will
	// try read this file concurrently.
	checkHotspot       bool
	hotspotMap         map[int]int
	checkHotspotCnt    int
	checkHotspotPeriod int
	lastHotspotIdx     int
	elemFromHotspot    *T

	logger *zap.Logger
}

// readerOpenerFn is a function that opens a sorted reader.
type readerOpenerFn[T heapElem, R sortedReader[T]] func() (*R, error)

// openAndGetFirstElem opens readers in parallel and reads the first element.
func openAndGetFirstElem[
	T heapElem,
	R sortedReader[T],
](openers ...readerOpenerFn[T, R]) ([]*R, []T, error) {
	wg := errgroup.Group{}
	mayNilReaders := make([]*R, len(openers))
	closeReaders := func() {
		for _, rp := range mayNilReaders {
			if rp == nil {
				continue
			}
			r := *rp
			_ = r.close()
		}
	}

	for i, f := range openers {
		i := i
		f := f
		wg.Go(func() error {
			rd, err := f()
			switch err {
			case nil:
			case io.EOF:
				// will leave a nil reader in `mayNilReaders`
				return nil
			default:
				return err
			}
			mayNilReaders[i] = rd
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		closeReaders()
		return nil, nil, err
	}

	elements := make([]T, len(mayNilReaders))
	for j, rp := range mayNilReaders {
		if rp == nil {
			continue
		}
		rd := *rp
		e, err := rd.next()
		if err == io.EOF {
			_ = rd.close()
			mayNilReaders[j] = nil
			continue
		}
		if err != nil {
			closeReaders()
			return nil, nil, err
		}
		elements[j] = e
	}
	return mayNilReaders, elements, nil
}

// newMergeIter creates a merge iterator for multiple sorted reader opener
// functions. mergeIter.readers will have same order as input readerOpeners.
func newMergeIter[
	T heapElem,
	R sortedReader[T],
](
	ctx context.Context,
	readerOpeners []readerOpenerFn[T, R],
	checkHotspot bool,
) (*mergeIter[T, R], error) {
	if len(readerOpeners) == 0 {
		return nil, errors.New("no reader openers")
	}
	logger := logutil.Logger(ctx)

	readers, firstElements, err := openAndGetFirstElem(readerOpeners...)
	if err != nil {
		return nil, err
	}

	i := &mergeIter[T, R]{
		h:             make(mergeHeap[T], 0, len(readers)),
		readers:       readers,
		lastReaderIdx: -1,
		checkHotspot:  checkHotspot,
		hotspotMap:    make(map[int]int),
		logger:        logger,
	}
	sampleKeySize := 0
	sampleKeyCnt := 0
	for j, rp := range i.readers {
		// the reader has no content and is closed by openAndGetFirstElem
		if rp == nil {
			continue
		}
		e := firstElements[j]
		i.h = append(i.h, mergeHeapElem[T]{
			elem:      e,
			readerIdx: j,
		})
		sampleKeySize += e.len()
		sampleKeyCnt++
	}
	// We check the hotspot when the elements size is almost the same as the concurrent reader buffer size.
	// So that we don't drop too many bytes if the hotspot shifts to other files.
	if sampleKeySize == 0 || sampleKeySize/sampleKeyCnt == 0 {
		i.checkHotspotPeriod = 10000
	} else {
		sizeThreshold := int(32 * size.MB)
		i.checkHotspotPeriod = max(1000, sizeThreshold/(sampleKeySize/sampleKeyCnt))
	}
	heap.Init(&i.h)
	return i, nil
}

// close must be called for a mergeIter when it is no longer used.
func (i *mergeIter[T, R]) close() error {
	var firstErr error
	for idx, rdp := range i.readers {
		if rdp == nil {
			continue
		}
		rd := *rdp
		err := rd.close()
		if err != nil {
			i.logger.Warn("failed to close reader",
				zap.String("path", rd.path()),
				zap.Error(err))
			if firstErr == nil {
				firstErr = err
			}
		}
		i.readers[idx] = nil
	}
	return firstErr
}

// next forwards the iterator to the next element.
//
// ok == false if there is no available element, and if the iterator is
// exhausted, i.err will be nil instead of io.EOF. For other errors, i.err will
// be set.
//
// closeReaderIdx >= 0 means that reader is closed after last invocation, -1
// means no reader is closed.
func (i *mergeIter[T, R]) next() (closeReaderIdx int, ok bool) {
	closeReaderIdx = -1
	if i.lastReaderIdx >= 0 {
		if i.checkHotspot {
			i.hotspotMap[i.lastReaderIdx] = i.hotspotMap[i.lastReaderIdx] + 1
			i.checkHotspotCnt++

			// check hotspot every checkPeriod times
			if i.checkHotspotCnt == i.checkHotspotPeriod {
				oldHotspotIdx := i.lastHotspotIdx
				i.lastHotspotIdx = -1
				for idx, cnt := range i.hotspotMap {
					// currently only one reader will become hotspot
					if cnt > (i.checkHotspotPeriod / 2) {
						i.lastHotspotIdx = idx
						break
					}
				}
				// we are going to switch concurrent reader and free its memory. Clone
				// the fields to avoid use-after-free.
				if oldHotspotIdx != i.lastHotspotIdx {
					if i.elemFromHotspot != nil {
						(*i.elemFromHotspot).cloneInnerFields()
						i.elemFromHotspot = nil
					}
				}

				for idx, rp := range i.readers {
					if rp == nil {
						continue
					}
					isHotspot := i.lastHotspotIdx == idx
					err := (*rp).switchConcurrentMode(isHotspot)
					if err != nil {
						i.err = err
						return closeReaderIdx, false
					}
				}
				i.checkHotspotCnt = 0
				i.hotspotMap = make(map[int]int)
			}
		}

		rd := *i.readers[i.lastReaderIdx]
		e, err := rd.next()

		switch err {
		case nil:
			if i.checkHotspot && i.lastReaderIdx == i.lastHotspotIdx {
				i.elemFromHotspot = &e
			}
			heap.Push(&i.h, mergeHeapElem[T]{elem: e, readerIdx: i.lastReaderIdx})
		case io.EOF:
			closeErr := rd.close()
			if closeErr != nil {
				i.logger.Warn("failed to close reader",
					zap.String("path", rd.path()),
					zap.Error(closeErr))
			}
			i.readers[i.lastReaderIdx] = nil
			delete(i.hotspotMap, i.lastReaderIdx)
			closeReaderIdx = i.lastReaderIdx
		default:
			i.logger.Error("failed to read next element",
				zap.String("path", rd.path()),
				zap.Error(err))
			i.err = err
			return closeReaderIdx, false
		}
	}
	i.lastReaderIdx = -1

	if i.h.Len() == 0 {
		return closeReaderIdx, false
	}
	currMergeElem := heap.Pop(&i.h).(mergeHeapElem[T])
	i.curr = currMergeElem.elem
	i.lastReaderIdx = currMergeElem.readerIdx
	return closeReaderIdx, true
}

// limitSizeMergeIter acts like a mergeIter, except that each reader has a weight
// and it will try to open more readers with the total weight doesn't exceed the
// limit.
//
// Because it's like a mergeIter it's expected to iterate in ascending order,
// caller should set a proper limit to do not block opening new readers
// containing the next minimum element.
type limitSizeMergeIter[T heapElem, R sortedReader[T]] struct {
	*mergeIter[T, R]
	readerOpeners []readerOpenerFn[T, R]
	weights       []int64
	nextReaderIdx int
	weightSum     int64
	limit         int64
}

func newLimitSizeMergeIter[
	T heapElem,
	R sortedReader[T],
](
	ctx context.Context,
	readerOpeners []readerOpenerFn[T, R],
	weights []int64,
	limit int64,
) (*limitSizeMergeIter[T, R], error) {
	if limit <= 0 {
		return nil, errors.Errorf("limit must be positive, got %d", limit)
	}
	end := 0
	cur := int64(0)
	for ; end < len(weights); end++ {
		if cur+weights[end] > limit {
			break
		}
		cur += weights[end]
	}
	iter, err := newMergeIter(ctx, readerOpeners[:end], false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := &limitSizeMergeIter[T, R]{
		mergeIter:     iter,
		readerOpeners: readerOpeners,
		weights:       weights,
		weightSum:     cur,
		nextReaderIdx: end,
		limit:         limit,
	}
	// newMergeIter may close readers if the reader has no content, so we need to
	// fill more
	for i, rp := range iter.readers {
		if rp != nil {
			continue
		}
		ret.weightSum -= weights[i]
	}

	return ret, ret.tryOpenMoreReaders()
}

func (i *limitSizeMergeIter[T, R]) tryOpenMoreReaders() error {
	for i.nextReaderIdx < len(i.readerOpeners) {
		weight := i.weights[i.nextReaderIdx]
		if i.weightSum+weight > i.limit {
			return nil
		}

		opener := i.readerOpeners[i.nextReaderIdx]
		i.nextReaderIdx++
		newReaders, firstElements, err := openAndGetFirstElem(opener)
		if err != nil {
			return err
		}
		newReader := newReaders[0]
		newReaderIdx := len(i.mergeIter.readers)
		i.mergeIter.readers = append(i.mergeIter.readers, newReader)
		if newReader == nil {
			// maybe this reader has no content, just skip it
			continue
		}
		e := firstElements[0]
		i.mergeIter.h = append(i.mergeIter.h, mergeHeapElem[T]{
			elem:      e,
			readerIdx: newReaderIdx,
		})
		heap.Fix(&i.mergeIter.h, len(i.mergeIter.h)-1)
		i.weightSum += weight
	}
	return nil
}

func (i *limitSizeMergeIter[T, R]) next() (ok bool, closeReaderIdx int) {
	closeReaderIdx, ok = i.mergeIter.next()
	if closeReaderIdx == -1 {
		return
	}

	mergeIterDrained := !ok && i.mergeIter.h.Len() == 0

	// limitSizeMergeIter will try to open next reader when one reader is closed.
	i.weightSum -= i.weights[closeReaderIdx]
	if err := i.tryOpenMoreReaders(); err != nil {
		i.mergeIter.err = err
		return false, closeReaderIdx
	}

	// we need to call next once because mergeIter doesn't use h[0] as current value,
	// but a separate curr field
	if mergeIterDrained && i.mergeIter.h.Len() > 0 {
		_, ok = i.mergeIter.next()
	}
	return
}

// begin instantiations of mergeIter

type kvPair struct {
	key   []byte
	value []byte
}

func (p *kvPair) sortKey() []byte {
	return p.key
}

func (p *kvPair) cloneInnerFields() {
	p.key = append([]byte{}, p.key...)
	p.value = append([]byte{}, p.value...)
}

func (p *kvPair) len() int {
	return len(p.key) + len(p.value)
}

type kvReaderProxy struct {
	p string
	r *kvReader
}

func (p kvReaderProxy) path() string {
	return p.p
}

func (p kvReaderProxy) next() (*kvPair, error) {
	k, v, err := p.r.nextKV()
	if err != nil {
		return nil, err
	}
	return &kvPair{key: k, value: v}, nil
}

func (p kvReaderProxy) switchConcurrentMode(useConcurrent bool) error {
	return p.r.byteReader.switchConcurrentMode(useConcurrent)
}

func (p kvReaderProxy) close() error {
	return p.r.Close()
}

// MergeKVIter is an iterator that merges multiple sorted KV pairs from different files.
type MergeKVIter struct {
	iter    *mergeIter[*kvPair, kvReaderProxy]
	memPool *membuf.Pool
}

// NewMergeKVIter creates a new MergeKVIter. The KV can be accessed by calling
// Next() then Key() or Values(). readBufferSize is the buffer size for each file
// reader, which means the total memory usage is readBufferSize * len(paths).
func NewMergeKVIter(
	ctx context.Context,
	paths []string,
	pathsStartOffset []uint64,
	exStorage storage.ExternalStorage,
	readBufferSize int,
	checkHotspot bool,
	outerConcurrency int,
) (*MergeKVIter, error) {
	readerOpeners := make([]readerOpenerFn[*kvPair, kvReaderProxy], 0, len(paths))
	if outerConcurrency <= 0 {
		outerConcurrency = 1
	}
	concurrentReaderConcurrency := max(256/outerConcurrency, 8)
	// TODO: merge-sort step passes outerConcurrency=0, so this bufSize might be
	// too large when checkHotspot = true(add-index).
	largeBufSize := ConcurrentReaderBufferSizePerConc * concurrentReaderConcurrency
	memPool := membuf.NewPool(
		membuf.WithBlockNum(1), // currently only one reader will become hotspot
		membuf.WithBlockSize(largeBufSize),
	)

	for i := range paths {
		i := i
		readerOpeners = append(readerOpeners, func() (*kvReaderProxy, error) {
			rd, err := newKVReader(ctx, paths[i], exStorage, pathsStartOffset[i], readBufferSize)
			if err != nil {
				return nil, err
			}
			rd.byteReader.enableConcurrentRead(
				exStorage,
				paths[i],
				concurrentReaderConcurrency,
				ConcurrentReaderBufferSizePerConc,
				memPool.NewBuffer(),
			)
			return &kvReaderProxy{p: paths[i], r: rd}, nil
		})
	}

	it, err := newMergeIter[*kvPair, kvReaderProxy](ctx, readerOpeners, checkHotspot)
	return &MergeKVIter{iter: it, memPool: memPool}, err
}

// Error returns the error of the iterator.
func (i *MergeKVIter) Error() error {
	return i.iter.err
}

// Next moves the iterator to the next position. When it returns false, the iterator is not usable.
func (i *MergeKVIter) Next() bool {
	_, ok := i.iter.next()
	return ok
}

// Key returns the current key.
func (i *MergeKVIter) Key() []byte {
	return i.iter.curr.key
}

// Value returns the current value.
func (i *MergeKVIter) Value() []byte {
	return i.iter.curr.value
}

// Close closes the iterator.
func (i *MergeKVIter) Close() error {
	if err := i.iter.close(); err != nil {
		return err
	}
	// memPool should be destroyed after reader's buffer pool.
	i.memPool.Destroy()
	return nil
}

func (p *rangeProperty) sortKey() []byte {
	return p.firstKey
}

func (p *rangeProperty) cloneInnerFields() {
	p.firstKey = append([]byte{}, p.firstKey...)
	p.lastKey = append([]byte{}, p.lastKey...)
}

func (p *rangeProperty) len() int {
	// 24 is the length of member offset, size and keys, which are all uint64
	return len(p.firstKey) + len(p.lastKey) + 24
}

type statReaderProxy struct {
	p string
	r *statsReader
}

func (p statReaderProxy) path() string {
	return p.p
}

func (p statReaderProxy) next() (*rangeProperty, error) {
	return p.r.nextProp()
}

func (p statReaderProxy) switchConcurrentMode(bool) error { return nil }

func (p statReaderProxy) close() error {
	return p.r.Close()
}

// mergePropBaseIter handles one MultipleFilesStat and use limitSizeMergeIter to
// run heap sort on it. Also, it's a sortedReader of *rangeProperty that can be
// used by MergePropIter to handle multiple MultipleFilesStat.
type mergePropBaseIter struct {
	iter            *limitSizeMergeIter[*rangeProperty, statReaderProxy]
	closeReaderFlag *bool
	closeCh         chan struct{}
	wg              *sync.WaitGroup
}

type readerAndError struct {
	r   *statReaderProxy
	err error
}

var errMergePropBaseIterClosed = errors.New("mergePropBaseIter is closed")

func newMergePropBaseIter(
	ctx context.Context,
	multiStat MultipleFilesStat,
	exStorage storage.ExternalStorage,
) (*mergePropBaseIter, error) {
	var limit int64
	if multiStat.MaxOverlappingNum <= 0 {
		// make it an easy usage that caller don't need to set it
		limit = int64(len(multiStat.Filenames))
	} else {
		// we have no time to open the next reader before we get the next value for
		// limitSizeMergeIter, so we directly open one more reader. If we don't do this,
		// considering:
		//
		// [1, 11, ...
		// [2, 12, ...
		// [3, 13, ...
		//
		// we limit the size to 2, so after read 2, the next read will be 11 and then we
		// insert the third reader into heap. TODO: refine limitSizeMergeIter and mergeIter
		// to support this.
		limit = multiStat.MaxOverlappingNum + 1
	}
	limit = min(limit, int64(len(multiStat.Filenames)))

	// we are rely on the caller have reduced the overall overlapping to less than
	// MergeSortOverlapThreshold for []MultipleFilesStat. And we are going to open
	// about 8000 connection to read files.
	preOpenLimit := limit * 2
	preOpenLimit = min(preOpenLimit, int64(len(multiStat.Filenames)))
	preOpenCh := make(chan chan readerAndError, preOpenLimit-limit)
	closeCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer close(preOpenCh)
		defer wg.Done()
		// newLimitSizeMergeIter will open #limit readers at the beginning, and for rest
		// readers we open them in advance to reduce block when we need to open them.
		for i := int(limit); i < len(multiStat.Filenames); i++ {
			filePair := multiStat.Filenames[i]
			path := filePair[1]
			asyncTask := make(chan readerAndError, 1)
			wg.Add(1)
			go func() {
				defer close(asyncTask)
				defer wg.Done()
				rd, err := newStatsReader(ctx, exStorage, path, 250*1024)
				select {
				case <-closeCh:
					_ = rd.Close()
					return
				case asyncTask <- readerAndError{r: &statReaderProxy{p: path, r: rd}, err: err}:
				}
			}()
			select {
			case <-closeCh:
				// when close, no other methods is called simultaneously, so this goroutine can
				// check the size of channel and drain all
				for j := len(preOpenCh); j > 0; j-- {
					asyncTask2 := <-preOpenCh
					t, ok := <-asyncTask2
					if !ok {
						continue
					}
					if t.err == nil {
						_ = t.r.close()
					}
				}
				t, ok := <-asyncTask
				if ok && t.err == nil {
					_ = t.r.close()
				}
				return
			case preOpenCh <- asyncTask:
			}
		}
	}()

	readerOpeners := make([]readerOpenerFn[*rangeProperty, statReaderProxy], 0, len(multiStat.Filenames))
	// first `limit` reader will be opened by newLimitSizeMergeIter
	for i := 0; i < int(limit); i++ {
		path := multiStat.Filenames[i][1]
		readerOpeners = append(readerOpeners, func() (*statReaderProxy, error) {
			rd, err := newStatsReader(ctx, exStorage, path, 250*1024)
			if err != nil {
				return nil, err
			}
			return &statReaderProxy{p: path, r: rd}, nil
		})
	}
	// rest reader will be opened in above goroutine, just read them from channel
	for i := int(limit); i < len(multiStat.Filenames); i++ {
		readerOpeners = append(readerOpeners, func() (*statReaderProxy, error) {
			select {
			case <-closeCh:
				return nil, errMergePropBaseIterClosed
			case asyncTask, ok := <-preOpenCh:
				if !ok {
					return nil, errMergePropBaseIterClosed
				}
				select {
				case <-closeCh:
					return nil, errMergePropBaseIterClosed
				case t, ok := <-asyncTask:
					if !ok {
						return nil, errMergePropBaseIterClosed
					}
					return t.r, t.err
				}
			}
		})
	}
	weight := make([]int64, len(readerOpeners))
	for i := range weight {
		weight[i] = 1
	}
	i, err := newLimitSizeMergeIter(ctx, readerOpeners, weight, limit)
	return &mergePropBaseIter{iter: i, closeCh: closeCh, wg: wg}, err
}

func (m mergePropBaseIter) path() string {
	return "mergePropBaseIter"
}

func (m mergePropBaseIter) next() (*rangeProperty, error) {
	ok, closeReaderIdx := m.iter.next()
	if m.closeReaderFlag != nil && closeReaderIdx >= 0 {
		*m.closeReaderFlag = true
	}
	if !ok {
		if m.iter.err == nil {
			return nil, io.EOF
		}
		return nil, m.iter.err
	}
	return m.iter.curr, nil
}

func (m mergePropBaseIter) switchConcurrentMode(bool) error {
	return nil
}

// close should not be called concurrently with next.
func (m mergePropBaseIter) close() error {
	close(m.closeCh)
	m.wg.Wait()
	return m.iter.close()
}

// MergePropIter is an iterator that merges multiple range properties from different files.
type MergePropIter struct {
	iter                *limitSizeMergeIter[*rangeProperty, mergePropBaseIter]
	baseCloseReaderFlag *bool
}

// NewMergePropIter creates a new MergePropIter.
//
// Input MultipleFilesStat should be processed by functions like
// MergeOverlappingFiles to reduce overlapping to less than
// MergeSortOverlapThreshold. MergePropIter will only open needed
// MultipleFilesStat and its Filenames when iterates, and input MultipleFilesStat
// must guarantee its order and its Filename order can be process from left to
// right.
func NewMergePropIter(
	ctx context.Context,
	multiStat []MultipleFilesStat,
	exStorage storage.ExternalStorage,
) (*MergePropIter, error) {
	closeReaderFlag := false
	readerOpeners := make([]readerOpenerFn[*rangeProperty, mergePropBaseIter], 0, len(multiStat))
	for _, m := range multiStat {
		m := m
		readerOpeners = append(readerOpeners, func() (*mergePropBaseIter, error) {
			baseIter, err := newMergePropBaseIter(ctx, m, exStorage)
			if err != nil {
				return nil, err
			}
			baseIter.closeReaderFlag = &closeReaderFlag
			return baseIter, nil
		})
	}
	weight := make([]int64, len(multiStat))
	for i := range weight {
		weight[i] = multiStat[i].MaxOverlappingNum
	}

	// see the comment of newMergePropBaseIter why we need to raise the limit
	limit := MergeSortOverlapThreshold * 2

	it, err := newLimitSizeMergeIter(ctx, readerOpeners, weight, limit)
	return &MergePropIter{
		iter:                it,
		baseCloseReaderFlag: &closeReaderFlag,
	}, err
}

// Error returns the error of the iterator.
func (i *MergePropIter) Error() error {
	return i.iter.err
}

// Next moves the iterator to the next position.
func (i *MergePropIter) Next() bool {
	*i.baseCloseReaderFlag = false
	ok, _ := i.iter.next()
	return ok
}

func (i *MergePropIter) prop() *rangeProperty {
	return i.iter.curr
}

// readerIndex returns the indices of last accessed 2 level reader.
func (i *MergePropIter) readerIndex() (int, int) {
	idx := i.iter.lastReaderIdx
	return idx, i.iter.readers[idx].iter.lastReaderIdx
}

// Close closes the iterator.
func (i *MergePropIter) Close() error {
	return i.iter.close()
}
