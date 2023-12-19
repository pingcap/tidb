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

func (h *mergeHeap[T]) Push(x interface{}) {
	*h = append(*h, x.(mergeHeapElem[T]))
}

func (h *mergeHeap[T]) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergeIter[T heapElem, R sortedReader[T]] struct {
	h             mergeHeap[T]
	readers       []*R
	curr          T
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

// newMergeIter creates a merge iterator for multiple sorted reader opener
// functions.
func newMergeIter[
	T heapElem,
	R sortedReader[T],
](ctx context.Context, readerOpeners []readerOpenerFn[T, R], checkHotspot bool) (*mergeIter[T, R], error) {
	logger := logutil.Logger(ctx)
	readers := make([]*R, len(readerOpeners))
	closeReaders := func() {
		for _, rp := range readers {
			if rp == nil {
				continue
			}
			r := *rp
			err := r.close()
			if err != nil {
				logger.Warn("failed to close reader",
					zap.String("path", r.path()),
					zap.Error(err))
			}
		}
	}

	// Open readers in parallel.
	wg := errgroup.Group{}
	for i, f := range readerOpeners {
		i := i
		f := f
		wg.Go(func() error {
			rd, err := f()
			switch err {
			case nil:
			case io.EOF:
				// will leave a nil reader in `readers` when reader is empty
				return nil
			default:
				return err
			}
			readers[i] = rd
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		closeReaders()
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
	for j := range i.readers {
		if i.readers[j] == nil {
			continue
		}
		rd := *i.readers[j]
		e, err := rd.next()
		if err == io.EOF {
			closeErr := rd.close()
			if closeErr != nil {
				i.logger.Warn("failed to close reader",
					zap.String("path", rd.path()),
					zap.Error(closeErr))
			}
			i.readers[j] = nil
			continue
		}
		if err != nil {
			closeErr := i.close()
			if closeErr != nil {
				i.logger.Warn("failed to close merge iterator",
					zap.Error(closeErr))
			}
			return nil, err
		}
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

func (i *mergeIter[T, R]) currElem() T {
	return i.curr
}

// next forwards the iterator to the next element. It returns false if there is
// no available element.
func (i *mergeIter[T, R]) next() bool {
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
						return false
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
		default:
			i.logger.Error("failed to read next element",
				zap.String("path", rd.path()),
				zap.Error(err))
			i.err = err
			return false
		}
	}
	i.lastReaderIdx = -1

	if i.h.Len() == 0 {
		return false
	}
	currMergeElem := heap.Pop(&i.h).(mergeHeapElem[T])
	i.curr = currMergeElem.elem
	i.lastReaderIdx = currMergeElem.readerIdx
	return true
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
	return i.iter.next()
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

// MergePropIter is an iterator that merges multiple range properties from different files.
type MergePropIter struct {
	iter *mergeIter[*rangeProperty, statReaderProxy]
}

// NewMergePropIter creates a new MergePropIter.
func NewMergePropIter(
	ctx context.Context,
	paths []string,
	exStorage storage.ExternalStorage,
	checkHotSpot bool,
) (*MergePropIter, error) {
	readerOpeners := make([]readerOpenerFn[*rangeProperty, statReaderProxy], 0, len(paths))
	for i := range paths {
		i := i
		readerOpeners = append(readerOpeners, func() (*statReaderProxy, error) {
			rd, err := newStatsReader(ctx, exStorage, paths[i], 4096)
			if err != nil {
				return nil, err
			}
			return &statReaderProxy{p: paths[i], r: rd}, nil
		})
	}

	it, err := newMergeIter[*rangeProperty, statReaderProxy](ctx, readerOpeners, checkHotSpot)
	return &MergePropIter{iter: it}, err
}

// Error returns the error of the iterator.
func (i *MergePropIter) Error() error {
	return i.iter.err
}

// Next moves the iterator to the next position.
func (i *MergePropIter) Next() bool {
	return i.iter.next()
}

func (i *MergePropIter) prop() *rangeProperty {
	return i.iter.curr
}

func (i *MergePropIter) readerIndex() int {
	return i.iter.lastReaderIdx
}

// Close closes the iterator.
func (i *MergePropIter) Close() error {
	return i.iter.close()
}
