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
	"io"
	"sort"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

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
	exStorage storeapi.Storage,
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
	// maxMergeSortOverlapThreshold for []MultipleFilesStat. And we are going to open
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
	for i := range int(limit) {
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
// maxMergeSortOverlapThreshold. MergePropIter will only open needed
// MultipleFilesStat and its Filenames when iterates, and input MultipleFilesStat
// must guarantee its order and its Filename order can be process from left to
// right.
func NewMergePropIter(
	ctx context.Context,
	multiStat []MultipleFilesStat,
	exStorage storeapi.Storage,
) (*MergePropIter, error) {
	// sort the multiStat by minKey
	// otherwise, if the number of readers is less than the weight, the kv may not in order
	sort.Slice(multiStat, func(i, j int) bool {
		return bytes.Compare(multiStat[i].MinKey, multiStat[j].MinKey) < 0
	})

	closeReaderFlag := false
	readerOpeners := make([]readerOpenerFn[*rangeProperty, mergePropBaseIter], 0, len(multiStat))
	for _, m := range multiStat {
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
	limit := maxMergeSortOverlapThreshold * 2

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
