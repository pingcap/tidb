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
	"encoding/hex"
	"fmt"
	"io"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type kvPair struct {
	key       []byte
	value     []byte
	fileIndex int
}

type kvPairHeap []*kvPair

func (h kvPairHeap) Len() int {
	return len(h)
}

func (h kvPairHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h kvPairHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *kvPairHeap) Push(x interface{}) {
	*h = append(*h, x.(*kvPair))
}

func (h *kvPairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// MergeIter is an iterator that merges multiple sorted KV pairs from different files.
type MergeIter struct {
	startKey       []byte
	endKey         []byte
	dataFilePaths  []string
	dataFileReader []*kvReader
	exStorage      storage.ExternalStorage
	kvHeap         kvPairHeap
	currKV         *kvPair
	lastFileIndex  int

	firstKey []byte

	err    error
	logger *zap.Logger
}

// NewMergeIter creates a new MergeIter.
// readBufferSize is the buffer size for each file reader, which means the total memory usage is
// readBufferSize * len(paths).
func NewMergeIter(
	ctx context.Context,
	paths []string,
	pathsStartOffset []uint64,
	exStorage storage.ExternalStorage,
	readBufferSize int,
) (*MergeIter, error) {
	logger := logutil.Logger(ctx)
	it := &MergeIter{
		dataFilePaths: paths,
		lastFileIndex: -1,
		logger:        logger,
	}
	it.dataFileReader = make([]*kvReader, len(paths))
	it.kvHeap = make([]*kvPair, 0, len(paths))

	// Open all data files concurrently.
	wg, wgCtx := errgroup.WithContext(ctx)
	for i := range paths {
		i := i
		wg.Go(func() error {
			rd, err := newKVReader(wgCtx, paths[i], exStorage, pathsStartOffset[i], readBufferSize)
			if err != nil {
				return err
			}
			it.dataFileReader[i] = rd
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}

	for i, rd := range it.dataFileReader {
		k, v, err := rd.nextKV()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if len(k) == 0 {
			closeErr := rd.Close()
			if closeErr != nil {
				logger.Warn(
					"failed to close file",
					zap.String("path", paths[i]),
					zap.Error(closeErr),
				)
			}
			continue
		}
		pair := kvPair{key: k, value: v, fileIndex: i}
		it.kvHeap.Push(&pair)
	}
	heap.Init(&it.kvHeap)
	return it, nil
}

// Error returns the error of the iterator.
func (i *MergeIter) Error() error {
	return i.err
}

// Valid returns whether the iterator is valid to be iterated.
func (i *MergeIter) Valid() bool {
	return i.err == nil && i.kvHeap.Len() > 0
}

// Next moves the iterator to the next position.
func (i *MergeIter) Next() bool {
	i.currKV = nil
	// Populate the heap.
	if i.lastFileIndex >= 0 {
		k, v, err := i.dataFileReader[i.lastFileIndex].nextKV()
		if err != nil && err != io.EOF {
			i.err = err
			return false
		}
		if len(k) > 0 {
			heap.Push(&i.kvHeap, &kvPair{k, v, i.lastFileIndex})
		} else {
			closeErr := i.dataFileReader[i.lastFileIndex].Close()
			if closeErr != nil {
				i.logger.Warn(
					"failed to close file",
					zap.String("path", i.dataFilePaths[i.lastFileIndex]),
					zap.Error(closeErr),
				)
			}
		}
	}
	i.lastFileIndex = -1

	if i.kvHeap.Len() == 0 {
		return false
	}
	i.currKV = heap.Pop(&i.kvHeap).(*kvPair)
	i.lastFileIndex = i.currKV.fileIndex
	return true
}

// Key returns the current key.
func (i *MergeIter) Key() []byte {
	return i.currKV.key
}

// Value returns the current value.
func (i *MergeIter) Value() []byte {
	return i.currKV.value
}

// Close closes the iterator.
func (i *MergeIter) Close() error {
	var firstErr error
	if i.lastFileIndex >= 0 {
		firstErr = i.dataFileReader[i.lastFileIndex].Close()
	}
	for _, p := range i.kvHeap {
		err := i.dataFileReader[p.fileIndex].Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

type prop struct {
	p         rangeProperty
	fileIndex int
}

type propHeap []*prop

func (h propHeap) Len() int {
	return len(h)
}

func (h propHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].p.key, h[j].p.key) < 0
}

func (h propHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *propHeap) Push(x interface{}) {
	*h = append(*h, x.(*prop))
}

func (h *propHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *propHeap) Print() string {
	var buf bytes.Buffer
	for _, p := range *h {
		fmt.Fprintf(&buf, "%s ", hex.EncodeToString(p.p.key))
	}
	return buf.String()
}

// MergePropIter is an iterator that merges multiple range properties from different files.
type MergePropIter struct {
	startKey       []byte
	endKey         []byte
	statFilePaths  []string
	statFileReader []*statsReader
	propHeap       propHeap
	currProp       *prop

	firstKey      []byte
	lastFileIndex int

	err    error
	logger *zap.Logger
}

// NewMergePropIter creates a new MergePropIter.
func NewMergePropIter(
	ctx context.Context,
	paths []string,
	exStorage storage.ExternalStorage,
) (*MergePropIter, error) {
	logger := logutil.Logger(ctx)
	it := &MergePropIter{
		statFilePaths: paths,
		lastFileIndex: -1,
		logger:        logger,
	}
	it.propHeap = make([]*prop, 0, len(paths))
	it.statFileReader = make([]*statsReader, len(paths))

	// Open all stat files concurrently.
	wg, wgCtx := errgroup.WithContext(ctx)
	for i := range paths {
		i := i
		wg.Go(func() error {
			rd, err := newStatsReader(wgCtx, exStorage, paths[i], 4096)
			if err != nil {
				return err
			}
			it.statFileReader[i] = rd
			return nil
		})
	}

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	for i, rd := range it.statFileReader {
		p, err := rd.nextProp()
		if err != nil && err != io.EOF {
			return nil, err
		}
		if p == nil {
			closeErr := rd.Close()
			if closeErr != nil {
				logger.Warn(
					"failed to close file",
					zap.String("path", paths[i]),
					zap.Error(closeErr),
				)
			}
			continue
		}
		pair := prop{p: *p, fileIndex: i}
		it.propHeap.Push(&pair)
	}
	heap.Init(&it.propHeap)
	return it, nil
}

// SeekPropsOffsets seeks the offsets of the range properties that are greater than the start key.
func SeekPropsOffsets(
	ctx context.Context,
	start kv.Key,
	paths []string,
	exStorage storage.ExternalStorage,
) ([]uint64, error) {
	iter, err := NewMergePropIter(ctx, paths, exStorage)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := iter.Close(); err != nil {
			logutil.BgLogger().Warn("failed to close merge prop iterator", zap.Error(err))
		}
	}()
	offsets := make([]uint64, len(paths))
	for iter.Next() {
		if iter.Error() != nil {
			return nil, iter.Error()
		}
		p := iter.prop()
		propKey := kv.Key(p.key)
		if propKey.Cmp(start) > 0 {
			return offsets, nil
		}
		offsets[iter.currProp.fileIndex] = iter.currProp.p.offset
	}
	return offsets, nil
}

// Error returns the error of the iterator.
func (i *MergePropIter) Error() error {
	return i.err
}

// Valid returns whether the iterator is valid to be iterated.
func (i *MergePropIter) Valid() bool {
	return i.err == nil && i.propHeap.Len() > 0
}

// Next moves the iterator to the next position.
func (i *MergePropIter) Next() bool {
	i.currProp = nil
	if i.lastFileIndex >= 0 {
		p, err := i.statFileReader[i.lastFileIndex].nextProp()
		if err != nil && err != io.EOF {
			i.err = err
			return false
		}
		if p != nil {
			heap.Push(&i.propHeap, &prop{*p, i.lastFileIndex})
		} else {
			closeErr := i.statFileReader[i.lastFileIndex].Close()
			if closeErr != nil {
				i.logger.Warn(
					"failed to close file",
					zap.String("path", i.statFilePaths[i.lastFileIndex]),
					zap.Error(closeErr),
				)
			}
		}
	}
	i.lastFileIndex = -1

	if i.propHeap.Len() == 0 {
		return false
	}
	i.currProp = heap.Pop(&i.propHeap).(*prop)
	i.lastFileIndex = i.currProp.fileIndex
	return true
}

func (i *MergePropIter) prop() *rangeProperty {
	return &i.currProp.p
}

// Close closes the iterator.
func (i *MergePropIter) Close() error {
	var firstErr error
	if i.lastFileIndex >= 0 {
		firstErr = i.statFileReader[i.lastFileIndex].Close()
	}
	for _, p := range i.propHeap {
		err := i.statFileReader[p.fileIndex].Close()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
