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
	"container/heap"
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type kvPair struct {
	key        []byte
	value      []byte
	fileOffset int
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

type MergeIter struct {
	startKey       []byte
	endKey         []byte
	dataFilePaths  []string
	dataFileReader []*kvReader
	exStorage      storage.ExternalStorage
	kvHeap         kvPairHeap
	currKV         *kvPair
	lastFileOffset int

	firstKey []byte

	err error
}

func NewMergeIter(ctx context.Context, paths []string, pathsStartOffset []uint64, exStorage storage.ExternalStorage, readBufferSize int) (*MergeIter, error) {
	it := &MergeIter{
		dataFilePaths:  paths,
		lastFileOffset: -1,
	}
	it.dataFileReader = make([]*kvReader, len(paths))
	it.kvHeap = make([]*kvPair, 0, len(paths))

	// Open all data files concurrently.
	var wg sync.WaitGroup
	wg.Add(len(paths))
	var perr atomic.Pointer[error]
	for i, path := range paths {
		i := i
		path := path
		go func() {
			rd, err := newKVReader(ctx, path, exStorage, pathsStartOffset[i], readBufferSize)
			if err != nil {
				perr.CompareAndSwap(nil, &err)
			}
			it.dataFileReader[i] = rd
			wg.Done()
		}()
	}
	wg.Wait()
	if perr.Load() != nil {
		return nil, *perr.Load()
	}

	for i, rd := range it.dataFileReader {
		k, v, err := rd.nextKV()
		if err != nil {
			return nil, err
		}
		if len(k) == 0 {
			continue
		}
		pair := kvPair{key: k, value: v, fileOffset: i}
		it.kvHeap.Push(&pair)
	}
	heap.Init(&it.kvHeap)
	return it, nil
}

func (i *MergeIter) Seek(key []byte) bool {
	// Don't support.
	return false
}

func (i *MergeIter) Error() error {
	return i.err
}

func (i *MergeIter) First() bool {
	// Don't support.
	return false
}

func (i *MergeIter) Last() bool {
	// Don't support.
	return false
}

func (i *MergeIter) Valid() bool {
	return i.currKV != nil
}

func (i *MergeIter) Next() bool {
	// Populate the heap.
	if i.lastFileOffset >= 0 {
		k, v, err := i.dataFileReader[i.lastFileOffset].nextKV()
		if err != nil {
			i.err = err
			return false
		}
		if len(k) > 0 {
			heap.Push(&i.kvHeap, &kvPair{k, v, i.lastFileOffset})
		}
	}
	if i.kvHeap.Len() == 0 {
		return false
	}
	i.currKV = heap.Pop(&i.kvHeap).(*kvPair)
	i.lastFileOffset = i.currKV.fileOffset
	return true
}

func (i *MergeIter) Key() []byte {
	return i.currKV.key
}

func (i *MergeIter) Value() []byte {
	return i.currKV.value
}

func (i *MergeIter) Close() error {
	for _, rd := range i.dataFileReader {
		if err := rd.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (i *MergeIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}

type prop struct {
	p          RangeProperty
	fileOffset int
}

type propHeap []*prop

func (h propHeap) Len() int {
	return len(h)
}

func (h propHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].p.Key, h[j].p.Key) < 0
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
		buf.WriteString(fmt.Sprintf("%s ", hex.EncodeToString(p.p.Key)))
	}
	return buf.String()
}

type MergePropIter struct {
	startKey       []byte
	endKey         []byte
	statFilePaths  []string
	statFileReader []*statsReader
	propHeap       propHeap
	currProp       *prop

	firstKey    []byte
	lastFileIdx int

	err error
}

func NewMergePropIter(ctx context.Context, paths []string, exStorage storage.ExternalStorage) (*MergePropIter, error) {
	it := &MergePropIter{
		statFilePaths: paths,
		lastFileIdx:   -1,
	}
	it.propHeap = make([]*prop, 0, len(paths))
	it.statFileReader = make([]*statsReader, len(paths))

	// Open all stat files concurrently.
	var wg sync.WaitGroup
	wg.Add(len(paths))
	var perr atomic.Pointer[error]
	for i, path := range paths {
		i := i
		path := path
		go func() {
			rd, err := newStatsReader(ctx, exStorage, path, 4096)
			if err != nil {
				perr.CompareAndSwap(nil, &err)
			}
			it.statFileReader[i] = rd
			wg.Done()
		}()
	}
	wg.Wait()
	if perr.Load() != nil {
		return nil, *perr.Load()
	}

	for i, rd := range it.statFileReader {
		p, err := rd.nextProp()
		if err != nil {
			return nil, err
		}
		if p == nil {
			continue
		}
		pair := prop{p: *p, fileOffset: i}
		it.propHeap.Push(&pair)
	}
	heap.Init(&it.propHeap)
	return it, nil
}

func SeekPropsOffsets(ctx context.Context, start kv.Key, paths []string, exStorage storage.ExternalStorage) ([]uint64, error) {
	iter, err := NewMergePropIter(ctx, paths, exStorage)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := iter.Close(); err != nil {
			logutil.BgLogger().Warn("failed to close merge prop iterator", zap.Error(err))
		}
	}()
	var (
		lastOffset  uint64
		lastFileIdx int
	)
	offsets := make([]uint64, len(paths))
	for iter.Next() {
		if iter.Error() != nil {
			return nil, iter.Error()
		}
		p := iter.Prop()
		offsets[iter.currProp.fileOffset] = iter.currProp.p.offset
		propKey := kv.Key(p.Key)
		if propKey.Cmp(start) > 0 {
			offsets[lastFileIdx] = lastOffset
			return offsets, nil
		}
		lastFileIdx = iter.currProp.fileOffset
		lastOffset = iter.currProp.p.offset
	}
	return offsets, nil
}

func (i *MergePropIter) Error() error {
	return i.err
}

func (i *MergePropIter) Valid() bool {
	return i.currProp != nil
}

func (i *MergePropIter) Next() bool {
	if i.lastFileIdx >= 0 {
		p, err := i.statFileReader[i.lastFileIdx].nextProp()
		if err != nil {
			i.err = err
			return false
		}
		if p != nil {
			heap.Push(&i.propHeap, &prop{*p, i.lastFileIdx})
		}
	}
	if i.propHeap.Len() == 0 {
		return false
	}
	i.currProp = heap.Pop(&i.propHeap).(*prop)
	i.lastFileIdx = i.currProp.fileOffset
	return true
}

func (i *MergePropIter) Prop() *RangeProperty {
	return &i.currProp.p
}

func (i *MergePropIter) Close() error {
	for _, rd := range i.statFileReader {
		if err := rd.Close(); err != nil {
			return err
		}
	}
	return nil
}
