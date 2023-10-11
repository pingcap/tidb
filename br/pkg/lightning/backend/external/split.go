// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"slices"

	"github.com/pingcap/tidb/br/pkg/storage"
)

type exhaustedHeapElem struct {
	key      []byte
	dataFile string
	statFile string
}

type exhaustedHeap []exhaustedHeapElem

func (h exhaustedHeap) Len() int {
	return len(h)
}

func (h exhaustedHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h exhaustedHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *exhaustedHeap) Push(x interface{}) {
	*h = append(*h, x.(exhaustedHeapElem))
}

func (h *exhaustedHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// RangeSplitter is used to split key ranges of an external engine. It will
// return one group of ranges by invoking `SplitOneRangesGroup` once.
type RangeSplitter struct {
	rangesGroupSize int64
	rangesGroupKeys int64
	rangeSize       int64
	rangeKeys       int64

	propIter  *MergePropIter
	dataFiles []string
	statFiles []string

	// filename -> index in dataFiles/statFiles
	activeDataFiles             map[string]int
	activeStatFiles             map[string]int
	curGroupSize                int64
	curGroupKeys                int64
	curRangeSize                int64
	curRangeKeys                int64
	recordSplitKeyAfterNextProp bool
	lastDataFile                string
	lastStatFile                string
	lastHeapSize                int
	lastRangeProperty           *rangeProperty
	willExhaustHeap             exhaustedHeap

	rangeSplitKeysBuf [][]byte
}

// NewRangeSplitter creates a new RangeSplitter.
// `dataFiles` and `statFiles` must be corresponding to each other.
// `rangesGroupSize` and `rangesGroupKeys` controls the total range group
// size of one `SplitOneRangesGroup` invocation, while `rangeSize` and
// `rangeKeys` controls the size of one range.
func NewRangeSplitter(
	ctx context.Context,
	dataFiles, statFiles []string,
	externalStorage storage.ExternalStorage,
	rangesGroupSize, rangesGroupKeys int64,
	maxRangeSize, maxRangeKeys int64,
) (*RangeSplitter, error) {
	propIter, err := NewMergePropIter(ctx, statFiles, externalStorage)
	if err != nil {
		return nil, err
	}

	return &RangeSplitter{
		rangesGroupSize: rangesGroupSize,
		rangesGroupKeys: rangesGroupKeys,
		propIter:        propIter,
		dataFiles:       dataFiles,
		statFiles:       statFiles,
		activeDataFiles: make(map[string]int),
		activeStatFiles: make(map[string]int),

		rangeSize:         maxRangeSize,
		rangeKeys:         maxRangeKeys,
		rangeSplitKeysBuf: make([][]byte, 0, 16),
	}, nil
}

// Close release the resources of RangeSplitter.
func (r *RangeSplitter) Close() error {
	return r.propIter.Close()
}

// GetRangeSplitSize returns the expected size of one range.
func (r *RangeSplitter) GetRangeSplitSize() int64 {
	return r.rangeSize
}

// SplitOneRangesGroup splits one group of ranges. `endKeyOfGroup` represents the
// end key of the group, but it will be nil when the group is the last one.
// `dataFiles` and `statFiles` are all the files that have overlapping key ranges
// in this group.
// `rangeSplitKeys` are the internal split keys of the ranges in this group.
func (r *RangeSplitter) SplitOneRangesGroup() (
	endKeyOfGroup []byte,
	dataFiles []string,
	statFiles []string,
	rangeSplitKeys [][]byte,
	err error,
) {
	var (
		exhaustedDataFiles, exhaustedStatFiles []string
		retDataFiles, retStatFiles             []string
		returnAfterNextProp                    = false
	)

	for r.propIter.Next() {
		if err = r.propIter.Error(); err != nil {
			return nil, nil, nil, nil, err
		}
		prop := r.propIter.prop()
		r.curGroupSize += int64(prop.size)
		r.curRangeSize += int64(prop.size)
		r.curGroupKeys += int64(prop.keys)
		r.curRangeKeys += int64(prop.keys)

		// a tricky way to detect source file will exhaust
		heapSize := r.propIter.iter.h.Len()
		if heapSize < r.lastHeapSize {
			heap.Push(&r.willExhaustHeap, exhaustedHeapElem{
				key:      r.lastRangeProperty.lastKey,
				dataFile: r.lastDataFile,
				statFile: r.lastStatFile,
			})
		}

		fileIdx := r.propIter.readerIndex()
		dataFilePath := r.dataFiles[fileIdx]
		statFilePath := r.statFiles[fileIdx]
		r.activeDataFiles[dataFilePath] = fileIdx
		r.activeStatFiles[statFilePath] = fileIdx
		r.lastDataFile = dataFilePath
		r.lastStatFile = statFilePath
		r.lastHeapSize = heapSize
		r.lastRangeProperty = prop

		for r.willExhaustHeap.Len() > 0 &&
			bytes.Compare(r.willExhaustHeap[0].key, prop.firstKey) < 0 {
			exhaustedDataFiles = append(exhaustedDataFiles, r.willExhaustHeap[0].dataFile)
			exhaustedStatFiles = append(exhaustedStatFiles, r.willExhaustHeap[0].statFile)
			heap.Pop(&r.willExhaustHeap)
		}

		if returnAfterNextProp {
			for _, p := range exhaustedDataFiles {
				delete(r.activeDataFiles, p)
			}
			exhaustedDataFiles = exhaustedDataFiles[:0]
			for _, p := range exhaustedStatFiles {
				delete(r.activeStatFiles, p)
			}
			exhaustedStatFiles = exhaustedStatFiles[:0]
			return prop.firstKey, retDataFiles, retStatFiles, r.takeSplitKeys(), nil
		}
		if r.recordSplitKeyAfterNextProp {
			r.rangeSplitKeysBuf = append(r.rangeSplitKeysBuf, slices.Clone(prop.firstKey))
			r.recordSplitKeyAfterNextProp = false
		}

		if r.curRangeSize >= r.rangeSize || r.curRangeKeys >= r.rangeKeys {
			r.curRangeSize = 0
			r.curRangeKeys = 0
			r.recordSplitKeyAfterNextProp = true
		}

		if r.curGroupSize >= r.rangesGroupSize || r.curGroupKeys >= r.rangesGroupKeys {
			retDataFiles, retStatFiles = r.cloneActiveFiles()

			r.curGroupSize = 0
			r.curGroupKeys = 0
			returnAfterNextProp = true
		}
	}

	retDataFiles, retStatFiles = r.cloneActiveFiles()
	r.activeDataFiles = make(map[string]int)
	r.activeStatFiles = make(map[string]int)
	return nil, retDataFiles, retStatFiles, r.takeSplitKeys(), r.propIter.Error()
}

func (r *RangeSplitter) cloneActiveFiles() (data []string, stat []string) {
	dataFiles := make([]string, 0, len(r.activeDataFiles))
	for path := range r.activeDataFiles {
		dataFiles = append(dataFiles, path)
	}
	slices.SortFunc(dataFiles, func(i, j string) int {
		return r.activeDataFiles[i] - r.activeDataFiles[j]
	})
	statFiles := make([]string, 0, len(r.activeStatFiles))
	for path := range r.activeStatFiles {
		statFiles = append(statFiles, path)
	}
	slices.SortFunc(statFiles, func(i, j string) int {
		return r.activeStatFiles[i] - r.activeStatFiles[j]
	})
	return dataFiles, statFiles
}

func (r *RangeSplitter) takeSplitKeys() [][]byte {
	ret := make([][]byte, len(r.rangeSplitKeysBuf))
	copy(ret, r.rangeSplitKeysBuf)
	r.rangeSplitKeysBuf = r.rangeSplitKeysBuf[:0]
	return ret
}
