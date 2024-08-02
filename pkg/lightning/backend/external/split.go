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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
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

func (h *exhaustedHeap) Push(x any) {
	*h = append(*h, x.(exhaustedHeapElem))
}

func (h *exhaustedHeap) Pop() any {
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

	propIter      *MergePropIter
	multiFileStat []MultipleFilesStat

	// filename -> 2 level index in dataFiles/statFiles
	activeDataFiles             map[string][2]int
	activeStatFiles             map[string][2]int
	curGroupSize                int64
	curGroupKeys                int64
	curRangeSize                int64
	curRangeKeys                int64
	recordSplitKeyAfterNextProp bool
	lastDataFile                string
	lastStatFile                string
	lastRangeProperty           *rangeProperty
	willExhaustHeap             exhaustedHeap

	rangeSplitKeysBuf [][]byte

	logger *zap.Logger
}

// NewRangeSplitter creates a new RangeSplitter.
// `rangesGroupSize` and `rangesGroupKeys` controls the total range group
// size of one `SplitOneRangesGroup` invocation, while `rangeSize` and
// `rangeKeys` controls the size of one range.
func NewRangeSplitter(
	ctx context.Context,
	multiFileStat []MultipleFilesStat,
	externalStorage storage.ExternalStorage,
	rangesGroupSize, rangesGroupKeys int64,
	maxRangeSize, maxRangeKeys int64,
) (*RangeSplitter, error) {
	logger := logutil.Logger(ctx)
	overlaps := make([]int64, 0, len(multiFileStat))
	fileNums := make([]int, 0, len(multiFileStat))
	for _, m := range multiFileStat {
		overlaps = append(overlaps, m.MaxOverlappingNum)
		fileNums = append(fileNums, len(m.Filenames))
	}
	logger.Info("create range splitter",
		zap.Int64s("overlaps", overlaps),
		zap.Ints("fileNums", fileNums),
		zap.Int64("rangesGroupSize", rangesGroupSize),
		zap.Int64("rangesGroupKeys", rangesGroupKeys),
		zap.Int64("maxRangeSize", maxRangeSize),
		zap.Int64("maxRangeKeys", maxRangeKeys),
	)
	propIter, err := NewMergePropIter(ctx, multiFileStat, externalStorage)
	if err != nil {
		return nil, err
	}

	return &RangeSplitter{
		rangesGroupSize: rangesGroupSize,
		rangesGroupKeys: rangesGroupKeys,
		propIter:        propIter,
		multiFileStat:   multiFileStat,
		activeDataFiles: make(map[string][2]int),
		activeStatFiles: make(map[string][2]int),

		rangeSize:         maxRangeSize,
		rangeKeys:         maxRangeKeys,
		rangeSplitKeysBuf: make([][]byte, 0, 16),

		logger: logger,
	}, nil
}

// Close release the resources of RangeSplitter.
func (r *RangeSplitter) Close() error {
	err := r.propIter.Close()
	r.logger.Info("close range splitter", zap.Error(err))
	return err
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

		// if this Next call will close the last reader
		if *r.propIter.baseCloseReaderFlag {
			heap.Push(&r.willExhaustHeap, exhaustedHeapElem{
				key:      r.lastRangeProperty.lastKey,
				dataFile: r.lastDataFile,
				statFile: r.lastStatFile,
			})
		}

		idx, idx2 := r.propIter.readerIndex()
		filePair := r.multiFileStat[idx].Filenames[idx2]
		dataFilePath := filePair[0]
		statFilePath := filePair[1]
		r.activeDataFiles[dataFilePath] = [2]int{idx, idx2}
		r.activeStatFiles[statFilePath] = [2]int{idx, idx2}
		r.lastDataFile = dataFilePath
		r.lastStatFile = statFilePath
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
	r.activeDataFiles = make(map[string][2]int)
	r.activeStatFiles = make(map[string][2]int)
	return nil, retDataFiles, retStatFiles, r.takeSplitKeys(), r.propIter.Error()
}

func (r *RangeSplitter) cloneActiveFiles() (data []string, stat []string) {
	dataFiles := make([]string, 0, len(r.activeDataFiles))
	for path := range r.activeDataFiles {
		dataFiles = append(dataFiles, path)
	}
	slices.SortFunc(dataFiles, func(i, j string) int {
		iInts := r.activeDataFiles[i]
		jInts := r.activeDataFiles[j]
		if iInts[0] != jInts[0] {
			return iInts[0] - jInts[0]
		}
		return iInts[1] - jInts[1]
	})
	statFiles := make([]string, 0, len(r.activeStatFiles))
	for path := range r.activeStatFiles {
		statFiles = append(statFiles, path)
	}
	slices.SortFunc(statFiles, func(i, j string) int {
		iInts := r.activeStatFiles[i]
		jInts := r.activeStatFiles[j]
		if iInts[0] != jInts[0] {
			return iInts[0] - jInts[0]
		}
		return iInts[1] - jInts[1]
	})
	return dataFiles, statFiles
}

func (r *RangeSplitter) takeSplitKeys() [][]byte {
	ret := make([][]byte, len(r.rangeSplitKeysBuf))
	copy(ret, r.rangeSplitKeysBuf)
	r.rangeSplitKeysBuf = r.rangeSplitKeysBuf[:0]
	return ret
}
