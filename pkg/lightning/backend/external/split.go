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

// RangeSplitter is used to split key ranges of an external engine. Please see
// NewRangeSplitter and SplitOneRangesGroup for more details.
type RangeSplitter struct {
	rangesGroupSize int64
	rangesGroupKeys int64

	rangeJobSize   int64
	rangeJobKeyCnt int64
	rangeJobKeys   [][]byte

	regionSplitSize   int64
	regionSplitKeyCnt int64
	regionSplitKeys   [][]byte

	propIter      *MergePropIter
	multiFileStat []MultipleFilesStat

	// filename -> 2 level index in dataFiles/statFiles
	activeDataFiles                map[string][2]int
	activeStatFiles                map[string][2]int
	curGroupSize                   int64
	curGroupKeyCnt                 int64
	curRangeJobSize                int64
	curRangeJobKeyCnt              int64
	recordRangeJobAfterNextProp    bool
	curRegionSplitSize             int64
	curRegionSplitKeyCnt           int64
	recordRegionSplitAfterNextProp bool
	lastDataFile                   string
	lastStatFile                   string
	lastRangeProperty              *rangeProperty
	willExhaustHeap                exhaustedHeap

	logger *zap.Logger
}

// NewRangeSplitter creates a new RangeSplitter to process the stat files of
// `multiFileStat` stored in `externalStorage`.
//
// `rangesGroupSize` and `rangesGroupKeyCnt` controls the total size and key
// count limit of the ranges group returned by one `SplitOneRangesGroup`
// invocation. The ranges group may contain multiple range jobs and region split
// keys. The size and keys limit of one range job are controlled by
// `rangeJobSize` and `rangeJobKeyCnt`. The size and keys limit of intervals of
// region split keys are controlled by `regionSplitSize` and `regionSplitKeyCnt`.
func NewRangeSplitter(
	ctx context.Context,
	multiFileStat []MultipleFilesStat,
	externalStorage storage.ExternalStorage,
	rangesGroupSize, rangesGroupKeyCnt int64,
	rangeJobSize, rangeJobKeyCnt int64,
	regionSplitSize, regionSplitKeyCnt int64,
) (*RangeSplitter, error) {
	logger := logutil.Logger(ctx)
	logger.Info("create range splitter",
		zap.Int64("rangesGroupSize", rangesGroupSize),
		zap.Int64("rangesGroupKeyCnt", rangesGroupKeyCnt),
		zap.Int64("rangeJobSize", rangeJobSize),
		zap.Int64("rangeJobKeyCnt", rangeJobKeyCnt),
		zap.Int64("regionSplitSize", regionSplitSize),
		zap.Int64("regionSplitKeyCnt", regionSplitKeyCnt),
	)
	propIter, err := NewMergePropIter(ctx, multiFileStat, externalStorage)
	if err != nil {
		return nil, err
	}

	return &RangeSplitter{
		rangesGroupSize: rangesGroupSize,
		rangesGroupKeys: rangesGroupKeyCnt,
		propIter:        propIter,
		multiFileStat:   multiFileStat,
		activeDataFiles: make(map[string][2]int),
		activeStatFiles: make(map[string][2]int),

		rangeJobSize:   rangeJobSize,
		rangeJobKeyCnt: rangeJobKeyCnt,
		rangeJobKeys:   make([][]byte, 0, 16),

		regionSplitSize:   regionSplitSize,
		regionSplitKeyCnt: regionSplitKeyCnt,
		regionSplitKeys:   make([][]byte, 0, 16),

		logger: logger,
	}, nil
}

// Close release the resources of RangeSplitter.
func (r *RangeSplitter) Close() error {
	err := r.propIter.Close()
	r.logger.Info("close range splitter", zap.Error(err))
	return err
}

// SplitOneRangesGroup splits one ranges group may contain multiple range jobs
// and region split keys. `endKeyOfGroup` represents the end key of the group,
// but it will be nil when the group is the last one. `dataFiles` and `statFiles`
// are all the files that have overlapping key ranges in this group.
// `interiorRangeJobKeys` are the interior boundary keys of the range jobs, the
// range can be constructed with start/end key at caller.
// `interiorRegionSplitKeys` are the split keys that will be used later to split
// regions.
func (r *RangeSplitter) SplitOneRangesGroup() (
	endKeyOfGroup []byte,
	dataFiles []string,
	statFiles []string,
	interiorRangeJobKeys [][]byte,
	interiorRegionSplitKeys [][]byte,
	err error,
) {
	var (
		exhaustedDataFiles, exhaustedStatFiles []string
		retDataFiles, retStatFiles             []string
		returnAfterNextProp                    = false
	)

	for r.propIter.Next() {
		if err = r.propIter.Error(); err != nil {
			return nil, nil, nil, nil, nil, err
		}
		prop := r.propIter.prop()
		r.curGroupSize += int64(prop.size)
		r.curRangeJobSize += int64(prop.size)
		r.curRegionSplitSize += int64(prop.size)
		r.curGroupKeyCnt += int64(prop.keys)
		r.curRangeJobKeyCnt += int64(prop.keys)
		r.curRegionSplitKeyCnt += int64(prop.keys)

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
			return prop.firstKey, retDataFiles, retStatFiles, r.takeRangeJobKeys(), r.takeRegionSplitKeys(), nil
		}
		if r.recordRangeJobAfterNextProp {
			r.rangeJobKeys = append(r.rangeJobKeys, slices.Clone(prop.firstKey))
			r.recordRangeJobAfterNextProp = false
		}
		if r.recordRegionSplitAfterNextProp {
			r.regionSplitKeys = append(r.regionSplitKeys, slices.Clone(prop.firstKey))
			r.recordRegionSplitAfterNextProp = false
		}

		if r.curRangeJobSize >= r.rangeJobSize || r.curRangeJobKeyCnt >= r.rangeJobKeyCnt {
			r.curRangeJobSize = 0
			r.curRangeJobKeyCnt = 0
			r.recordRangeJobAfterNextProp = true
		}

		if r.curRegionSplitSize >= r.regionSplitSize || r.curRegionSplitKeyCnt >= r.regionSplitKeyCnt {
			r.curRegionSplitSize = 0
			r.curRegionSplitKeyCnt = 0
			r.recordRegionSplitAfterNextProp = true
		}

		if r.curGroupSize >= r.rangesGroupSize || r.curGroupKeyCnt >= r.rangesGroupKeys {
			retDataFiles, retStatFiles = r.cloneActiveFiles()

			r.curGroupSize = 0
			r.curGroupKeyCnt = 0
			returnAfterNextProp = true
		}
	}

	retDataFiles, retStatFiles = r.cloneActiveFiles()
	r.activeDataFiles = make(map[string][2]int)
	r.activeStatFiles = make(map[string][2]int)
	return nil, retDataFiles, retStatFiles, r.takeRangeJobKeys(), r.takeRegionSplitKeys(), r.propIter.Error()
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

func (r *RangeSplitter) takeRangeJobKeys() [][]byte {
	ret := make([][]byte, len(r.rangeJobKeys))
	copy(ret, r.rangeJobKeys)
	r.rangeJobKeys = r.rangeJobKeys[:0]
	return ret
}

func (r *RangeSplitter) takeRegionSplitKeys() [][]byte {
	ret := make([][]byte, len(r.regionSplitKeys))
	copy(ret, r.regionSplitKeys)
	r.regionSplitKeys = r.regionSplitKeys[:0]
	return ret
}
