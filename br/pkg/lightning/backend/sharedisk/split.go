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
	"github.com/pingcap/tidb/kv"
	"golang.org/x/exp/slices"
)

type RangeSplitter struct {
	maxSize        uint64
	maxKeys        uint64
	maxWays        uint64
	propIter       *MergePropIter
	dataFileHandle FilePathHandle
	exhausted      bool

	activeDataFiles map[string]int
	activeStatFiles map[string]int

	maxSplitRegionSize int64
	maxSplitRegionKey  int64

	regionSplitKeys [][]byte
}

func NewRangeSplitter(maxSize, maxKeys, maxWays uint64, propIter *MergePropIter, data FilePathHandle,
	maxSplitRegionSize, maxSplitRegionKey int64) *RangeSplitter {
	return &RangeSplitter{
		maxSize:         maxSize,
		maxKeys:         maxKeys,
		maxWays:         maxWays,
		propIter:        propIter,
		dataFileHandle:  data,
		activeDataFiles: make(map[string]int),
		activeStatFiles: make(map[string]int),

		maxSplitRegionSize: maxSplitRegionSize,
		maxSplitRegionKey:  maxSplitRegionKey,
		regionSplitKeys:    make([][]byte, 0, 16),
	}
}

func (r *RangeSplitter) Close() error {
	return r.propIter.Close()
}

func (r *RangeSplitter) SplitOne() (kv.Key, []string, []string, [][]byte, error) {
	if r.exhausted {
		return nil, nil, nil, nil, nil
	}

	var curSize, curKeys uint64
	var curRegionSize, curRegionKeys int64
	var lastFilePath string
	var lastWays int
	var exhaustedFilePaths []string
	for r.propIter.Next() {
		if err := r.propIter.Error(); err != nil {
			return nil, nil, nil, nil, err
		}
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curRegionSize += int64(prop.rangeOffsets.Size)
		curKeys += prop.rangeOffsets.Keys
		curRegionKeys += int64(prop.rangeOffsets.Keys)

		ways := r.propIter.propHeap.Len()
		if ways < lastWays {
			exhaustedFilePaths = append(exhaustedFilePaths, lastFilePath)
		}

		dataFilePath := r.dataFileHandle.Get(prop.WriterID, prop.DataSeq)
		fileIdx := r.propIter.currProp.fileOffset
		statFilePath := r.propIter.statFilePaths[fileIdx]
		r.activeDataFiles[dataFilePath] = fileIdx
		r.activeStatFiles[statFilePath] = fileIdx

		if curRegionSize >= r.maxSplitRegionSize || curRegionKeys >= r.maxSplitRegionKey {
			r.regionSplitKeys = append(r.regionSplitKeys, kv.Key(prop.Key).Clone())
			curRegionSize = 0
			curRegionKeys = 0
		}

		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(r.activeDataFiles)) >= r.maxWays {
			dataFiles, statsFiles := r.collectFiles()
			for _, p := range exhaustedFilePaths {
				delete(r.activeDataFiles, p)
			}
			splitKeys := r.collectSplitKeys()
			return prop.Key, dataFiles, statsFiles, splitKeys, nil
		}

		lastFilePath = dataFilePath
		lastWays = ways
	}

	dataFiles, statsFiles := r.collectFiles()
	splitKeys := r.collectSplitKeys()
	return nil, dataFiles, statsFiles, splitKeys, r.propIter.Error()
}

func (r *RangeSplitter) collectFiles() (data []string, stats []string) {
	dataFiles := make([]string, 0, len(r.activeDataFiles))
	for path := range r.activeDataFiles {
		dataFiles = append(dataFiles, path)
	}
	slices.SortFunc(dataFiles, func(i, j string) bool {
		return r.activeDataFiles[i] < r.activeDataFiles[j]
	})
	statsFiles := make([]string, 0, len(r.activeStatFiles))
	for path := range r.activeStatFiles {
		statsFiles = append(statsFiles, path)
	}
	slices.SortFunc(statsFiles, func(i, j string) bool {
		return r.activeStatFiles[i] < r.activeStatFiles[j]
	})
	return dataFiles, statsFiles
}

func (r *RangeSplitter) collectSplitKeys() [][]byte {
	ret := make([][]byte, 0, len(r.regionSplitKeys))
	for _, key := range r.regionSplitKeys {
		ret = append(ret, key)
	}
	r.regionSplitKeys = r.regionSplitKeys[:0]
	return ret
}
