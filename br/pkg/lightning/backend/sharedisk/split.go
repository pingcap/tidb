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
)

type RangeSplitter struct {
	maxSize        uint64
	maxKeys        uint64
	maxWays        uint64
	propIter       *MergePropIter
	dataFileHandle FilePathHandle
	exhausted      bool

	activeDataFiles map[string]struct{}
	activeStatFiles map[string]struct{}
}

func NewRangeSplitter(maxSize, maxKeys, maxWays uint64, propIter *MergePropIter, data FilePathHandle) *RangeSplitter {
	return &RangeSplitter{
		maxSize:         maxSize,
		maxKeys:         maxKeys,
		maxWays:         maxWays,
		propIter:        propIter,
		dataFileHandle:  data,
		activeDataFiles: make(map[string]struct{}),
		activeStatFiles: make(map[string]struct{}),
	}
}

func (r *RangeSplitter) Close() error {
	return r.propIter.Close()
}

func (r *RangeSplitter) SplitOne() (kv.Key, []string, []string, error) {
	if r.exhausted {
		return nil, nil, nil, nil
	}

	var curSize, curKeys uint64
	var lastFilePath string
	var lastWays int
	var exhaustedFilePaths []string
	for r.propIter.Next() {
		if err := r.propIter.Error(); err != nil {
			return nil, nil, nil, err
		}
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curKeys += prop.rangeOffsets.Keys

		ways := r.propIter.propHeap.Len()
		if ways < lastWays {
			exhaustedFilePaths = append(exhaustedFilePaths, lastFilePath)
		}

		dataFilePath := r.dataFileHandle.Get(prop.WriterID, prop.DataSeq)
		r.activeDataFiles[dataFilePath] = struct{}{}
		fileIdx := r.propIter.currProp.fileOffset
		statFilePath := r.propIter.statFilePaths[fileIdx]
		r.activeStatFiles[statFilePath] = struct{}{}

		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(r.activeDataFiles)) >= r.maxWays {
			dataFiles, statsFiles := r.collectFiles()
			for _, p := range exhaustedFilePaths {
				delete(r.activeDataFiles, p)
			}
			return prop.Key, dataFiles, statsFiles, nil
		}

		lastFilePath = dataFilePath
		lastWays = ways
	}

	dataFiles, statsFiles := r.collectFiles()
	return nil, dataFiles, statsFiles, r.propIter.Error()
}

func (r *RangeSplitter) collectFiles() (data []string, stats []string) {
	dataFiles := make([]string, 0, len(r.activeDataFiles))
	for path := range r.activeDataFiles {
		dataFiles = append(dataFiles, path)
	}
	statsFiles := make([]string, 0, len(r.activeStatFiles))
	for path := range r.activeStatFiles {
		statsFiles = append(statsFiles, path)
	}
	return dataFiles, statsFiles
}
