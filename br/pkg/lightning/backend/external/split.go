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
	"context"
	"slices"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/kv"
)

// RangeSplitter is used to split key ranges of an external engine. It will
// return one group of ranges by invoking `SplitOneRangesGroup` once.
type RangeSplitter struct {
	rangesGroupSize int64
	rangesGroupKeys int64
	maxWays         int
	rangeSize       int64
	rangeKeys       int64

	propIter  *MergePropIter
	dataFiles []string
	statFiles []string

	activeDataFiles map[string]int
	activeStatFiles map[string]int

	rangeSplitKeysBuf [][]byte
}

// NewRangeSplitter creates a new RangeSplitter.
// `dataFiles` and `statFiles` must be corresponding to each other.
// `rangesGroupSize` and `rangesGroupKeys` controls the total range group
// size of one `SplitOneRangesGroup` invocation, while `rangeSize` and
// `rangeKeys` controls the size of one range.
// `maxWays` controls the merge heap size limit. When the actual heap size drops
// below `maxWays`, the `SplitOneRangesGroup` will return as the last group.
func NewRangeSplitter(
	ctx context.Context,
	dataFiles, statFiles []string,
	externalStorage storage.ExternalStorage,
	maxTotalRangesSize, maxTotalRangesKeys int64,
	maxWays int,
	maxRangeSize, maxRangeKeys int64,
) (*RangeSplitter, error) {
	propIter, err := NewMergePropIter(ctx, statFiles, externalStorage)
	if err != nil {
		return nil, err
	}

	return &RangeSplitter{
		rangesGroupSize: maxTotalRangesSize,
		rangesGroupKeys: maxTotalRangesKeys,
		maxWays:         maxWays,
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

// SplitOneRangesGroup splits one group of ranges. `endKeyOfGroup` represents
// the end key of the group, but it will be nil when the group is the last one.
// `dataFiles` and `statFiles` are the files that should be accessed in this
// group.
func (r *RangeSplitter) SplitOneRangesGroup() (
	endKeyOfGroup kv.Key,
	dataFiles []string,
	statFiles []string,
	rangeSplitKeys [][]byte,
	err error,
) {
	var curGroupSize, curGroupKeys int64
	var curRangeSize, curRangeKeys int64
	var lastFile, lastStats string
	var lastWays int
	var exhaustedFiles, exhaustedStats []string
	for r.propIter.Next() {
		if err = r.propIter.Error(); err != nil {
			return nil, nil, nil, nil, err
		}
		prop := r.propIter.prop()
		curGroupSize += int64(prop.size)
		curRangeSize += int64(prop.size)
		curGroupKeys += int64(prop.keys)
		curRangeKeys += int64(prop.keys)

		// TODO(lance6716): check if we have easier way to detect file exhausted
		ways := r.propIter.iter.h.Len()
		if ways < lastWays {
			exhaustedFiles = append(exhaustedFiles, lastFile)
			exhaustedStats = append(exhaustedStats, lastStats)
		}

		fileIdx := r.propIter.readerIndex()
		dataFilePath := r.dataFiles[fileIdx]
		statFilePath := r.statFiles[fileIdx]
		r.activeDataFiles[dataFilePath] = fileIdx
		r.activeStatFiles[statFilePath] = fileIdx

		if curRangeSize >= r.rangeSize || curRangeKeys >= r.rangeKeys {
			r.rangeSplitKeysBuf = append(r.rangeSplitKeysBuf, kv.Key(prop.key).Clone())
			curRangeSize = 0
			curRangeKeys = 0
		}

		if curGroupSize >= r.rangesGroupSize ||
			curGroupKeys >= r.rangesGroupKeys ||
			len(r.activeDataFiles) >= r.maxWays {
			retDataFiles, retStatFiles := r.collectFiles()
			for _, p := range exhaustedFiles {
				delete(r.activeDataFiles, p)
			}
			for _, p := range exhaustedStats {
				delete(r.activeStatFiles, p)
			}
			splitKeys := r.collectSplitKeys()
			return prop.key, retDataFiles, retStatFiles, splitKeys, nil
		}

		lastFile = dataFilePath
		lastStats = statFilePath
		lastWays = ways
	}

	retDataFiles, retStatFiles := r.collectFiles()
	splitKeys := r.collectSplitKeys()
	return nil, retDataFiles, retStatFiles, splitKeys, r.propIter.Error()
}

func (r *RangeSplitter) collectFiles() (data []string, stats []string) {
	dataFiles := make([]string, 0, len(r.activeDataFiles))
	for path := range r.activeDataFiles {
		dataFiles = append(dataFiles, path)
	}
	slices.SortFunc(dataFiles, func(i, j string) int {
		return r.activeDataFiles[i] - r.activeDataFiles[j]
	})
	statsFiles := make([]string, 0, len(r.activeStatFiles))
	for path := range r.activeStatFiles {
		statsFiles = append(statsFiles, path)
	}
	slices.SortFunc(statsFiles, func(i, j string) int {
		return r.activeStatFiles[i] - r.activeStatFiles[j]
	})
	return dataFiles, statsFiles
}

func (r *RangeSplitter) collectSplitKeys() [][]byte {
	ret := make([][]byte, 0, len(r.rangeSplitKeysBuf))
	for _, key := range r.rangeSplitKeysBuf {
		ret = append(ret, key)
	}
	r.rangeSplitKeysBuf = r.rangeSplitKeysBuf[:0]
	return ret
}
