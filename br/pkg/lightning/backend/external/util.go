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
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// seekPropsOffsets seeks the statistic files to find the largest offset of
// sorted data file offsets such that the key at offset is less than or equal to
// the given start key.
func seekPropsOffsets(
	ctx context.Context,
	start kv.Key,
	paths []string,
	exStorage storage.ExternalStorage,
) ([]uint64, error) {
	iter, err := NewMergePropIter(ctx, paths, exStorage)
	if err != nil {
		return nil, err
	}
	logger := logutil.Logger(ctx)
	defer func() {
		if err := iter.Close(); err != nil {
			logger.Warn("failed to close merge prop iterator", zap.Error(err))
		}
	}()
	offsets := make([]uint64, len(paths))
	moved := false
	for iter.Next() {
		p := iter.prop()
		propKey := kv.Key(p.firstKey)
		if propKey.Cmp(start) > 0 {
			if !moved {
				return nil, fmt.Errorf("start key %s is too small for stat files %v",
					start.String(),
					paths,
				)
			}
			return offsets, nil
		}
		moved = true
		offsets[iter.readerIndex()] = p.offset
	}
	if iter.Error() != nil {
		return nil, iter.Error()
	}
	return offsets, nil
}

// GetAllFileNames returns data file paths and stat file paths. Both paths are
// sorted.
func GetAllFileNames(
	ctx context.Context,
	store storage.ExternalStorage,
	subDir string,
) ([]string, []string, error) {
	var data []string
	var stats []string

	err := store.WalkDir(ctx,
		&storage.WalkOption{SubDir: subDir},
		func(path string, size int64) error {
			// path example: /subtask/0_stat/0

			// extract the parent dir
			bs := hack.Slice(path)
			lastIdx := bytes.LastIndexByte(bs, '/')
			secondLastIdx := bytes.LastIndexByte(bs[:lastIdx], '/')
			parentDir := path[secondLastIdx+1 : lastIdx]

			if strings.HasSuffix(parentDir, statSuffix) {
				stats = append(stats, path)
			} else {
				data = append(data, path)
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}
	// in case the external storage does not guarantee the order of walk
	sort.Strings(data)
	sort.Strings(stats)
	return data, stats, nil
}

// CleanUpFiles delete all data and stat files under one subDir.
func CleanUpFiles(ctx context.Context, store storage.ExternalStorage, subDir string) error {
	dataNames, statNames, err := GetAllFileNames(ctx, store, subDir)
	if err != nil {
		return err
	}
	allFiles := make([]string, 0, len(dataNames)+len(statNames))
	allFiles = append(allFiles, dataNames...)
	allFiles = append(allFiles, statNames...)
	return store.DeleteFiles(ctx, allFiles)
}

// MockExternalEngine generates an external engine with the given keys and values.
func MockExternalEngine(
	storage storage.ExternalStorage,
	keys [][]byte,
	values [][]byte,
) (dataFiles []string, statsFiles []string, err error) {
	subDir := "/mock-test"
	writer := NewWriterBuilder().
		SetMemorySizeLimit(128).
		SetPropSizeDistance(32).
		SetPropKeysDistance(4).
		Build(storage, "/mock-test", "0")
	return MockExternalEngineWithWriter(storage, writer, subDir, keys, values)
}

// MockExternalEngineWithWriter generates an external engine with the given
// writer, keys and values.
func MockExternalEngineWithWriter(
	storage storage.ExternalStorage,
	writer *Writer,
	subDir string,
	keys [][]byte,
	values [][]byte,
) (dataFiles []string, statsFiles []string, err error) {
	ctx := context.Background()
	for i := range keys {
		err := writer.WriteRow(ctx, keys[i], values[i], nil)
		if err != nil {
			return nil, nil, err
		}
	}
	err = writer.Close(ctx)
	if err != nil {
		return nil, nil, err
	}
	return GetAllFileNames(ctx, storage, subDir)
}

// EndpointTp is the type of Endpoint.Key.
type EndpointTp int

const (
	// ExclusiveEnd represents "..., Endpoint.Key)".
	ExclusiveEnd EndpointTp = iota
	// InclusiveStart represents "[Endpoint.Key, ...".
	InclusiveStart
	// InclusiveEnd represents "..., Endpoint.Key]".
	InclusiveEnd
)

// Endpoint represents an endpoint of an interval which can be used by GetMaxOverlapping.
type Endpoint struct {
	Key    []byte
	Tp     EndpointTp
	Weight int64 // all EndpointTp use positive weight
}

// GetMaxOverlapping returns the maximum overlapping weight treating given
// `points` as endpoints of intervals. `points` are not required to be sorted,
// and will be sorted in-place in this function.
func GetMaxOverlapping(points []Endpoint) int64 {
	slices.SortFunc(points, func(i, j Endpoint) int {
		if cmp := bytes.Compare(i.Key, j.Key); cmp != 0 {
			return cmp
		}
		return int(i.Tp) - int(j.Tp)
	})
	var maxWeight int64
	var curWeight int64
	for _, p := range points {
		switch p.Tp {
		case InclusiveStart:
			curWeight += p.Weight
		case ExclusiveEnd, InclusiveEnd:
			curWeight -= p.Weight
		}
		if curWeight > maxWeight {
			maxWeight = curWeight
		}
	}
	return maxWeight
}

// SortedKVMeta is the meta of sorted kv.
type SortedKVMeta struct {
	MinKey      []byte `json:"min-key"`
	MaxKey      []byte `json:"max-key"`
	TotalKVSize uint64 `json:"total-kv-size"`
	// seems those 2 fields always generated from MultipleFilesStats,
	// maybe remove them later.
	DataFiles          []string            `json:"data-files"`
	StatFiles          []string            `json:"stat-files"`
	MultipleFilesStats []MultipleFilesStat `json:"multiple-files-stats"`
}

// NewSortedKVMeta creates a SortedKVMeta from a WriterSummary.
func NewSortedKVMeta(summary *WriterSummary) *SortedKVMeta {
	meta := &SortedKVMeta{
		MinKey:             summary.Min.Clone(),
		MaxKey:             summary.Max.Clone(),
		TotalKVSize:        summary.TotalSize,
		MultipleFilesStats: summary.MultipleFilesStats,
	}
	for _, f := range summary.MultipleFilesStats {
		for _, filename := range f.Filenames {
			meta.DataFiles = append(meta.DataFiles, filename[0])
			meta.StatFiles = append(meta.StatFiles, filename[1])
		}
	}
	return meta
}

// Merge merges the other SortedKVMeta into this one.
func (m *SortedKVMeta) Merge(other *SortedKVMeta) {
	m.MinKey = NotNilMin(m.MinKey, other.MinKey)
	m.MaxKey = NotNilMax(m.MaxKey, other.MaxKey)
	m.TotalKVSize += other.TotalKVSize

	m.DataFiles = append(m.DataFiles, other.DataFiles...)
	m.StatFiles = append(m.StatFiles, other.StatFiles...)

	m.MultipleFilesStats = append(m.MultipleFilesStats, other.MultipleFilesStats...)
}

// MergeSummary merges the WriterSummary into this SortedKVMeta.
func (m *SortedKVMeta) MergeSummary(summary *WriterSummary) {
	m.Merge(NewSortedKVMeta(summary))
}

// NotNilMin returns the smallest of a and b, ignoring nil values.
func NotNilMin(a, b []byte) []byte {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

// NotNilMax returns the largest of a and b, ignoring nil values.
func NotNilMax(a, b []byte) []byte {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}
