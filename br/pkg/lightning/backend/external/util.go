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
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// seekPropsOffsets seeks the statistic files to find the largest offset of
// sorted data file offsets such that the key at offset is less than or equal to
// the given start key.
func seekPropsOffsets(
	ctx context.Context,
	start kv.Key,
	paths []string,
	exStorage storage.ExternalStorage,
	checkHotSpot bool,
) (_ []uint64, err error) {
	logger := logutil.Logger(ctx)
	task := log.BeginTask(logger, "seek props offsets")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()
	iter, err := NewMergePropIter(ctx, paths, exStorage, checkHotSpot)
	if err != nil {
		return nil, err
	}
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
				return nil, fmt.Errorf("start key %s is too small for stat files %v, propKey %s",
					start.String(),
					paths,
					propKey.String(),
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
		SetMemorySizeLimit(10*(lengthBytes*2+10)).
		SetBlockSize(10*(lengthBytes*2+10)).
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
	StartKey           []byte              `json:"start-key"`
	EndKey             []byte              `json:"end-key"` // exclusive
	TotalKVSize        uint64              `json:"total-kv-size"`
	MultipleFilesStats []MultipleFilesStat `json:"multiple-files-stats"`
}

// NewSortedKVMeta creates a SortedKVMeta from a WriterSummary. If the summary
// is empty, it will return a pointer to zero SortedKVMeta.
func NewSortedKVMeta(summary *WriterSummary) *SortedKVMeta {
	if summary == nil || (len(summary.Min) == 0 && len(summary.Max) == 0) {
		return &SortedKVMeta{}
	}
	return &SortedKVMeta{
		StartKey:           summary.Min.Clone(),
		EndKey:             summary.Max.Clone().Next(),
		TotalKVSize:        summary.TotalSize,
		MultipleFilesStats: summary.MultipleFilesStats,
	}
}

// Merge merges the other SortedKVMeta into this one.
func (m *SortedKVMeta) Merge(other *SortedKVMeta) {
	if len(other.StartKey) == 0 && len(other.EndKey) == 0 {
		return
	}
	if len(m.StartKey) == 0 && len(m.EndKey) == 0 {
		*m = *other
		return
	}

	m.StartKey = BytesMin(m.StartKey, other.StartKey)
	m.EndKey = BytesMax(m.EndKey, other.EndKey)
	m.TotalKVSize += other.TotalKVSize

	m.MultipleFilesStats = append(m.MultipleFilesStats, other.MultipleFilesStats...)
}

// MergeSummary merges the WriterSummary into this SortedKVMeta.
func (m *SortedKVMeta) MergeSummary(summary *WriterSummary) {
	m.Merge(NewSortedKVMeta(summary))
}

// GetDataFiles returns all data files in the meta.
func (m *SortedKVMeta) GetDataFiles() []string {
	var ret []string
	for _, stat := range m.MultipleFilesStats {
		for _, files := range stat.Filenames {
			ret = append(ret, files[0])
		}
	}
	return ret
}

// GetStatFiles returns all stat files in the meta.
func (m *SortedKVMeta) GetStatFiles() []string {
	var ret []string
	for _, stat := range m.MultipleFilesStats {
		for _, files := range stat.Filenames {
			ret = append(ret, files[1])
		}
	}
	return ret
}

// BytesMin returns the smallest of byte slice a and b.
func BytesMin(a, b []byte) []byte {
	if bytes.Compare(a, b) < 0 {
		return a
	}
	return b
}

// BytesMax returns the largest of byte slice a and b.
func BytesMax(a, b []byte) []byte {
	if bytes.Compare(a, b) > 0 {
		return a
	}
	return b
}

func getSpeed(n uint64, dur float64, isBytes bool) string {
	if dur == 0 {
		return "-"
	}
	if isBytes {
		return units.BytesSize(float64(n) / dur)
	}
	return strconv.FormatFloat(float64(n)/dur, 'f', 4, 64)
}
