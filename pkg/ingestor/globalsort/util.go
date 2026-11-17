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

package globalsort

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"reflect"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/errdef"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

const (
	metaName = "meta.json"
)

// CleanUpFiles delete all data and stat files under the same non-partitioned dirs.
// see randPartitionedPrefix for how we partition the files.
func CleanUpFiles(ctx context.Context, store storeapi.Storage, nonPartitionedDirs ...string) error {
	failpoint.Inject("skipCleanUpFiles", func() {
		failpoint.Return(nil)
	})
	if len(nonPartitionedDirs) == 0 {
		return nil
	}
	// TODO: GetAllFileNames accumulates every matching file name in memory before
	// deletion. Large imports can therefore consume excessive memory or cause an
	// OOM. List and delete files in bounded batches to keep memory usage bounded.
	names, err := simplesst.GetAllFileNames(ctx, store, nonPartitionedDirs...)
	if err != nil {
		return err
	}
	return store.DeleteFiles(ctx, names)
}

// MockExternalEngine generates an external engine with the given keys and values.
func MockExternalEngine(
	storage storeapi.Storage,
	keys [][]byte,
	values [][]byte,
) (dataFiles []string, statsFiles []string, err error) {
	var summary *simplesst.WriterSummary
	writer := simplesst.NewWriterBuilder().
		SetMemorySizeLimit(10*(simplesst.LengthBytes*2+10)).
		SetBlockSize(10*(simplesst.LengthBytes*2+10)).
		SetPropSizeDistance(32).
		SetPropKeysDistance(4).
		SetOnCloseFunc(func(s *simplesst.WriterSummary) { summary = s }).
		Build(storage, "/mock-test", "0")
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
	for _, ms := range summary.MultipleFilesStats {
		for _, f := range ms.Filenames {
			dataFiles = append(dataFiles, f[0])
			statsFiles = append(statsFiles, f[1])
		}
	}
	return
}

// SortedKVMeta is the meta of sorted kv.
type SortedKVMeta struct {
	StartKey           []byte                        `json:"start-key"`
	EndKey             []byte                        `json:"end-key"` // exclusive
	TotalKVSize        uint64                        `json:"total-kv-size"`
	TotalKVCnt         uint64                        `json:"total-kv-cnt"`
	MultipleFilesStats []simplesst.MultipleFilesStat `json:"multiple-files-stats"`
	ConflictInfo       engineapi.ConflictInfo        `json:"conflict-info"`
}

// NewSortedKVMeta creates a SortedKVMeta from a WriterSummary. If the summary
// is empty, it will return a pointer to zero SortedKVMeta.
func NewSortedKVMeta(summary *simplesst.WriterSummary) *SortedKVMeta {
	if summary == nil || (len(summary.Min) == 0 && len(summary.Max) == 0) {
		return &SortedKVMeta{}
	}
	return &SortedKVMeta{
		StartKey:           summary.Min.Clone(),
		EndKey:             summary.Max.Clone().Next(),
		TotalKVSize:        summary.TotalSize,
		TotalKVCnt:         summary.TotalCnt,
		MultipleFilesStats: summary.MultipleFilesStats,
		ConflictInfo:       summary.ConflictInfo,
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
	m.TotalKVCnt += other.TotalKVCnt

	m.MultipleFilesStats = append(m.MultipleFilesStats, other.MultipleFilesStats...)
	m.ConflictInfo.Merge(&other.ConflictInfo)
}

// MergeSummary merges the WriterSummary into this SortedKVMeta.
func (m *SortedKVMeta) MergeSummary(summary *simplesst.WriterSummary) {
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

// marshalWithOverride marshals the provided struct with the ability to override
func marshalWithOverride(src any, hideCond func(f reflect.StructField) bool) ([]byte, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return json.Marshal(src)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return json.Marshal(src)
	}
	t := v.Type()
	fields := make([]reflect.StructField, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		newTag := f.Tag
		if hideCond(f) {
			newTag = `json:"-"`
		}
		fields = append(fields, reflect.StructField{
			Name:      f.Name,
			Type:      f.Type,
			Tag:       newTag,
			Offset:    f.Offset,
			Anonymous: f.Anonymous,
		})
	}
	newType := reflect.StructOf(fields)
	newVal := reflect.New(newType).Elem()
	j := 0
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		newVal.Field(j).Set(v.Field(i))
		j++
	}
	return json.Marshal(newVal.Interface())
}

// marshalInternalFields marshal all fields except those with external:"true" tag.
func marshalInternalFields(src any) ([]byte, error) {
	return marshalWithOverride(src, func(f reflect.StructField) bool {
		return f.Tag.Get("external") == "true"
	})
}

// marshalExternalFields marshal all fields with external:"true" tag.
func marshalExternalFields(src any) ([]byte, error) {
	return marshalWithOverride(src, func(f reflect.StructField) bool {
		return f.Tag.Get("external") != "true"
	})
}

// BaseExternalMeta is the base meta of external meta.
type BaseExternalMeta struct {
	// ExternalPath is the path to the external storage where the external meta is stored.
	ExternalPath string
}

// Marshal serializes the provided alias to JSON.
// Usage: If ExternalPath is set, marshals using internal meta; otherwise marshals the alias directly.
func (m BaseExternalMeta) Marshal(alias any) ([]byte, error) {
	if m.ExternalPath == "" {
		return json.Marshal(alias)
	}
	return marshalInternalFields(alias)
}

// WriteJSONToExternalStorage writes the serialized external meta JSON to external storage.
// Usage: Store external meta after appropriate modifications.
func (m BaseExternalMeta) WriteJSONToExternalStorage(ctx context.Context, store storeapi.Storage, a any) error {
	if m.ExternalPath == "" {
		return nil
	}
	data, err := marshalExternalFields(a)
	if err != nil {
		return errors.Trace(err)
	}
	return store.WriteFile(ctx, m.ExternalPath, data)
}

// ReadJSONFromExternalStorage reads and unmarshals JSON from external storage into the provided alias.
// Usage: Retrieve external meta for further processing.
func (m BaseExternalMeta) ReadJSONFromExternalStorage(ctx context.Context, store storeapi.Storage, a any) error {
	if m.ExternalPath == "" {
		return nil
	}
	data, err := store.ReadFile(ctx, m.ExternalPath)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(data, a)
}

// PlanMetaPath returns the path of the plan meta file.
func PlanMetaPath(taskID int64, step string, idx int) string {
	return path.Join(strconv.FormatInt(taskID, 10), "plan", step, strconv.Itoa(idx), metaName)
}

// PreparedMetaPath returns the path of the prepared meta file.
func PreparedMetaPath(taskID int64) string {
	return path.Join(strconv.FormatInt(taskID, 10), "plan", "prepared", metaName)
}

// SubtaskMetaPath returns the path of the subtask meta file.
func SubtaskMetaPath(taskID int64, subtaskID int64) string {
	return path.Join(strconv.FormatInt(taskID, 10), strconv.FormatInt(subtaskID, 10), metaName)
}

// DivideMergeSortDataFiles divides data files into groups, one per merge-sort
// subtask. It balances groups in rounds of nodeCnt to use all available
// resources. It also limits each group's input files and caps the total target
// files so the following ingest step can read them all.
//
// Known issue: the target file count is exact only when merge execution uses
// the same concurrency passed here. Distributed add-index and IMPORT INTO do
// not persist that concurrency in merge subtask metadata; they derive it again
// from the current execution resource. If the resource changes after planning,
// the merge subtasks can produce more files than estimated here and exceed the
// total file count expected by the ingest step. Fixing this requires pinning the
// concurrency across planning, merge execution, and ingest, or revalidating and
// regenerating all pending merge groups when it changes.
//
// since we have a 4000 hard limit on the target file count, when the concurrency
// is larger than 16 (4000/250), DivideMergeSortDataFiles might incorrectly return
// ErrTooManyDataFiles on some input, such as:
//
//	file-count=940001 / nodeCnt=2 / concurrency=17
//
// the 4000 is an experience value which came from internal tests where the max
// node spec we used is 16c, we haven't tested on 32c or 64c. maybe we can remove
// this 4000 hard limit, and calculate the limit by concurrency * 250.
//
// suppose we have a 8c node with cpu:mem = 1:4, but on the node only 7c and
// 26.9GiB memory is available to tidb-server, below table give the max supported
// row KV size before reporting ErrTooManyDataFiles for different params. such
// as for 1 secondary index, with concurrency=1, the max all supported row KV
// size is from 29.56TiB (when each index kv size = row kv size) to 92.98 TiB
// (when each index kv size = 0.1 * row kv size).
//
//	| Index Count | Concurrency 1 | Concurrency 3  |  Concurrency 7  |
//	| ----------: | ------------: | -------------: | --------------: |
//	|    No index |     121.6 TiB |      364.8 TiB |       851.2 TiB |
//	|     1 index | 29.6–93.0 TiB | 88.7–279.0 TiB | 206.9–650.9 TiB |
//	|  16 indexes |  6.7–18.5 TiB |  20.0–55.6 TiB |  46.7–129.6 TiB |
//	| 128 indexes |   1.0–2.7 TiB |    2.9–8.1 TiB |    6.7–18.8 TiB |
//
// if the 8c node is with cpu:mem = 1:2, the max supported kv size is around
// half of above table.
func DivideMergeSortDataFiles(files []string, nodeCnt, concurrency int) ([][]string, error) {
	if nodeCnt == 0 {
		return nil, errors.Errorf("unsupported zero node count")
	}
	if len(files) == 0 {
		return [][]string{}, nil
	}
	maxFiles := simplesst.GetAdjustedMergeSortFileCountStep(concurrency)
	fileCnt := len(files)
	groups := make([][]string, 0, nodeCnt)
	// Part 1: Fill complete rounds of maximum-sized groups so every available
	// subtask slot has work.
	fullGroupCnt := fileCnt / maxFiles / nodeCnt * nodeCnt
	for range fullGroupCnt {
		groups = append(groups, files[:maxFiles])
		files = files[maxFiles:]
	}
	targetCnt := fullGroupCnt * getTargetFileCount(maxFiles, concurrency)
	targetLimit := int(simplesst.GetAdjustedMergeSortOverlapThreshold(concurrency))
	remaining := fileCnt - fullGroupCnt*maxFiles
	if remaining == 0 {
		if targetCnt > targetLimit {
			return nil, errdef.ErrTooManyDataFiles.GenWithStackByArgs(fileCnt, concurrency, targetLimit)
		}
		return groups, nil
	}

	// Part 2: Divide the remaining files evenly while preserving parallelism
	// and keeping the target file count within the ingest limit.
	minFiles := 32 // Each subtask should merge at least 32 files.
	maxGroups := max(min(remaining/minFiles, nodeCnt), 1)
	minGroups := (remaining + maxFiles - 1) / maxFiles
	groupCnt := 0
	// Prefer more groups for parallelism while staying within the ingest limit.
	for candidateCnt := maxGroups; candidateCnt >= minGroups; candidateCnt-- {
		candidateTargetCnt := targetCnt + getGroupedTargetFileCount(remaining, candidateCnt, concurrency)
		if candidateTargetCnt <= targetLimit {
			groupCnt = candidateCnt
			break
		}
	}
	if groupCnt == 0 {
		return nil, errdef.ErrTooManyDataFiles.GenWithStackByArgs(fileCnt, concurrency, targetLimit)
	}

	sizes := mathutil.Divide2Batches(remaining, groupCnt)
	for _, size := range sizes {
		groups = append(groups, files[:size])
		files = files[size:]
	}
	return groups, nil
}
