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
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

const (
	metaName = "meta.json"
)

// CleanUpFiles delete all data and stat files under the same non-partitioned dir.
// see randPartitionedPrefix for how we partition the files.
func CleanUpFiles(ctx context.Context, store storeapi.Storage, nonPartitionedDir string) error {
	failpoint.Inject("skipCleanUpFiles", func() {
		failpoint.Return(nil)
	})
	names, err := simplesst.GetAllFileNames(ctx, store, nonPartitionedDir)
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

// DivideMergeSortDataFiles divides the data files into multiple groups for
// merge sort. Each group will be assigned to a node for sorting.
// The number of files in each group is limited to MaxMergeSortFileCountStep.
func DivideMergeSortDataFiles(dataFiles []string, nodeCnt int, mergeConc int) ([][]string, error) {
	if nodeCnt == 0 {
		return nil, errors.Errorf("unsupported zero node count")
	}
	if len(dataFiles) == 0 {
		return [][]string{}, nil
	}
	adjustedMergeSortFileCountStep := simplesst.GetAdjustedMergeSortFileCountStep(mergeConc)
	dataFilesCnt := len(dataFiles)
	result := make([][]string, 0, nodeCnt)
	batches := len(dataFiles) / adjustedMergeSortFileCountStep
	rounds := batches / nodeCnt
	for range rounds * nodeCnt {
		result = append(result, dataFiles[:adjustedMergeSortFileCountStep])
		dataFiles = dataFiles[adjustedMergeSortFileCountStep:]
	}
	remainder := dataFilesCnt - (nodeCnt * rounds * adjustedMergeSortFileCountStep)
	if remainder == 0 {
		return result, nil
	}
	// adjust node cnt for remainder files to avoid having too much target files.
	adjustNodeCnt := nodeCnt
	maxTargetFilesPerSubtask := max(simplesst.MergeSortMaxSubtaskTargetFiles, mergeConc)
	for (rounds*nodeCnt*maxTargetFilesPerSubtask)+(adjustNodeCnt*maxTargetFilesPerSubtask) > int(simplesst.GetAdjustedMergeSortOverlapThreshold(mergeConc)) {
		adjustNodeCnt--
		if adjustNodeCnt == 0 {
			return nil, errors.Errorf("unexpected zero node count, dataFiles=%d, nodeCnt=%d", dataFilesCnt, nodeCnt)
		}
	}
	minimalFileCount := 32 // Each subtask should merge at least 32 files.
	adjustNodeCnt = max(min(remainder/minimalFileCount, adjustNodeCnt), 1)
	sizes := mathutil.Divide2Batches(remainder, adjustNodeCnt)
	for _, s := range sizes {
		result = append(result, dataFiles[:s])
		dataFiles = dataFiles[s:]
	}
	return result, nil
}
