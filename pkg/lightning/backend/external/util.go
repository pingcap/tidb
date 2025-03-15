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
	"encoding/json"
	goerrors "errors"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap/zapcore"
)

// seekPropsOffsets reads the statistic files to find the largest offset of
// corresponding sorted data file such that the key at offset is less than or
// equal to the given start keys. These returned offsets can be used to seek data
// file reader, read, parse and skip few smaller keys, and then locate the needed
// data.
//
// Caller can specify multiple ascending keys and seekPropsOffsets will return
// the offsets list per file for each key.
func seekPropsOffsets(
	ctx context.Context,
	starts []kv.Key,
	paths []string,
	exStorage storage.ExternalStorage,
) (_ [][]uint64, err error) {
	logger := logutil.Logger(ctx)
	task := log.BeginTask(logger, "seek props offsets")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	offsetsPerFile := make([][]uint64, len(paths))
	for i := range offsetsPerFile {
		offsetsPerFile[i] = make([]uint64, len(starts))
	}

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	for i := range paths {
		eg.Go(func() error {
			r, err2 := newStatsReader(egCtx, exStorage, paths[i], 250*1024)
			if err2 != nil {
				if goerrors.Is(err2, io.EOF) {
					return nil
				}
				return errors.Trace(err2)
			}
			defer r.Close()

			keyIdx := 0
			curKey := starts[keyIdx]

			p, err3 := r.nextProp()
			for {
				if err3 != nil {
					if goerrors.Is(err3, io.EOF) {
						// fill the rest of the offsets with the last offset
						currOffset := offsetsPerFile[i][keyIdx]
						for keyIdx++; keyIdx < len(starts); keyIdx++ {
							offsetsPerFile[i][keyIdx] = currOffset
						}
						return nil
					}
					return errors.Trace(err3)
				}
				propKey := kv.Key(p.firstKey)
				for propKey.Cmp(curKey) > 0 {
					keyIdx++
					if keyIdx >= len(starts) {
						return nil
					}
					offsetsPerFile[i][keyIdx] = offsetsPerFile[i][keyIdx-1]
					curKey = starts[keyIdx]
				}
				offsetsPerFile[i][keyIdx] = p.offset
				p, err3 = r.nextProp()
			}
		})
	}

	if err = eg.Wait(); err != nil {
		return nil, err
	}

	// TODO(lance6716): change the caller so we don't need to transpose the result
	offsetsPerKey := make([][]uint64, len(starts))
	for i := range starts {
		offsetsPerKey[i] = make([]uint64, len(paths))
		for j := range paths {
			offsetsPerKey[i][j] = offsetsPerFile[j][i]
		}
	}
	return offsetsPerKey, nil
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
	TotalKVCnt         uint64              `json:"total-kv-cnt"`
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
		TotalKVCnt:         summary.TotalCnt,
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
	m.TotalKVCnt += other.TotalKVCnt

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

// copyInternalFields makes a copy of the internal fields.
func copyInternalFields(a any) any {
	v := reflect.ValueOf(a)
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		newVal := reflect.New(v.Elem().Type())
		copyStruct(v.Elem(), newVal.Elem())
		return newVal.Interface()
	case reflect.Struct:
		newVal := reflect.New(v.Type()).Elem()
		copyStruct(v, newVal)
		return newVal.Interface()
	default:
		return a
	}
}

// copyExternalFields extracts and returns external fields tagged for inclusion during JSON marshaling.
// NOTE: Only handle the first level of external tags
func copyExternalFields(a any) map[string]any {
	res := make(map[string]any)
	v := reflect.ValueOf(a)
	if !v.IsValid() {
		return res
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return res
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return res
	}
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := t.Field(i)
		jsonTag := f.Tag.Get("json")
		if f.PkgPath != "" || jsonTag == "-" || strings.HasPrefix(jsonTag, "-,") || f.Tag.Get("external") != "true" {
			continue
		}
		if strings.Contains(jsonTag, "inline") {
			fieldVal := v.Field(i)
			if fieldVal.Kind() == reflect.Pointer && fieldVal.IsNil() {
				continue
			}
			for k, val := range copyExternalFields(fieldVal.Interface()) {
				res[k] = val
			}
			continue
		}
		key := f.Name
		var tagOpts []string
		if jsonTag != "" {
			parts := strings.Split(jsonTag, ",")
			if parts[0] != "" {
				key = parts[0]
			}
			if len(parts) > 1 {
				tagOpts = parts[1:]
			}
		}
		// Handle omitempty: if present and field is zero value, skip this field.
		fieldVal := v.Field(i)
		skip := false
		for _, opt := range tagOpts {
			if opt == "omitempty" && fieldVal.IsZero() {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		res[key] = fieldVal.Interface()
	}
	return res
}

// copyStruct copies non-external fields from source struct to destination struct.
func copyStruct(src, dst reflect.Value) {
	t := src.Type()
	for i := 0; i < src.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" || field.Tag.Get("external") == "true" {
			continue
		}
		dst.Field(i).Set(src.Field(i))
	}
}

// marshalInternalFields serializes internal fields to JSON.
func marshalInternalFields(a any) ([]byte, error) {
	return json.Marshal(copyInternalFields(a))
}

// marshalExternalFields serializes the external fields to JSON.
// Usage: For saving external meta, ensuring only intended fields are stored.
func marshalExternalFields(a any) ([]byte, error) {
	return json.Marshal(copyExternalFields(a))
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
func (m BaseExternalMeta) WriteJSONToExternalStorage(ctx context.Context, store storage.ExternalStorage, a any) error {
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
func (m BaseExternalMeta) ReadJSONFromExternalStorage(ctx context.Context, store storage.ExternalStorage, a any) error {
	if m.ExternalPath == "" || store == nil {
		return nil
	}
	data, err := store.ReadFile(ctx, m.ExternalPath)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(data, a)
}
