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
	goerrors "errors"
	"io"
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
		i := i
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
<<<<<<< HEAD
=======

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
	var fields []reflect.StructField
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

// SubtaskMetaPath returns the path of the subtask meta file.
func SubtaskMetaPath(taskID int64, subtaskID int64) string {
	return path.Join(strconv.FormatInt(taskID, 10), strconv.FormatInt(subtaskID, 10), metaName)
}

// remove all duplicates inside sorted array in place, i.e. input elements will be changed.
func removeDuplicates[E any](in []E, keyGetter func(*E) []byte, recordRemoved bool) ([]E, []E, int) {
	return doRemoveDuplicates(in, keyGetter, 0, recordRemoved)
}

// remove all duplicates inside sorted array in place if the duplicate count is
// more than 2, and keep the first two duplicates.
// we also return the total number of duplicates as the third return value.
func removeDuplicatesMoreThanTwo[E any](in []E, keyGetter func(*E) []byte) (out []E, removed []E, totalDup int) {
	return doRemoveDuplicates(in, keyGetter, 2, true)
}

// remove duplicates inside the sorted slice 'in', if keptDupCnt=2, we keep the
// first 2 duplicates, if keptDupCnt=0, we remove all duplicates.
// removed duplicates are returned in 'removed' if recordRemoved=true.
// we also return the total number of duplicates, either it's removed or not, as
// the third return value.
func doRemoveDuplicates[E any](
	in []E,
	keyGetter func(*E) []byte,
	keptDupCnt int,
	recordRemoved bool,
) (out []E, removed []E, totalDup int) {
	intest.Assert(keptDupCnt == 0 || keptDupCnt == 2, "keptDupCnt must be 0 or 2")
	if len(in) <= 1 {
		return in, []E{}, 0
	}
	pivotIdx, fillIdx := 0, 0
	pivot := keyGetter(&in[pivotIdx])
	if recordRemoved {
		removed = make([]E, 0, 2)
	}
	for idx := 1; idx <= len(in); idx++ {
		var key []byte
		if idx < len(in) {
			key = keyGetter(&in[idx])
			if bytes.Compare(pivot, key) == 0 {
				continue
			}
		}
		dupCount := idx - pivotIdx
		if dupCount >= 2 {
			totalDup += dupCount
			// keep the first keptDupCnt duplicates, and remove the rest
			for startIdx := pivotIdx; startIdx < pivotIdx+keptDupCnt; startIdx++ {
				if startIdx != fillIdx {
					in[fillIdx] = in[startIdx]
				}
				fillIdx++
			}
			if recordRemoved {
				removed = append(removed, in[pivotIdx+keptDupCnt:idx]...)
			}
		} else {
			if pivotIdx != fillIdx {
				in[fillIdx] = in[pivotIdx]
			}
			fillIdx++
		}
		pivotIdx = idx
		pivot = key
	}
	return in[:fillIdx], removed, totalDup
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
	adjustedMergeSortFileCountStep := GetAdjustedMergeSortFileCountStep(mergeConc)
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
	maxTargetFilesPerSubtask := max(MergeSortMaxSubtaskTargetFiles, mergeConc)
	for (rounds*nodeCnt*maxTargetFilesPerSubtask)+(adjustNodeCnt*maxTargetFilesPerSubtask) > int(GetAdjustedMergeSortOverlapThreshold(mergeConc)) {
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
>>>>>>> 06674e23094 (*: adjusts the merge sort overlap threshold/merge sort file count step based on concurrency (#62483))
